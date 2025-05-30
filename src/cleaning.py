from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
import pyspark.sql.functions as spf
from dataclasses import dataclass
import wordninja
import logging

from . import string_utils
from .cleaners.ColCleaner import ColCleaner
from .cleaners.DropCleaner import DropCleaner
from .cleaners.AutoCleaner import AutoCleaner

_empty_logger = logging.getLogger("empty_logger")
_empty_logger.addHandler(logging.NullHandler())


@dataclass
class Result:
    value: DataFrame | ConnectDataFrame
    renamed_cols: dict[str, str]

    def __post_init__(self):

        assert isinstance(self.value, (DataFrame, ConnectDataFrame))
        assert isinstance(self.renamed_cols, dict)


def clean_table(
    df: DataFrame | ConnectDataFrame,
    cleaners: dict[str, ColCleaner],
    drop_complete_duplicates: bool = False,
    logger: logging.Logger = _empty_logger,
) -> Result:
    """
    Handles the following tasks:
    - Trims all string columns
    - Handles decimal precision
    - Handles currency precision
    - Handles date fmt
    - Handles datetime fmt
    - Consistent column names
    """

    # Convert every column to string
    for col in df.columns:
        df = df.withColumn(col, spf.col(col).cast("string"))

    default_cleaner = cleaners.get("*", AutoCleaner())

    # -----------------------------------------------------------------------------------------------------
    # Cleaning columns
    # -----------------------------------------------------------------------------------------------------

    logger.info("Cleaning columns...")

    for col in df.columns:
        cleaner: ColCleaner = cleaners.get(col, default_cleaner)
        logger.debug(
            f"Cleaning col '{col}' with '{cleaner}' cleaner..."
        )

        if isinstance(cleaner, DropCleaner):
            logger.info(f"Dropping column '{col}'...")
            df = df.drop(col)
            continue

        df = df.withColumn(col, cleaner.clean_col(col))

    logger.info("Cleaning columns done.")

    # -----------------------------------------------------------------------------------------------------
    # Dropping complete duplicates
    # -----------------------------------------------------------------------------------------------------

    if drop_complete_duplicates:
        logger.info("Dropping complete duplicates...")

        before_drop_count = df.count()
        df = df.dropDuplicates()
        drop_count = before_drop_count - df.count()

        logger.info(
            f"Dropping complete duplicates done, dropped {drop_count}.")

    # -----------------------------------------------------------------------------------------------------
    # Renaming columns
    # -----------------------------------------------------------------------------------------------------

    logger.info("Renaming columns...")

    rename_mapping: dict[str, str] = {}

    for col in df.columns:

        # Calculate new name for the column
        cleaner = cleaners.get(col, default_cleaner)
        if cleaner is not None and cleaner.rename_to is not None:
            new_name = cleaner.rename_to
        else:
            words = wordninja.split(col)
            new_name = "_".join(words)
            new_name = string_utils.to_snake_case(new_name)

        # Register new name for renaming only if it is different than what it already is,
        # no need to clutter up the rename mapping and logs
        if new_name != col:
            logger.info(f"Renaming column '{col}' to '{new_name}'...")
            rename_mapping[col] = new_name

    df = df.withColumnsRenamed(rename_mapping)

    logger.info(f"Renaming columns done.")

    return Result(
        value=df,
        renamed_cols=rename_mapping,
    )

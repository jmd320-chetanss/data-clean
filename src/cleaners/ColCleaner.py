from dataclasses import dataclass
from typing import Optional
from abc import ABC, abstractmethod
from pyspark.sql import Column
import pyspark.sql.functions as spf


def col_cleaner_default_preprocessor(value: str | None) -> str | None:
    """
    Default preprocessor function to handle None values.

    :param value: The value to preprocess.
    :return: The preprocessed value.
    """
    if value is None:
        return None

    value = value.strip()
    if value == "":
        return None

    return value


@dataclass
class ColCleaner(ABC):
    preprocess: callable = col_cleaner_default_preprocessor

    # Rename the column
    rename_to: Optional[str] = None

    # Is this a key column
    key: bool = False

    # The type of the column for the database table
    datatype: str = "string"

    def clean_col(self, col: Column) -> Column:
        """
        Clean the column using the cleaner function.

        :param col: The column to clean.
        :return: The cleaned column.
        """
        cleaner = self._get_cleaner()
        cleaner_udf = spf.udf(cleaner, "string")
        return cleaner_udf(col).cast(self.datatype)

    @abstractmethod
    def _get_cleaner(self) -> callable:
        """
        Get the cleaner function for the column.
        """
        pass

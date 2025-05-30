from dataclasses import dataclass, KW_ONLY
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


@dataclass(frozen=True)
class ColCleaner(ABC):
    _: KW_ONLY

    preprocess: callable = col_cleaner_default_preprocessor

    # Rename the column
    rename_to: str | None = None

    # The type of the column for the database table
    datatype: str = "string"

    def __post_init__(self):
        assert callable(self.preprocess), \
            "Preprocess must be a callable function."

        assert isinstance(self.rename_to, (str, type(None))), \
            "Rename_to must be a string or None."

        assert isinstance(self.datatype, str), \
            "Datatype must be a string."

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
    def clean_value(self, value: str | None) -> str | None:
        """
        Returns a cleaned value.
        """
        pass

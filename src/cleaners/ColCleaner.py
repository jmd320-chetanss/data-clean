from dataclasses import dataclass, KW_ONLY
from abc import ABC, abstractmethod
from pyspark.sql import Column
from typing import Literal, staticmethod
import pyspark.sql.functions as spf


def default_preprocessor(value: str | None) -> str | None:
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

    # Preprocessor function to apply before cleaning
    preprocessor: callable | None = default_preprocessor

    # Postprocessor function to apply after cleaning
    postprocessor: callable | None = None

    # Rename the column
    rename_to: str | None = None

    # Error handling strategy for cleaning
    onerror: Literal["raise", "value", "none"] = "raise"

    # The type of the column for the database table
    datatype: str = "string"

    def __post_init__(self):
        assert self.preprocessor is None or callable(
            self.preprocessor
        ), "Preprocess must be a callable function or None."

        assert self.postprocessor is None or callable(
            self.postprocessor
        ), "Postprocess must be a callable function or None."

        assert isinstance(
            self.rename_to, (str, type(None))
        ), "Rename_to must be a string or None."

        assert isinstance(self.datatype, str), "Datatype must be a string."

    def clean_col(self, col: Column) -> Column:
        """
        Clean the column using the cleaner function.

        :param col: The column to clean.
        :return: The cleaned column.
        """

        def cleaner(value: str | None) -> str | None:
            """
            Cleaner function that applies the preprocessing and cleaning logic.

            :param value: The value to clean.
            :return: The cleaned value.
            """
            preprocessed_value = (
                self.preprocessor(value) if self.preprocessor is not None else value
            )

            if preprocessed_value is None:
                return None

            try:
                cleaned_value = self.clean_value(preprocessed_value)
            except Exception as e:
                if self.onerror == "raise":
                    raise RuntimeError(
                        f"Error cleaning value '{preprocessed_value}' in column '{col}', error: {e}"
                    )

                elif self.onerror == "none":
                    cleaned_value = None
                elif self.onerror == "value":
                    cleaned_value = preprocessed_value
                else:
                    raise ValueError(f"Invalid onerror value: {self.onerror}")

            posprocessed_value = (
                self.postprocessor(cleaned_value)
                if self.postprocessor is not None
                else cleaned_value
            )

            return posprocessed_value

        cleaner_udf = spf.udf(cleaner, "string")
        return cleaner_udf(col).cast(self.datatype)

    @abstractmethod
    def clean_value(self, value: str | None) -> str | None:
        """
        Returns a cleaned value.
        """
        pass

from dataclasses import dataclass, field, KW_ONLY
from abc import ABC, abstractmethod
from pyspark.sql import Column
import pyspark.sql.functions as spf
from dataclean.src.preprocessors import default_preprocessor
from dataclean.src.error_handlers import (
    ErrorHandler,
    ErrorContext,
    error_handler_default,
)


@dataclass(frozen=True)
class ColCleaner(ABC):
    _: KW_ONLY

    # Preprocessor function to apply before cleaning
    preprocessors: list[callable] = field(
        default_factory=lambda: [default_preprocessor]
    )

    # Postprocessor function to apply after cleaning
    postprocessors: list[callable] = field(default_factory=list)

    # Rename the column
    rename_to: str | None = None

    # Error handling strategy for cleaning
    error_handler: ErrorHandler = field(default=error_handler_default)

    # The type of the column for the database table
    datatype: str = "string"

    def __post_init__(self):
        assert isinstance(self.preprocessors, list), "Preprocessors must be a list."

        assert isinstance(self.postprocessors, list), "Postprocessors must be a list."

        assert all(
            callable(preprocessor) for preprocessor in self.preprocessors
        ), "All preprocessors must be callable."

        assert all(
            callable(postprocessor) for postprocessor in self.postprocessors
        ), "All postprocessors must be callable."

        assert isinstance(
            self.rename_to, (str, type(None))
        ), "Rename_to must be a string or None."

        assert isinstance(self.datatype, str), "Datatype must be a string."

    def clean_col(self, col: str) -> Column:
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

            # --------------------------------------------------------------------------------------
            # Preprocessing stage
            # --------------------------------------------------------------------------------------

            preprocessed_value = value

            if len(self.preprocessors) > 0:
                try:
                    for preprocessor in self.preprocessors:
                        preprocessed_value = preprocessor(preprocessed_value)

                except Exception as ex:
                    error_context = ErrorContext(
                        col=col,
                        stage="preprocessing",
                        value=value,
                        error=ex,
                    )

                    preprocessed_value = self.error_handler(error_context)

            if preprocessed_value is None:
                return None

            # --------------------------------------------------------------------------------------
            # Cleaning stage
            # --------------------------------------------------------------------------------------

            try:
                cleaned_value = self.clean_value(preprocessed_value)

            except Exception as ex:
                error_context = ErrorContext(
                    col=col,
                    stage="cleaning",
                    value=preprocessed_value,
                    error=ex,
                )

                cleaned_value = self.error_handler(error_context)

            # --------------------------------------------------------------------------------------
            # Postprocessing stage
            # --------------------------------------------------------------------------------------

            postprocessed_value = cleaned_value

            if len(self.postprocessors) > 0:
                try:
                    for postprocessor in self.postprocessors:
                        postprocessed_value = postprocessor(postprocessed_value)

                except Exception as ex:
                    error_context = ErrorContext(
                        col=col,
                        stage="postprocessing",
                        value=cleaned_value,
                        error=ex,
                    )

                    postprocessed_value = self.error_handler(error_context)

            return postprocessed_value

        cleaner_udf = spf.udf(cleaner, "string")
        return cleaner_udf(col).cast(self.datatype)

    @abstractmethod
    def clean_value(self, value: str | None) -> str | None:
        """
        Returns a cleaned value.
        """
        pass

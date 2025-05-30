from dataclasses import dataclass
from typing import Optional
from .. import math_utils
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class IntCleaner(ColCleaner):
    """
    Class to clean integer columns in a DataFrame.
    """

    # Minimum value of the signed integer
    min_value: Optional[int] = None

    # Maximum value of the signed integer
    max_value: Optional[int] = 9223372036854775807

    def __post_init__(self):

        assert self.min_value is None or isinstance(
            self.min_value, int
        ), f"min_value must be an int or None, got {type(self.min_value)}"

        assert self.max_value is None or isinstance(
            self.max_value, int
        ), f"max_value must be an int or None, got {type(self.max_value)}"

        object.__setattr__(self, "datatype", "bigint")

    def clean_value(self, value: str) -> str | None:

        parsed_value = math_utils.parse_int(value)

        if parsed_value is None:
            raise ValueError(f"Cannot parse '{value}' as integer.")

        if self.min_value is not None and parsed_value < self.min_value:
            raise ValueError(
                f"Value '{value}' parsed as '{parsed_value}' is less than the specified '{self.min_value}' value."
            )

        if self.max_value is not None and parsed_value > self.max_value:
            raise ValueError(
                f"Value '{value}' parsed as '{parsed_value}' is greater than the specified '{self.max_value}' value."
            )

        return parsed_value

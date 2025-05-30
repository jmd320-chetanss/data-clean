from dataclasses import dataclass
from .. import math_utils
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class FloatCleaner(ColCleaner):
    """
    Class to clean float columns in a DataFrame.
    """

    # Precision of the decimal
    precision: int = 2

    def __post_init__(self):

        assert self.precision >= 0, "Precision must be a non-negative integer"

        assert self.precision <= 38, "Precision must be less than or equal to 38"

        object.__setattr__
        self.datatype = f"decimal(38, {self.precision})"

    def clean_value(self, value: str | None):

        parsed_value = math_utils.parse_float(value)
        if parsed_value is None:
            raise ValueError(f"Cannot parse '{value}' as decimal")

        parsed_value = math_utils.floor_float(parsed_value, self.precision)
        return parsed_value

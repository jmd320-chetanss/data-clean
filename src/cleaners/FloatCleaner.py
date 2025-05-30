from dataclasses import dataclass, field
from typing import Literal
from .. import math_utils
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class FloatCleaner(ColCleaner):
    """
    Class to clean float columns in a DataFrame.
    """

    range: Literal["f128", "money"] = "f128"

    # Precision of the decimal
    precision: int = 2

    _precision: int = field(init=False, repr=False)

    def __post_init__(self):

        assert self.precision >= 0, "Precision must be a non-negative integer"

        assert self.precision <= 38, "Precision must be less than or equal to 38"

        if self.range == "money":
            precision = 3
        else:
            precision = 2

        object.__setattr__(self, "_precision", precision)
        object.__setattr__(self, "datatype", f"decimal(38, {precision})")

    def clean_value(self, value: str | None):

        parsed_value = float(value)
        parsed_value = math_utils.floor_float(parsed_value, self.precision)
        return parsed_value

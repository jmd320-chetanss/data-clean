from dataclasses import dataclass
from typing import Literal
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class GenderCleaner(ColCleaner):

    # Output format to represent gender
    fmt: Literal["malefemale", "mf"] = "malefemale"

    # Should the output format be in lowercase
    lower: bool = False

    def __post_init__(self):

        assert self.fmt in [
            "malefemale",
            "mf",
        ], f"Invalid format '{self.fmt}'. Must be 'malefemale' or 'mf'."

        assert isinstance(
            self.lower, bool
        ), f"Lower must be a boolean, not {type(self.lower)}."

    def clean_value(self, value: str | None) -> str | None:

        value_clean = value.lower().strip()
        ismale = value_clean in ["male", "m"]
        isfemale = value_clean in ["female", "f"]

        if not ismale and not isfemale:
            raise ValueError(f"Cannot parse '{value}' as gender")

        if self.fmt == "malefemale":
            result = "Male" if ismale else "Female"
            return result.lower() if self.lower else result

        if self.fmt == "mf":
            result = "M" if ismale else "F"
            return result.lower() if self.lower else result

        raise ValueError(f"Invalid output format '{self.fmt}'")

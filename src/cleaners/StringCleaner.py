from dataclasses import dataclass, field
from typing import Literal, override
from .. import string_utils
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class StringCleaner(ColCleaner):
    # Minimum length of the string
    min_length: int = 0

    # Maximum length of the string
    max_length: int = 255

    # Should the string be trimmed
    trim: bool = True

    # Should the string be converted to lowercase
    case: Literal["lower", "upper", "snake", "camel", "pascal"] | None = None

    _case_updater: callable = field(init=False, repr=False)

    def __post_init__(self):

        assert self.min_length >= 0, \
            "Minimum length must be non-negative."

        assert self.max_length >= self.min_length, \
            "Maximum length must be greater than or equal to minimum length."

        assert self.case in (None, "lower", "upper", "snake", "camel", "pascal"), \
            "Case must be one of 'lower', 'upper', 'snake', 'camel', 'pascal' or None."

        assert isinstance(self.trim, bool), "Trim must be a boolean value."

        case_updater = self._get_case_updater(self.case)

        if case_updater is None:
            raise ValueError(f"Invalid case '{self.case}'.")

        object.__setattr__(self, "_case_updater", case_updater)

    @override
    def clean_value(self, value: str | None) -> str | None:

        if value is None:
            return None

        if self.trim:
            value = value.strip()

        value = self._case_updater(value)
        return value

    def _get_case_updater(self, case: str) -> callable:
        match case:
            case None:
                return lambda value: value
            case "lower":
                return lambda value: value.lower()
            case "upper":
                return lambda value: value.upper()
            case "snake":
                return lambda value: string_utils.to_snake_case(value)
            case "camel":
                return lambda value: string_utils.to_camel_case(value)
            case "pascal":
                return lambda value: string_utils.to_pascal_case(value)
            case _:
                return None

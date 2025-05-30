from dataclasses import dataclass, field
from typing import override
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class EnumCleaner(ColCleaner):
    """
    A class to clean enum values.
    """

    cases: list[str] = field(default_factory=list)
    default_value: str | None = None
    ignore_case: bool = True

    def __post_init__(self):

        object.__setattr__(self, "datatype", "boolean")

    @override
    def clean_value(self, value: str | None) -> str | None:

        value_clean = value.lower().strip()
        istrue = value_clean in self._true_cases
        isfalse = value_clean in self._false_cases

        if not istrue and not isfalse:
            raise ValueError(f"Cannot parse '{value}' as boolean.")

        return self.true_value if istrue else self.false_value

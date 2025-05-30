from dataclasses import dataclass, field
from typing import ClassVar, override
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class BoolCleaner(ColCleaner):
    """
    A class to clean boolean values.
    """

    DEFAULT_TRUE_CASES: ClassVar[list[str]] = [
        "true", "t", "yes", "y", "on", "1"
    ]

    DEFAULT_FALSE_CASES: ClassVar[list[str]] = [
        "false", "f", "no", "n", "off", "0"
    ]

    DEFAULT_TRUE_OUTPUT: ClassVar[str] = "True"
    DEFAULT_FALSE_OUTPUT: ClassVar[str] = "False"

    true_cases: list[str] = field(
        default_factory=lambda: BoolCleaner.DEFAULT_TRUE_CASES
    )

    false_cases: list[str] = field(
        default_factory=lambda: BoolCleaner.DEFAULT_FALSE_CASES
    )

    extra_true_cases: list[str] = field(default_factory=list)
    extra_false_cases: list[str] = field(default_factory=list)
    true_value: str = DEFAULT_TRUE_OUTPUT
    false_value: str = DEFAULT_FALSE_OUTPUT

    _true_cases: set[str] = field(init=False, repr=False)
    _false_cases: set[str] = field(init=False, repr=False)

    def __post_init__(self):

        assert isinstance(self.true_cases, list), \
            f"true_cases must be a list, got {type(self.true_cases)}"

        assert all(isinstance(item, str) for item in self.true_cases), \
            f"All items in true_cases must be strings, got {self.true_cases}"

        assert len(self.true_cases) > 0, \
            "true_cases must not be empty"

        assert isinstance(self.false_cases, list), \
            f"false_cases must be a list, got {type(self.false_cases)}"

        assert all(isinstance(item, str) for item in self.false_cases), \
            f"All items in false_cases must be strings, got {self.false_cases}"

        assert len(self.false_cases) > 0, \
            "false_cases must not be empty"

        assert isinstance(self.extra_true_cases, list), \
            f"extra_true_cases must be a list, got {type(self.extra_true_cases)}"

        assert all(isinstance(item, str) for item in self.extra_true_cases), \
            f"All items in extra_true_cases must be strings, got {self.extra_true_cases}"

        assert isinstance(self.extra_false_cases, list), \
            f"extra_false_cases must be a list, got {type(self.extra_false_cases)}"

        assert all(isinstance(item, str) for item in self.extra_false_cases), \
            f"All items in extra_false_cases must be strings, got {self.extra_false_cases}"

        assert isinstance(self.true_value, str), \
            f"true_value must be a string, got {type(self.true_value)}"

        assert isinstance(self.false_value, str), \
            f"false_value must be a string, got {type(self.false_value)}"

        true_cases = self.true_cases + self.extra_true_cases
        false_cases = self.false_cases + self.extra_false_cases

        object.__setattr__(self, "_true_cases", set(true_cases))
        object.__setattr__(self, "_false_cases", set(false_cases))
        object.__setattr__(self, "datatype", "boolean")

    @override
    def clean_value(self, value: str | None) -> str | None:

        value_clean = value.lower().strip()
        istrue = value_clean in self._true_cases
        isfalse = value_clean in self._false_cases

        if not istrue and not isfalse:
            raise ValueError(f"Cannot parse '{value}' as boolean.")

        return self.true_value if istrue else self.false_value

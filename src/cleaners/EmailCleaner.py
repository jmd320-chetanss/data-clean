import re
import itertools
from dataclasses import dataclass, field
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class EmailCleaner(ColCleaner):

    max_parse_count: int = 1
    value_separator: str = ", "

    _email_pattern: re.Pattern = field(
        init=False,
        default=re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"),
    )

    def __post_init__(self):

        assert self.value_separator is not None, "value_separator cannot be None"

        assert self.max_parse_count > 0, "max_parse_count must be greater than 0"

    def clean_value(self, value: str | None) -> str | None:

        matches = itertools.islice(
            self._email_pattern.finditer(value), self.max_parse_count
        )
        results = [match.group(0).lower() for match in matches]

        if len(results) != 0:
            return self.value_separator.join(results)

        # If no email address is found, we will try to clean the value.
        # We don't do this at the start because we want to find the valid email first.

        # Replace consecutive '@' symbols with a single '@'
        value = re.sub(r"@{2,}", "@", value)

        # Replace consecutive '@' symbols with a single '.'
        value = re.sub(r"\.{2,}", ".", value)

        # Replace any space or underscores after '@' symbol with hyphen
        value = re.sub(
            r"(@[^ ]+)[ _]",
            lambda m: m.group(0).replace(" ", "").replace("_", ""),
            value,
        )

        matches = itertools.islice(
            self._email_pattern.finditer(value), self.max_parse_count
        )
        results = [match.group(0).lower() for match in matches]

        if len(results) != 0:
            return self.value_separator.join(results)

        raise ValueError(f"No valid email found in '{value}'")

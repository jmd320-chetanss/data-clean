import re
import itertools
from dataclasses import dataclass, field
from typing import override
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class EmailCleaner(ColCleaner):

    parse_count: int = 1
    value_separator: str = ", "

    _email_pattern = field(
        init=False,
        default=re.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'),
    )

    def __post_init__(self):

        assert self.value_separator is not None, \
            "value_separator cannot be None"

        assert self.parse_count > 0, \
            "parse_count must be greater than 0"

    @override
    def clean_value(self, value: str | None) -> str | None:

        if value is None:
            return None

        matches = itertools.islice(
            self._email_pattern.finditer(value), self.parse_count)
        results = [match.group(0).lower() for match in matches]

        return self.value_separator.join(results)

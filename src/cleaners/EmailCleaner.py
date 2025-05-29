from dataclasses import dataclass
from .ColCleaner import ColCleaner
import re
import itertools


@dataclass
class EmailCleaner(ColCleaner):

    parse_count: int = 1
    value_separator: str = ", "

    _email_pattern = re.compile(
        r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')

    def __post_init__(self):
        assert self.value_separator is not None, "value_separator cannot be None"
        assert self.parse_count > 0, "parse_count must be greater than 0"

    def _get_cleaner(self) -> callable:
        """
        Returns a function that cleans email addresses by stripping whitespace and converting to lowercase.
        """

        def _cleaner(value: str | None) -> str | None:
            value = self.preprocess(value)

            if value is None:
                return None

            matches = itertools.islice(
                self._email_pattern.finditer(value), self.parse_count)
            results = [match.group(0).lower() for match in matches]
            return self.value_separator.join(results)

        return _cleaner

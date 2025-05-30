from dataclasses import dataclass
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class NoneCleaner(ColCleaner):

    def clean_value(self, value: str | None) -> str | None:
        return value

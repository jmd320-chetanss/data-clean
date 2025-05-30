from dataclasses import dataclass
from .ColCleaner import ColCleaner
from typing import override


@dataclass(frozen=True)
class NoneCleaner(ColCleaner):

    @override
    def clean_value(self, value: str | None) -> str | None:
        return value

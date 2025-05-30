from dataclasses import dataclass
from .StringCleaner import StringCleaner


@dataclass(frozen=True)
class UuidCleaner(StringCleaner):
    pass

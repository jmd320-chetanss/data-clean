from dataclasses import dataclass
from .StringCleaner import StringCleaner


@dataclass(frozen=True)
class AutoCleaner(StringCleaner):
    pass

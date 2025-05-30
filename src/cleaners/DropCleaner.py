from dataclasses import dataclass
from .NoneCleaner import NoneCleaner


@dataclass(frozen=True)
class DropCleaner(NoneCleaner):
    pass

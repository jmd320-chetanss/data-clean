from dataclasses import dataclass, field
from typing import override
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class EnumCleaner(StringCleaner):
    pass

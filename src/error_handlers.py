from dataclasses import dataclass, KW_ONLY
from typing import Callable


@dataclass(frozen=True)
class ErrorContext:
    _: KW_ONLY

    col: str
    stage: str
    value: str | None
    error: Exception

    def __post_init__(self):
        assert isinstance(self.col, str), "Column name must be a string."
        assert len(self.col) > 0, "Column must not be empty."
        assert isinstance(
            self.value, (str, type(None))
        ), "Value must be a string or None."
        assert isinstance(self.error, Exception), "Error must be an Exception instance."


ErrorHandler = Callable[[ErrorContext], str | None]


def error_handler_raise(ctx: ErrorContext) -> str | None:
    raise RuntimeError(
        f"Error cleaning value '{ctx.value}' in column '{ctx.col}', error: {ctx.error}"
    )


def error_handler_value(ctx: ErrorContext) -> str | None:
    return ctx.value


def error_handler_none(_: ErrorContext) -> str | None:
    return None


error_handler_default = error_handler_raise

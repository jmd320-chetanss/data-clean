from dataclasses import dataclass, field
from dateutil import parser
from typing import Literal, override, ClassVar
from .ColCleaner import ColCleaner


@dataclass(frozen=True)
class DatetimeCleaner(ColCleaner):
    """
    Class to clean datetime columns in a DataFrame.
    """

    DEFAULT_DATETIME_FORMAT: ClassVar[str] = "%Y-%m-%d %H:%M:%S"
    DEFAULT_DATE_FORMAT: ClassVar[str] = "%Y-%m-%d"
    DEFAULT_TIME_FORMAT: ClassVar[str] = "%H:%M:%S"

    # Parse formats of the datetime
    parse_formats: str | list[str] = "%Y-%m-%d %H:%M:%S"

    # Format of the datetime
    format: Literal["datetime", "date", "time"] | str = "%Y-%m-%d %H:%M:%S"

    _parse_formats: list[str] = field(init=False, repr=False)
    _format: str = field(init=False, repr=False)

    def __post_init__(self):

        assert isinstance(
            self.format, str
        ), f"format must be a string, got {type(self.format)}"

        assert len(self.format) > 0, "format must not be empty"

        assert isinstance(
            self.parse_formats, (str, list)
        ), f"parse_formats must be a string or a list of strings, got {type(self.parse_formats)}"

        assert len(self.parse_formats) > 0, "parse_formats must not be empty"

        assert all(
            isinstance(fmt, str) for fmt in self.parse_formats
        ), f"All parse_formats must be strings, got {self.parse_formats}"

        assert all(
            len(fmt) > 0 for fmt in self.parse_formats
        ), f"All parse_formats must not be empty, got {self.parse_formats}"

        _parse_formats = (
            self.parse_formats
            if isinstance(self.parse_formats, list)
            else [self.parse_formats]
        )

        _format = self._get_format(self.format)

        object.__setattr__(self, "_parse_formats", _parse_formats)
        object.__setattr__(self, "_format", _format)
        object.__setattr__(self, "datatype", "timestamp")

    @override
    def clean_value(self, value: str | None) -> str | None:

        try:
            parsed_value = parser.parse(value)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot parse {type(value)} '{value}' as date")

        return parsed_value.strftime(self._format)

    def _get_format(self, format: str) -> str:
        """
        Returns a predefined format based on the specified format type.
        """

        if self.format == "datetime":
            return [self.DEFAULT_DATETIME_FORMAT]

        if self.format == "date":
            return [self.DEFAULT_DATE_FORMAT]

        if self.format == "time":
            return [self.DEFAULT_TIME_FORMAT]

        return [self.format]

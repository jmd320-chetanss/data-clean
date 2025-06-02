from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, ClassVar
from .ColCleaner import ColCleaner
from dataclean.src import utils


@dataclass(frozen=True)
class DatetimeCleaner(ColCleaner):
    """
    Class to clean datetime columns in a DataFrame.
    """

    DATETIME_FORMAT: ClassVar[str] = "%Y-%m-%d %H:%M:%S"
    DATE_FORMAT: ClassVar[str] = "%Y-%m-%d"
    TIME_FORMAT: ClassVar[str] = "%H:%M:%S"

    # Parse formats of the datetime
    parse_formats: list[str | Literal["datetime", "date", "time"]] = field(
        default_factory=lambda: ["datetime", "date"]
    )

    # Format of the datetime
    format: str | Literal["datetime", "date", "time"] = "datetime"

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

        # Clean parse formats
        parse_formats = [self._get_format(fmt) for fmt in self.parse_formats]
        parse_formats = utils.remove_duplicates(parse_formats)

        format = self._get_format(self.format)

        object.__setattr__(self, "_parse_formats", parse_formats)
        object.__setattr__(self, "_format", format)
        object.__setattr__(self, "datatype", "timestamp")

    def clean_value(self, value: str | None) -> str | None:

        parsed_value = None
        for format in self._parse_formats:
            try:
                parsed_value = datetime.strptime(value, format)
                break
            except ValueError:
                continue

        if parsed_value is None:
            raise ValueError(
                f"Cannot parse '{value}' with any of the formats: {self._parse_formats}"
            )

        return datetime.strftime(parsed_value, self._format)

    def _get_format(self, format: str) -> str:
        """
        Returns a predefined format based on the specified format type.
        """

        if format == "datetime":
            return self.DATETIME_FORMAT

        if format == "date":
            return self.DATE_FORMAT

        if format == "time":
            return self.TIME_FORMAT

        return format

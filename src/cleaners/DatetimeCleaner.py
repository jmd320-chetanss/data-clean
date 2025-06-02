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

    DATETIMEMS_FORMAT: ClassVar[str] = "%Y-%m-%d %H:%M:%S.%f"
    DATETIME_FORMAT: ClassVar[str] = "%Y-%m-%d %H:%M:%S"
    DATE_FORMAT: ClassVar[str] = "%Y-%m-%d"
    DATE_DMY_FORMAT: ClassVar[str] = "%d/%m/%Y"
    TIMEMS_FORMAT: ClassVar[str] = "%H:%M:%S.%f"
    TIME_FORMAT: ClassVar[str] = "%H:%M:%S"

    Formats = Literal["datetimems", "datetime", "date", "date_dmy", "timems", "time"]

    # Parse formats of the datetime
    parse_formats: list[str | Formats] = field(
        default_factory=lambda: ["datetimems", "datetime", "date", "date_dmy"]
    )

    # Format of the datetime
    format: str | Formats = "datetime"

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

        match format:
            case "datetimems":
                return self.DATETIMEMS_FORMAT

            case "datetime":
                return self.DATETIME_FORMAT

            case "date":
                return self.DATE_FORMAT

            case "date_dmy":
                return self.DATE_DMY_FORMAT

            case "timems":
                return self.TIMEMS_FORMAT

            case "time":
                return self.TIME_FORMAT

        return format

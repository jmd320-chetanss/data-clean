from dataclasses import dataclass, field
from .ColCleaner import ColCleaner
from typing import Literal, override
import phonenumbers


@dataclass
class PhoneCleaner(ColCleaner):
    """
    A class to clean phone numbers in a DataFrame column.

    Attributes:
        col_name (str): The name of the column to clean.
        df (pd.DataFrame): The DataFrame containing the column to clean.
    """

    # Output format for phone number
    fmt: Literal["E164", "INTERNATIONAL", "NATIONAL", "RFC3966"] = "E164"

    # Regions to consider for phone number validation in the specified order.
    regions: list[str] = field(
        default_factory=lambda: [
            "",  # Default or Global
            "US",  # United States
            "UK",  # United Kingdom (often includes Northern Ireland)
            "GB",  # Great Britain (England, Scotland, Wales)
            "CA",  # Canada
            "AU",  # Australia
            "NZ",  # New Zealand
            "IE",  # Ireland
            "IN",  # India
            "DE",  # Germany
            "FR",  # France
            "IT",  # Italy
            "ES",  # Spain
            "MX",  # Mexico
            "BR",  # Brazil
            "JP",  # Japan
            "KR",  # South Korea
            "CN",  # China
            "SG",  # Singapore
            "ZA",  # South Africa
            "AE",  # United Arab Emirates
            "SE",  # Sweden
            "NL",  # Netherlands
            "CH",  # Switzerland
            "BE",  # Belgium
        ]
    )

    _pn_format: phonenumbers.PhoneNumberFormat = field(init=False, repr=False)

    def __post_init__(self):

        assert self.fmt in [
            "E164",
            "INTERNATIONAL",
            "NATIONAL",
            "RFC3966",
        ], f"Invalid format: {self.fmt}. Must be one of 'E164', 'INTERNATIONAL', 'NATIONAL', or 'RFC3966'."

        assert isinstance(self.regions, list) and all(
            isinstance(region, str) for region in self.regions
        ), "Regions must be a list of strings."

        pn_format = self._convert_format(self.fmt)

        object.__setattr__(self, "_pn_format", pn_format)

    @override
    def clean_value(self, value: str | None) -> str | None:

        numobj: phonenumbers.PhoneNumber = None

        # Parsing phonenumber using every regions
        for region in self.regions:
            matcher = phonenumbers.PhoneNumberMatcher(
                value, region=region, leniency=phonenumbers.Leniency.POSSIBLE
            )

            if not matcher.has_next():
                continue

            numobj = matcher.next().number
            break

        if numobj is None:
            return None

        result = phonenumbers.format_number(numobj, num_format=self._pn_format)
        return result

    def _convert_format(self, format: str) -> phonenumbers.PhoneNumberFormat:
        """
        Convert a string format to a phonenumbers.PhoneNumberFormat enum.

        Args:
            format (str): The format string.

        Returns:
            phonenumbers.PhoneNumberFormat: The corresponding PhoneNumberFormat enum.
        """
        return {
            "E164": phonenumbers.PhoneNumberFormat.E164,
            "INTERNATIONAL": phonenumbers.PhoneNumberFormat.INTERNATIONAL,
            "NATIONAL": phonenumbers.PhoneNumberFormat.NATIONAL,
            "RFC3966": phonenumbers.PhoneNumberFormat.RFC3966,
        }[format]

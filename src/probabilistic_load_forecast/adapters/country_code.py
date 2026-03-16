"""Country code normalization adapters."""

import pycountry

from probabilistic_load_forecast.application.ports import CountryCodeNormalizer
from probabilistic_load_forecast.domain.exceptions import InvalidCountryCodeError
from probabilistic_load_forecast.domain.model import CountryCode


class PycountryCountryCodeNormalizer(CountryCodeNormalizer):
    """Normalize country names or alternate ISO codes into alpha-2 codes."""

    def normalize(self, value: str) -> CountryCode:
        normalized = value.strip().upper()

        if len(normalized) == 2 and pycountry.countries.get(alpha_2=normalized):
            return CountryCode(normalized)

        if len(normalized) == 3:
            country = pycountry.countries.get(alpha_3=normalized)
            if country:
                return CountryCode(country.alpha_2)

        try:
            country = pycountry.countries.lookup(value)
        except LookupError as exc:
            raise InvalidCountryCodeError(f"Unknown country: {value}") from exc

        return CountryCode(country.alpha_2)

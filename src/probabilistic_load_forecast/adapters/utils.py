from datetime import datetime, timezone
import pycountry


def to_utc(dt: datetime) -> datetime:
    """Convert any aware datetime to UTC."""
    if dt.tzinfo is None:
        raise ValueError("Naive datetime encountered; attach tzinfo first.")
    return dt.astimezone(timezone.utc)


def remove_tz_info(dt: datetime) -> datetime:
    """Remove the tz-information from a datetime object"""
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def normalize_country(value: str) -> str:
    """Return ISO 3166-1 alpha-2 code for a country name or code."""
    value = value.strip().upper()

    # If already an alpha-2 code (like AT), verify it exists
    if len(value) == 2 and pycountry.countries.get(alpha_2=value):
        return value

    # If alpha-3 (like AUT)
    if len(value) == 3:
        country = pycountry.countries.get(alpha_3=value)
        if country:
            return country.alpha_2

    # Otherwise try by name
    try:
        country = pycountry.countries.lookup(value)
        return country.alpha_2
    except LookupError as exc:
        raise ValueError(f"Unknown country: {value}") from exc

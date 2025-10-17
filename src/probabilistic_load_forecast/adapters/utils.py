from datetime import datetime, timezone


def to_utc(dt: datetime) -> datetime:
    """Convert any aware datetime to UTC."""
    if dt.tzinfo is None:
        raise ValueError("Naive datetime encountered; attach tzinfo first.")
    return dt.astimezone(timezone.utc)

def remove_tz_info(dt:datetime)-> datetime:
    """Remove the tz-information from a datetime object"""
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt

from datetime import datetime, timezone

def to_utc(dt: datetime) -> datetime:
    """Convert any aware datetime to UTC."""
    if dt.tzinfo is None:
        raise ValueError("Naive datetime encountered; attach tzinfo first.")
    return dt.astimezone(timezone.utc)
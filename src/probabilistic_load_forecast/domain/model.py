"""
This module contains the core business entities.
"""

from enum import StrEnum
from dataclasses import dataclass
from datetime import date, datetime

class Resolution(StrEnum):
    PT15M = "15min"
    PT1H = "1h"

class WeatherVariable(StrEnum):
    T2M = "t2m"
    U10 = "u10"
    V10 = "v10"
    TP = "tp"
    SSRD = "ssrd"

@dataclass(frozen=True)
class BiddingZone:
    eic_code: str          # "10YAT-APG------L"
    code: str              # "AT"
    display_name: str      # "Austria"
    country_code: str | None

@dataclass(frozen=True)
class WeatherArea:
    code: str   # "AT"

@dataclass(frozen=True)
class AreaMapping:
    bidding_zone: BiddingZone
    weather_area: WeatherArea

class IntervalStatistic(StrEnum):
    TOTAL = "total"
    MEAN = "mean"

@dataclass(frozen=True)
class TimeInterval:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("Use timezone-aware datetimes")
        if self.end <= self.start:
            raise ValueError("end must be after start")

@dataclass(frozen=True)
class LoadMeasurement:
    """
    Domain entity representing a single measurement of load data.
    """
    bidding_zone: BiddingZone

    interval: TimeInterval
    load_mw: float

@dataclass(frozen=True)
class ForecastIssue:
    bidding_zone: BiddingZone

    target_day: date
    issued_at: datetime
    resolution: Resolution

@dataclass(frozen=True)
class InstantWeatherValue:
    area: WeatherArea
    variable: WeatherVariable
    valid_at: datetime
    value: float

@dataclass(frozen=True)
class IntervalWeatherValue:
    area: WeatherArea
    variable: WeatherVariable
    interval: TimeInterval
    statistic: IntervalStatistic
    value: float

@dataclass(frozen=True)
class ForecastPoint:
    timestamp: datetime
    quantile: float
    value_mw: float

@dataclass(frozen=True)
class ProbabilisticForecast:
    issue: ForecastIssue
    model_version: str
    points: tuple[ForecastPoint, ...]

@dataclass(frozen=True)
class LoadSeries:
    """Domain entity representing a timeseries of load measurements."""
    bidding_zone: BiddingZone
    resolution: Resolution
    observations: tuple[LoadMeasurement, ...]

    def __post_init__(self) -> None:
        if not self.observations:
            return
        starts = [obs.interval.start for obs in self.observations]
        if starts != sorted(starts):
            raise ValueError("observations must be sorted by start time")
        if any(obs.bidding_zone != self.area for obs in self.observations):
            raise ValueError("all observations must belong to the same area")

@dataclass(frozen=True)
class Era5Series:
    """Domain entity representing a timeseries of load measurements."""
    area: WeatherArea
    resolution: Resolution
    observations: tuple[InstantWeatherValue | IntervalWeatherValue, ...]


# TODO: refactor out this classes
# @dataclass
# class LoadTimeseries:

#     data: pd.DataFrame
#     bidding_zone: str


# @dataclass
# class Era5Timeseries:

#     data: pd.Series
#     variable_name: str
#     stat: str

BIDDING_ZONE_REGISTRY = {
    "10YAT-APG------L": BiddingZone(
        eic_code="10YAT-APG------L",
        code="AT",
        display_name="Austria",
        country_code="AT",
    ),
}
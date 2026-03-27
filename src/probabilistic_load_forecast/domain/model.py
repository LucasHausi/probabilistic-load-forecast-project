"""
This module contains the core business entities.
"""

from enum import StrEnum
from dataclasses import dataclass
from datetime import date, datetime
import re

from probabilistic_load_forecast.domain.exceptions import (
    InvalidCountryCodeError,
    UnknownBiddingZoneError,
)

class Resolution(StrEnum):
    PT15M = "15min"
    PT1H = "1h"
    PT3H = "3h"

class WeatherVariable(StrEnum):
    T2M = "t2m"
    U10 = "u10"
    V10 = "v10"
    TP = "tp"
    SSRD = "ssrd"

class WeatherValueKind(StrEnum):
    INSTANT = "instant"
    INTERVAL_END = "interval_end"

@dataclass(frozen=True)
class CountryCode:
    value: str  # "AT"

    def __post_init__(self) -> None:
        normalized = self.value.strip().upper()
        if not re.fullmatch(r"[A-Z]{2}", normalized):
            raise InvalidCountryCodeError(
                "country code must be a valid ISO 3166-1 alpha-2 code"
            )
        object.__setattr__(self, "value", normalized)

    def __str__(self) -> str:
        return self.value

@dataclass(frozen=True)
class BiddingZone:
    eic_code: str          # "10YAT-APG------L"
    display_name: str      # "Austria"
    country_code: CountryCode

@dataclass(frozen=True)
class WeatherArea:
    code: CountryCode

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
        if any(obs.bidding_zone.country_code.value != self.bidding_zone.country_code.value for obs in self.observations):
            raise ValueError("all observations must belong to the same area")
    
    @classmethod
    def from_measurements(cls, measurements: list[LoadMeasurement]) -> "LoadSeries":
        if not measurements:
            raise ValueError("cannot build LoadSeries from empty measurements")

        ordered = tuple(sorted(measurements, key=lambda m: m.interval.start))
        first = ordered[0]
        return cls(
            bidding_zone=first.bidding_zone,
            resolution=Resolution.PT15M,
            observations=ordered,
        )

@dataclass(frozen=True)
class Era5Series:
    """Domain entity representing a timeseries of load measurements."""
    area: WeatherArea
    resolution: Resolution
    observations: tuple[InstantWeatherValue | IntervalWeatherValue, ...]
    variable: WeatherVariable

    def __post_init__(self) -> None:
        if any(obs.area.code != self.area.code for obs in self.observations):
            raise ValueError("all observations must belong to the same area")
        
        if any(obs.variable != self.variable for obs in self.observations):
            raise ValueError("all observations must of the same weather variable type")

BIDDING_ZONE_REGISTRY = {
    "10YAT-APG------L": BiddingZone(
        eic_code="10YAT-APG------L",
        display_name="Austria",
        country_code=CountryCode("AT"),
    ),
}

VARIABLE_VALUE_KIND = {
    WeatherVariable.T2M: WeatherValueKind.INSTANT,
    WeatherVariable.U10: WeatherValueKind.INSTANT,
    WeatherVariable.V10: WeatherValueKind.INSTANT,
    WeatherVariable.SSRD: WeatherValueKind.INTERVAL_END,
    WeatherVariable.TP: WeatherValueKind.INTERVAL_END,
}

def resolve_bidding_zone(eic_code):
    try:
        return BIDDING_ZONE_REGISTRY[eic_code]
    except KeyError as exc:
        raise UnknownBiddingZoneError(
            f"Unknown bidding zone code: {eic_code}"
        ) from exc

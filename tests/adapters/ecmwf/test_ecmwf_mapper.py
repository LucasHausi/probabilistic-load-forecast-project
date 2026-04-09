from datetime import datetime, timezone
from pathlib import Path

from probabilistic_load_forecast.adapters.ecmwf.mapper import ECMWFMapper
from probabilistic_load_forecast.domain.model import (
    CountryCode,
    Era5Series,
    TimeInterval,
    WeatherArea,
    WeatherVariable,
    Resolution,
)


def test_ecmwf_mapper_returns_country_average_series_for_t2m():
    mapper = ECMWFMapper()
    area = WeatherArea(code=CountryCode("AT"))

    result = mapper.map(
        [
            str(Path("tests/fixtures/2026-03-24.grib2")),
            str(Path("tests/fixtures/2026-03-25.grib2")),
        ],
        area=area,
        weather_variable=WeatherVariable.T2M,
        interval=TimeInterval(
            start=datetime(2026, 3, 25, 0, 0, tzinfo=timezone.utc),
            end=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
        ),
    )

    assert isinstance(result, Era5Series)
    assert result.area == area
    assert result.variable == WeatherVariable.T2M
    assert result.resolution == Resolution.PT1H
    assert len(result.observations) == 48
    assert result.observations[0].valid_at == datetime(
        2026, 3, 25, 0, 0, tzinfo=timezone.utc
    )
    assert result.observations[-1].valid_at == datetime(
        2026, 3, 26, 23, 0, tzinfo=timezone.utc
    )

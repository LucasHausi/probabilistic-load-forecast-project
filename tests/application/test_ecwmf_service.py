from datetime import datetime, timezone

from probabilistic_load_forecast.domain.model import (
    CountryCode,
    Era5Series,
    InstantWeatherValue,
    Resolution,
    TimeInterval,
    WeatherArea,
    WeatherVariable,
)
from probabilistic_load_forecast.application.services import (
    ImportWeatherForecast
)

from probabilistic_load_forecast.application.ports import(
    DataProvider
)

class FakeECMWFProvider(DataProvider):
    def __init__(self, result):
        self.result = result
        self.calls = []

    def get_data(self, interval, **kwargs):
        self.calls.append((interval, kwargs))
        return self.result


class FakeEra5Repository:
    def __init__(self):
        self.calls = []

    def add(self, series, schema="public"):
        self.calls.append((series, schema))


def test_import_weather_forecast_stores_series_from_provider():
    start = datetime(2026, 3, 26, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 26, 2, 0, tzinfo=timezone.utc)
    interval = TimeInterval(start=start, end=end)

    area = WeatherArea(code=CountryCode("AT"))

    expected_series = Era5Series(
        area=area,
        resolution=Resolution.PT1H,
        variable=WeatherVariable.T2M,
        observations=(
            InstantWeatherValue(
                area=area,
                variable=WeatherVariable.T2M,
                valid_at=datetime(2026, 3, 26, 0, 0, tzinfo=timezone.utc),
                value=270.0,
            ),
            InstantWeatherValue(
                area=area,
                variable=WeatherVariable.T2M,
                valid_at=datetime(2026, 3, 26, 1, 0, tzinfo=timezone.utc),
                value=272.0,
            ),
        ),
    )

    provider = FakeECMWFProvider(result=expected_series)
    repo = FakeEra5Repository()

    service = ImportWeatherForecast(provider=provider, repo=repo)
    service(interval, area)

    assert provider.calls == [(interval, {"area": area})]
    assert repo.calls == [(expected_series, "public")]

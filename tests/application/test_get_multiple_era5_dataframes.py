from datetime import datetime, timezone

import pandas as pd
from pandas.testing import assert_frame_equal

from probabilistic_load_forecast.application.services.cds_services import (
    GetMultipleERA5DataFrameFromDB,
)
from probabilistic_load_forecast.domain.model import (
    CountryCode,
    TimeInterval,
    WeatherArea,
    WeatherVariable,
    Era5Series,
    Resolution,
    InstantWeatherValue
)


class FakeEra5Repository:
    def __init__(self, results) -> None:
        self.results = results
        self.calls = []

    def get(self, interval, area, variable, schema="public"):
        self.calls.append((interval, area, variable, schema))
        return self.results[variable]


def test_get_multiple_era5_returns_dataframe():
    interval = TimeInterval(
        start=datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc),
        end=datetime(2025, 7, 13, 0, 30, tzinfo=timezone.utc),
    )
    area = WeatherArea(CountryCode("AT"))
    variables = [WeatherVariable.T2M]

    repo_return = {
        WeatherVariable.T2M : Era5Series(
        area=area,
        resolution=Resolution.PT1H,
        observations=(
            InstantWeatherValue(
                area=area,
                valid_at=datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc),
                variable=WeatherVariable.T2M,
                value=12.5
            ),
        ),
        variable=WeatherVariable.T2M
    )
    }

    expected_frames = {
        WeatherVariable.T2M: pd.DataFrame(
            {
                "value": [12.5],
            },
            index=pd.DatetimeIndex([datetime(2025, 7, 13, 0, 0)], tz=timezone.utc, name="valid_time")
        ),
    }

    repo = FakeEra5Repository(repo_return)
    usecase = GetMultipleERA5DataFrameFromDB(repo)

    result = usecase(variables=variables, area=area, interval=interval)

    assert set(result) == set(expected_frames)
    for variable in variables:
        assert_frame_equal(result[variable], expected_frames[variable])

    assert repo.calls == [
        (interval, area, WeatherVariable.T2M, "public"),
    ]

from datetime import datetime, date, timezone, timedelta
from ecmwf.opendata import Client
import pandas as pd

from probabilistic_load_forecast.domain.model import WeatherVariable, TimeInterval
from pathlib import Path

WEATHER_VARIABLE_MAPPING = {
    WeatherVariable.T2M: "2t",
    WeatherVariable.U10: "10u",
    WeatherVariable.V10: "10v",
    WeatherVariable.SSRD: "ssrd",
    WeatherVariable.TP: "tp",
}


class ECMWFAPIClient:
    def __init__(self, target_dir: Path, client) -> None:
        self.client = client
        self.target_dir = target_dir

    def forecast_issue_dates_for(self, interval: TimeInterval) -> list[date]:
        # Treat the requested interval as [start, end).
        # If end is exactly 00:00, that calendar day is excluded.
        start_day = interval.start.astimezone(timezone.utc).date()
        last_target_day = (
            interval.end.astimezone(timezone.utc) - timedelta(microseconds=1)
        ).date()

        day_count = (last_target_day - start_day).days + 1

        return [
            start_day + timedelta(days=i) - timedelta(days=1) for i in range(day_count)
        ]

    def fetch(
        self,
        interval: TimeInterval,
        weather_variables: list[WeatherVariable],
    ):
        forecast_dates = self.forecast_issue_dates_for(interval)
        forecast_variables = [
            WEATHER_VARIABLE_MAPPING[variable] for variable in weather_variables
        ]

        result_paths = []

        for forecast_date in forecast_dates:
            file_path = self.target_dir / f"{forecast_date.strftime('%Y-%m-%d')}.grib2"
            
            result_paths.append(str(file_path))

            self.client.retrieve(
                date=forecast_date,
                time=12,  # 12 UTC run
                type="fc",  # forecast
                step=[12, 15, 18, 21, 24, 27, 30, 33, 36],
                param=forecast_variables,
                target=file_path,
            )

        return result_paths


from typing import Any

from probabilistic_load_forecast.domain.model import(
    TimeInterval,
    Era5Series,
    WeatherArea
)

from probabilistic_load_forecast.application.ports import(
    DataProvider
)

class ImportWeatherForecast():
    def __init__(self, provider: DataProvider, repo):
        self.dataprovider = provider
        self.repo = repo
    
    def __call__(self, interval: TimeInterval, area: WeatherArea) -> None:
        series = self.dataprovider.get_data(interval, area=area)
        self.repo.add(series)
from typing import Any

from probabilistic_load_forecast.domain.model import(
    TimeInterval,
    Era5Series,
    WeatherArea,
    WeatherVariable,
)

from probabilistic_load_forecast.application.ports import(
    DataProvider
)

class ImportWeatherForecast():
    def __init__(self, provider: DataProvider, repo):
        self.dataprovider = provider
        self.repo = repo
    
    def __call__(
        self,
        interval: TimeInterval,
        area: WeatherArea,
        weather_variable: WeatherVariable,
    ) -> None:
        series = self.dataprovider.get_data(
            interval,
            area=area,
            weather_variable=weather_variable,
        )
        self.repo.add(series)

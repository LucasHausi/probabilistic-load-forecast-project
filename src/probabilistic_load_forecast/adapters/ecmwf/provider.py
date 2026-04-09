from probabilistic_load_forecast.application.ports import(
    DataProvider
)
from probabilistic_load_forecast.domain.model import (
    TimeInterval,
    Era5Series,
    WeatherVariable,
)

class ECMWFDataProvider(DataProvider):
    def __init__(self, fetcher, mapper) -> None:
        self.fetcher = fetcher
        self.mapper = mapper
    
    def get_data(self, interval: TimeInterval, **kwargs) -> Era5Series:
        weather_variable = kwargs["weather_variable"]
        raw_data = self.fetcher.fetch(
            interval,
            weather_variables=[weather_variable],
        )
        mapped_data = self.mapper.map(
            raw_data,
            interval=interval,
            area=kwargs["area"],
            weather_variable=weather_variable,
        )
        return mapped_data


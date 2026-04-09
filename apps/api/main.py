from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv
from pydantic import AwareDatetime

from fastapi import FastAPI
from fastapi import Depends
import uvicorn

from probabilistic_load_forecast import config
from probabilistic_load_forecast.adapters.db import (
    EntsoePostgreRepository,
    Era5PostgreRepository,
    ForecastMetadataRepository
)
from probabilistic_load_forecast.adapters.country_code import (
    PycountryCountryCodeNormalizer,
)

from probabilistic_load_forecast.application.services import (
    GetActualLoadData,
    GetERA5DataFromDB,
    GetLatestCommonTimestamp
)

from probabilistic_load_forecast.domain.model import (
    resolve_bidding_zone,
    TimeInterval,
    WeatherArea,
    WeatherVariable,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


@lru_cache
def get_load_repository() -> EntsoePostgreRepository:
    return EntsoePostgreRepository(config.get_postgre_uri())


@lru_cache
def get_era5_repository() -> Era5PostgreRepository:
    return Era5PostgreRepository(config.get_postgre_uri())


@lru_cache
def get_country_code_normalizer() -> PycountryCountryCodeNormalizer:
    return PycountryCountryCodeNormalizer()

@lru_cache
def get_forecast_metadata_repository() -> ForecastMetadataRepository:
    return ForecastMetadataRepository(config.get_postgre_uri())

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/load-data")
def get_load_data(
    start: AwareDatetime,
    end: AwareDatetime,
    eic_code: str,
    repo: EntsoePostgreRepository = Depends(get_load_repository),
):
    bidding_zone = resolve_bidding_zone(eic_code)

    service = GetActualLoadData(repo)

    return service(start, end, bidding_zone)


@app.get("/weather-data")
def get_weather_data(
    start: AwareDatetime,
    end: AwareDatetime,
    variable: WeatherVariable,
    area_code: str = "AT",
    repo: Era5PostgreRepository = Depends(get_era5_repository),
    country_code_normalizer: PycountryCountryCodeNormalizer = Depends(
        get_country_code_normalizer
    ),
):

    service = GetERA5DataFromDB(repo)

    interval = TimeInterval(start=start, end=end)
    area = WeatherArea(code=country_code_normalizer.normalize(area_code))

    return service(
        variable=variable,
        area=area,
        interval=interval,
    )

@app.get("/latest-common-timestamp")
def get_latest_common_timestamp(repo: ForecastMetadataRepository = Depends(get_forecast_metadata_repository)):
    service = GetLatestCommonTimestamp(repo)
    return  {"timestamp": service()}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
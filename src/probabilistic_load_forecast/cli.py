import argparse
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path

import cdsapi
from ecmwf.opendata import Client as ECMWFOpenDataClient
from dotenv import load_dotenv

from probabilistic_load_forecast import config
from probabilistic_load_forecast.adapters.cds import CDSAPIClient, CDSConfig, CDSDataProvider, FileRepository
from probabilistic_load_forecast.adapters.country_code import PycountryCountryCodeNormalizer
from probabilistic_load_forecast.adapters.db import EntsoePostgreRepository, Era5PostgreRepository
from probabilistic_load_forecast.adapters.ecmwf.api_client import ECMWFAPIClient
from probabilistic_load_forecast.adapters.ecmwf.mapper import ECMWFMapper
from probabilistic_load_forecast.adapters.ecmwf.provider import ECMWFDataProvider
from probabilistic_load_forecast.adapters.entsoe import EntsoeAPIClient, EntsoeDataProvider, EntsoeFetcher, XmlLoadMapper
from probabilistic_load_forecast.application.services import (
    CreateCDSCountryAverages,
    GetActualLoadData,
    GetERA5DataFromCDSStore,
    GetERA5DataFromDB,
    ImportHistoricalLoadData,
    ImportWeatherForecast,
)
from probabilistic_load_forecast.domain.model import (
    TimeInterval,
    WeatherArea,
    WeatherVariable,
    resolve_bidding_zone,
)

ROOT_DIR = Path(__file__).resolve().parents[2]

def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def load_env() -> None:
    if not load_dotenv(ROOT_DIR / ".env"):
        raise FileNotFoundError("Could not load .env file in project root.")

def build_entsoe_provider() -> EntsoeDataProvider:
    client = EntsoeAPIClient(
        endpoint=config.get_entsoe_url(),
        security_token=config.get_entsoe_security_token(),
    )
    return EntsoeDataProvider(EntsoeFetcher(client), XmlLoadMapper())

def build_load_repo() -> EntsoePostgreRepository:
    return EntsoePostgreRepository(config.get_postgre_uri())

def build_weather_repo() -> Era5PostgreRepository:
    return Era5PostgreRepository(config.get_postgre_uri())

def build_cds_file_repo() -> FileRepository:
    return FileRepository()

def build_cds_provider() -> CDSDataProvider:
    client = cdsapi.Client(
        url=config.get_cdsapi_url(),
        key=config.get_cdsapi_key(),
        wait_until_complete=False,
    )
    cfg = CDSConfig(
        dataset="reanalysis-era5-land",
        variable=[
            "2m_temperature",
            "surface_solar_radiation_downwards",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "total_precipitation",
        ],
        area=[49.05, 9.5, 46.35, 17.17],
        field_limit=12000,
    )
    return CDSDataProvider(fetcher=CDSAPIClient(client=client, config=cfg))

def build_ecmwf_provider(target_dir: Path) -> ECMWFDataProvider:
    target_dir.mkdir(parents=True, exist_ok=True)
    client = ECMWFOpenDataClient()
    return ECMWFDataProvider(
        fetcher=ECMWFAPIClient(target_dir=target_dir, client=client),
        mapper=ECMWFMapper(),
    )

def to_json(value) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, default=str, indent=2)

def cmd_load_import(args: argparse.Namespace) -> int:
    service = ImportHistoricalLoadData(build_entsoe_provider(), build_load_repo())
    interval = TimeInterval(start=parse_dt(args.start), end=parse_dt(args.end))
    service(interval)
    return 0

def cmd_load_get(args: argparse.Namespace) -> int:
    service = GetActualLoadData(build_load_repo())
    result = service(parse_dt(args.start), parse_dt(args.end), resolve_bidding_zone(args.eic_code))
    print(to_json(result))
    return 0

def cmd_weather_get_db(args: argparse.Namespace) -> int:
    normalizer = PycountryCountryCodeNormalizer()
    service = GetERA5DataFromDB(build_weather_repo())
    result = service(
        variable=WeatherVariable(args.variable),
        area=WeatherArea(code=normalizer.normalize(args.area_code)),
        interval=TimeInterval(start=parse_dt(args.start), end=parse_dt(args.end)),
    )
    print(to_json(result))
    return 0

def cmd_weather_fetch_store(args: argparse.Namespace) -> int:
    interval = TimeInterval(start=parse_dt(args.start), end=parse_dt(args.end))

    fetch_service = GetERA5DataFromCDSStore(build_cds_provider())
    # downloaded_paths = [
    #     "era5_2025_10.nc",
    #     "era5_2025_11.nc",
    #     "era5_2025_12.nc",
    #     "era5_2026_01.nc",
    #     "era5_2026_02.nc",
    #     "era5_2026_03.nc"
    # ]
    downloaded_paths = fetch_service(interval)

    aggregate_service = CreateCDSCountryAverages(
        build_cds_file_repo(),
        build_weather_repo(),
        PycountryCountryCodeNormalizer(),
    )
    aggregate_service(interval)

    print(
        to_json(
            {
                "status": "ok",
                "downloaded_files": downloaded_paths,
                "download_count": len(downloaded_paths),
                "stored_interval": {
                    "start": interval.start,
                    "end": interval.end,
                },
            }
        )
    )
    return 0

def cmd_weather_store_averages(args: argparse.Namespace) -> int:
    interval = TimeInterval(start=parse_dt(args.start), end=parse_dt(args.end))

    service = CreateCDSCountryAverages(
        build_cds_file_repo(),
        build_weather_repo(),
        PycountryCountryCodeNormalizer()
    )

    service(interval)

    print(
        to_json(
            {
                "status": "ok",
                "stored_interval": {
                    "start": interval.start,
                    "end": interval.end,
                },
            }
        )
    )
    return 0

def cmd_weather_import_forecast(args: argparse.Namespace) -> int:
    interval = TimeInterval(start=parse_dt(args.start), end=parse_dt(args.end))
    normalizer = PycountryCountryCodeNormalizer()

    service = ImportWeatherForecast(
        build_ecmwf_provider(Path(args.target_dir)),
        build_weather_repo(),
    )
    area = WeatherArea(code=normalizer.normalize(args.area_code))
    variable = WeatherVariable(args.variable)

    service(interval=interval, area=area, weather_variable=variable)

    print(
        to_json(
            {
                "status": "ok",
                "stored_interval": {
                    "start": interval.start,
                    "end": interval.end,
                },
                "area_code": area.code.value,
                "variable": variable.value,
                "target_dir": str(Path(args.target_dir)),
            }
        )
    )
    return 0

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="plf")
    sub = parser.add_subparsers(dest="command", required=True)

    load = sub.add_parser("load")
    load_sub = load.add_subparsers(dest="load_command", required=True)

    load_import = load_sub.add_parser("import")
    load_import.add_argument("--start", required=True)
    load_import.add_argument("--end", required=True)
    load_import.set_defaults(handler=cmd_load_import)

    load_get = load_sub.add_parser("get")
    load_get.add_argument("--start", required=True)
    load_get.add_argument("--end", required=True)
    load_get.add_argument("--eic-code", required=True)
    load_get.set_defaults(handler=cmd_load_get)

    weather = sub.add_parser("weather")
    weather_sub = weather.add_subparsers(dest="weather_command", required=True)

    weather_get = weather_sub.add_parser("get-db")
    weather_get.add_argument("--start", required=True)
    weather_get.add_argument("--end", required=True)
    weather_get.add_argument(
        "--variable",
        required=True,
        choices=[variable.value for variable in WeatherVariable],
    )
    weather_get.add_argument("--area-code", default="AT")
    weather_get.set_defaults(handler=cmd_weather_get_db)

    weather_fetch_store = weather_sub.add_parser("fetch-store")
    weather_fetch_store.add_argument("--start", required=True)
    weather_fetch_store.add_argument("--end", required=True)
    weather_fetch_store.set_defaults(handler=cmd_weather_fetch_store)

    weather_store_averages = weather_sub.add_parser("store-averages")
    weather_store_averages.add_argument("--start", required=True)
    weather_store_averages.add_argument("--end", required=True)
    weather_store_averages.set_defaults(handler=cmd_weather_store_averages)

    weather_import_forecast = weather_sub.add_parser("import-forecast")
    weather_import_forecast.add_argument("--start", required=True)
    weather_import_forecast.add_argument("--end", required=True)
    weather_import_forecast.add_argument(
        "--variable",
        required=True,
        choices=[variable.value for variable in WeatherVariable],
    )
    weather_import_forecast.add_argument("--area-code", default="AT")
    weather_import_forecast.add_argument(
        "--target-dir",
        default=str(ROOT_DIR / "data" / "ecmwf"),
    )
    weather_import_forecast.set_defaults(handler=cmd_weather_import_forecast)

    return parser

def main(argv: list[str] | None = None) -> int:
    load_env()
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)

if __name__ == "__main__":
    raise SystemExit(main())

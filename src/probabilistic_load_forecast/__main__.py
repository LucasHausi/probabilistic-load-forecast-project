"""
This module currently orchestrates the logic for a simple POC
"""

import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import cdsapi
from dotenv import load_dotenv

from probabilistic_load_forecast import config

from probabilistic_load_forecast.adapters.entsoe import (
    EntsoeDataProvider,
    EntsoeFetcher,
    XmlLoadMapper,
    EntsoeAPIClient,
)

from probabilistic_load_forecast.adapters.db import PostgreRepository

from probabilistic_load_forecast.application.services import (
    FetchAndStoreMeasurements,
    GetActualLoadData,
)

from probabilistic_load_forecast.adapters.cds import (
    CDSAPIClient,
    CDSConfig,
    CDSDataProvider,
    CDSMapper,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def write_results_to_debug_file(results):
    """A function to generate debug files."""
    with open("debug.txt", mode="w", encoding="UTF-8") as f:
        #     f.write(results)
        for item in results:
            f.write(str(item) + "\n")

    # min_date = min(results, key= lambda x: x.timestamp)
    # max_date = max(results, key= lambda x: x.timestamp)
    # print(f"Min date: {min_date}")
    # print(f"Max date: {max_date}")


def main():
    """Entry point for the function calls

    Raises:
        FileNotFoundError: When the .env File is not found
    """
    if load_dotenv(".env"):
        enstoe_api_client = EntsoeAPIClient(
            endpoint=config.get_entsoe_url(),
            security_token=config.get_entsoe_security_token(),
        )
        entsoe_fetcher = EntsoeFetcher(enstoe_api_client)
        mapper = XmlLoadMapper()
        entsoe_repo = EntsoeDataProvider(entsoe_fetcher, mapper)
        postgres_repo = PostgreRepository(config.get_postgre_uri())

        # Creating the cds client
        cds_client = cdsapi.Client(
            url=config.get_cdsapi_url(),
            key=config.get_cdsapi_key(),
            wait_until_complete=False,  # So we can batch multiple polls
        )
        cds_config = CDSConfig(
            dataset="reanalysis-era5-land",
            variable=["2m_temperature", "snow_cover", "surface_net_solar_radiation"],
            area=[49.03, 9.5, 46.35, 17.17],  # Bounding box for austria
            field_limit=12000,
        )
        cds_api_client = CDSAPIClient(client=cds_client, config=cds_config)
        cds_mapper = CDSMapper()
        cds_repo = CDSDataProvider(fetcher=cds_api_client, mapper=cds_mapper)

        # ----------------------------------------------------------------
        #                    Testing the CDS Fetching
        # ----------------------------------------------------------------
        # area_tz = ZoneInfo(os.getenv("AREA_TZ_ENV", "Europe/Vienna"))

        # start_local = datetime(2018, 10, 1, 0, 0, tzinfo=area_tz)
        # end_local = datetime(2018, 12, 31, 0, 0, tzinfo=area_tz)
        # cds_repo.get_data(start=start_local, end=end_local)

        # ----------------------------------------------------------------
        #                    Testing the ENTSOE Fetching
        # ----------------------------------------------------------------

        area_tz = ZoneInfo(os.getenv("AREA_TZ_ENV", "Europe/Vienna"))

        # use_case = FetchAndStoreMeasurements(entsoe_repo, postgres_repo)
        # start_local = datetime(2018, 10, 1, 0, 0, tzinfo=area_tz)
        # end_local = datetime.now(area_tz)
        # results = use_case(start_local, end_local)
        # print(results)

        use_case = GetActualLoadData(postgres_repo)
        start_local = datetime(2018, 10, 1, 0, 0, tzinfo=area_tz)
        end_local = datetime(2018, 10, 1, 1, 7, tzinfo=area_tz)
        print(use_case(start_local, end_local))

    else:
        raise FileNotFoundError(
            "Could not load .env file." "Please ensure it exists in the project root."
        )


if __name__ == "__main__":
    main()

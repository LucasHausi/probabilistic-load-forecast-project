"""
This module currently orchestrates the logic for a simple POC
"""
import os
import logging

import cdsapi

from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from probabilistic_load_forecast.infrastructure.entsoe.repository import EntsoeRepository
from probabilistic_load_forecast.infrastructure.entsoe.fetcher import EntsoeFetcher
from probabilistic_load_forecast.infrastructure.entsoe.mapper import XmlLoadMapper
from probabilistic_load_forecast.infrastructure.entsoe.api_client import EntsoeAPIClient

from probabilistic_load_forecast.infrastructure.db.repository import PostgreRepository
from probabilistic_load_forecast.application.use_cases import FetchAndStoreMeasurements

from probabilistic_load_forecast.infrastructure.cds.api_client import CDSAPIClient
from probabilistic_load_forecast.infrastructure.cds.repository import CDSRepository
from probabilistic_load_forecast.infrastructure.cds.mapper import CDSMapper

from probabilistic_load_forecast.infrastructure.cds.api_client import CDSConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def write_results_to_debug_file(results):
    """A function to generate debug files.
    """
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
        enstoe_api_client = EntsoeAPIClient(endpoint=os.getenv("ENTSOE_BASE_URL"),
                        security_token=os.getenv("ENTSOE_SECURITY_TOKEN"))
        entsoe_fetcher = EntsoeFetcher(enstoe_api_client)
        mapper = XmlLoadMapper()
        entsoe_repo = EntsoeRepository(entsoe_fetcher, mapper)
        postgres_repo = PostgreRepository(os.getenv("PG_DSN"))

        # Creating the cds client
        cds_client = cdsapi.Client(
            url=os.getenv("CDSAPI_URL"),
            key=os.getenv("CDSAPI_KEY"),
            wait_until_complete=False # So we can batch multiple polls
        )
        cds_config = CDSConfig(
            dataset="reanalysis-era5-land",
            variable=[
                "2m_temperature",
                "snow_cover",
                "surface_net_solar_radiation"
            ],
            area=[49.03, 9.5, 46.35, 17.17], # Bounding box for austria
            field_limit=12000
        )
        cds_api_client = CDSAPIClient(client=cds_client, config=cds_config)
        cds_mapper = CDSMapper()
        cds_repo = CDSRepository(fetcher=cds_api_client, mapper=cds_mapper)
        # cds_api_client.fetch_data(
        #     year="2021",
        #     month="02",
        #     day=[
        #         "01", "02", "03",
        #         "04", "05", "06",
        #         "07", "08", "09",
        #         "10", "11", "12",
        #         "13", "14", "15",
        #         "16", "17", "18",
        #         "19", "20", "21",
        #         "22", "23", "24",
        #         "25", "26", "27",
        #         "28"],
        #     time=[
        #         "00:00", "01:00", "02:00",
        #         "03:00", "04:00", "05:00",
        #         "06:00", "07:00", "08:00",
        #         "09:00", "10:00", "11:00",
        #         "12:00", "13:00", "14:00",
        #         "15:00", "16:00", "17:00",
        #         "18:00", "19:00", "20:00",
        #         "21:00", "22:00", "23:00"],
        #     filename="test.grib"
        # )

        # ----------------------------------------------------------------
        #                    Testing the CDS Fetching
        # ----------------------------------------------------------------
        area_tz = ZoneInfo(os.getenv("AREA_TZ_ENV", "Europe/Vienna"))

        start_local = datetime(2018, 10, 1, 0, 0, tzinfo=area_tz)
        end_local =  datetime(2019, 3, 31, 0, 0, tzinfo=area_tz)
        cds_repo.get_data(start=start_local, end=end_local)

        # ----------------------------------------------------------------
        #                    Testing the ENTSOE Fetching
        # ----------------------------------------------------------------

        # use_case = FetchAndStoreMeasurements(entsoe_repo, postgres_repo)


        # area_tz = ZoneInfo(os.getenv("AREA_TZ_ENV", "Europe/Vienna"))

        # start_local = datetime(2018, 10, 1, 1, 0, tzinfo=area_tz)
        # end_local =  datetime.now(area_tz)
        # results = use_case(start_local, end_local)
        # print(results)

    else:
        raise FileNotFoundError(
            "Could not load .env file."
            "Please ensure it exists in the project root."
        )

if __name__ == "__main__":
    main()

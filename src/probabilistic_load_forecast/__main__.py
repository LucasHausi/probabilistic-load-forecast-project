"""
This module currently orchestrates the logic for a simple POC
"""
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from probabilistic_load_forecast.infrastructure.entsoe.repository import EntsoeRepository
from probabilistic_load_forecast.infrastructure.entsoe.fetcher import EntsoeFetcher
from probabilistic_load_forecast.infrastructure.entsoe.mapper import XmlLoadMapper
from probabilistic_load_forecast.infrastructure.entsoe.api_client import EntsoeAPIClient

from probabilistic_load_forecast.infrastructure.db.repository import PostgreRepository
from probabilistic_load_forecast.application.use_cases import FetchAndStoreMeasurements

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
        use_case = FetchAndStoreMeasurements(entsoe_repo, postgres_repo)


        area_tz = ZoneInfo(os.getenv("AREA_TZ_ENV", "Europe/Vienna"))

        start_local = datetime(2018, 10, 1, 1, 0, tzinfo=area_tz)
        end_local =  datetime.now(area_tz)
        results = use_case(start_local, end_local)
        print(results)

    else:
        raise FileNotFoundError(
            "Could not load .env file."
            "Please ensure it exists in the project root."
        )

if __name__ == "__main__":
    main()

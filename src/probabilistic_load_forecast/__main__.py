import os
from dotenv import load_dotenv
from probabilistic_load_forecast.repository.repository import EntsoeRepository
from probabilistic_load_forecast.repository.fetcher import EntsoeFetcher
from probabilistic_load_forecast.repository.mapper import XML_Load_Mapper
from probabilistic_load_forecast.infrastructure.api_client import EntsoeAPIClient

def main():
    if load_dotenv(".env"):
        enstoe_api_client = EntsoeAPIClient(endpoint=os.getenv("ENTSOE_BASE_URL"),
                        security_token=os.getenv("ENTSOE_SECURITY_TOKEN"))
        entsoe_fetcher = EntsoeFetcher(enstoe_api_client)
        mapper = XML_Load_Mapper()
        repository = EntsoeRepository(entsoe_fetcher, mapper)

        query_params = {
            "documentType":"A65",
            "processType":"A16",
            "outBiddingZone_Domain":"10YAT-APG------L",
            "periodStart":"202507130000",
            "periodEnd":"202507140000"
        }
        print(repository.get_data(query_params))

if __name__ == "__main__":
    main()
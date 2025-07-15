import os
from dotenv import load_dotenv
from probabilistic_load_forecast.repository.fetcher import EntsoeFetcher
from probabilistic_load_forecast.infrastructure.api_client import EntsoeAPIClient

def main():
    if load_dotenv(".env"):
        enstoe_client = EntsoeAPIClient(endpoint=os.getenv("ENTSOE_BASE_URL"),
                        security_token=os.getenv("ENTSOE_SECURITY_TOKEN"))
        entsoe_fetcher = EntsoeFetcher(enstoe_client)
        params = {
            "documentType":"A65",
            "processType":"A16",
            "outBiddingZone_Domain":"10YAT-APG------L",
            "periodStart":"202507130000",
            "periodEnd":"202507140000"
        }
        print(entsoe_fetcher.get_load_data(params))

if __name__ == "__main__":
    main()
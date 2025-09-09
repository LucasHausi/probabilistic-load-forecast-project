"""
This module contains the basic logic to fetch data from the public ENTSOE API Endpoints.
"""
import logging
import requests

TIMEOUT = 30
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class EntsoeAPIClient():
    def __init__(self, endpoint, security_token):
        self._endpoint = endpoint
        self._security_token = security_token
        self._timeout = TIMEOUT


    def fetch_load_data(self, params):
        params["securityToken"] = self._security_token
        try:
            response = requests.get(url = self._endpoint, params=params, timeout=self._timeout)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.warning("An error occured while fetching the load data: "
            "Parameters: %s"
            "Error: %s", params, e)
            return None

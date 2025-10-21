"""This module handles configuration retrieval from environment variables."""

import os


def get_postgre_uri() -> str:
    """Fetches the PostgreSQL DSN from environment variables."""
    postgre_uri = os.getenv("PG_DSN")
    if postgre_uri is None:
        raise ValueError(
            "PostgreSQL DSN is missing. Set the PG_DSN environment variable."
        )
    return postgre_uri


def get_entsoe_url() -> str:
    """Fetches the ENTSOE base URL from environment variables."""
    entsoe_url = os.getenv("ENTSOE_BASE_URL")
    if entsoe_url is None:
        raise ValueError(
            "ENTSOE base URL is missing. Set the ENTSOE_BASE_URL environment variable."
        )
    return entsoe_url


def get_entsoe_security_token():
    """Fetches the ENTSOE security token from environment variables."""
    print(" ")
    entsoe_token = os.getenv("ENTSOE_SECURITY_TOKEN")
    if entsoe_token is None:
        raise ValueError(
            "ENTSOE security token is missing. Set the ENTSOE_SECURITY_TOKEN environment variable."
        )
    return entsoe_token


def get_cdsapi_url() -> str:
    """Fetches the CDS API URL from environment variables."""
    cdsapi_url = os.getenv("CDSAPI_URL")
    if cdsapi_url is None:
        raise ValueError(
            "CDS API URL is missing. Set the CDSAPI_URL environment variable."
        )
    return cdsapi_url


def get_cdsapi_key() -> str:
    """Fetches the CDS API key from environment variables."""
    cdsapi_key = os.getenv("CDSAPI_KEY")
    if cdsapi_key is None:
        raise ValueError(
            "CDS API key is missing. Set the CDSAPI_KEY environment variable."
        )
    return cdsapi_key

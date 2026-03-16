import os
from uuid import uuid4
from datetime import datetime, timezone

import psycopg
import pytest

from probabilistic_load_forecast.adapters.db.repository import (
    EntsoePostgreRepository, Era5PostgreRepository
)

from probabilistic_load_forecast.domain.model import (
    BiddingZone,
    LoadMeasurement,
    Resolution,
    TimeInterval,
    LoadSeries,
    Era5Series,
    InstantWeatherValue,
    WeatherArea,
    CountryCode,
    WeatherVariable
)


@pytest.fixture
def postgres_dsn() -> str:
    return os.getenv(
        "TEST_PG_DSN",
        "postgresql://postgres:secret@localhost:5432/plf",
    )


@pytest.fixture
def bidding_zone() -> BiddingZone:
    return BiddingZone(
        eic_code="10YAT-APG------L",
        display_name="Austria",
        country_code=CountryCode("AT"),
    )


@pytest.fixture
def test_schema(postgres_dsn: str):
    schema = f"test_{uuid4().hex[:8]}"

    try:
        with psycopg.connect(postgres_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA "{schema}"')
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not available for integration test: {exc}")

    try:
        yield schema
    finally:
        with psycopg.connect(postgres_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

def test_entsoe_repository_roundtrip(postgres_dsn: str, test_schema: str, bidding_zone: BiddingZone):
    repo = EntsoePostgreRepository(postgres_dsn)

    measurements = LoadSeries(
        bidding_zone=bidding_zone,
        resolution=Resolution.PT15M,
        observations=(
        LoadMeasurement(
            bidding_zone=bidding_zone,
            interval=TimeInterval(
                start=datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc),
                end=datetime(2025, 7, 13, 0, 15, tzinfo=timezone.utc),
            ),
            load_mw=4544.0,
        ),
        LoadMeasurement(
            bidding_zone=bidding_zone,
            interval=TimeInterval(
                start=datetime(2025, 7, 13, 0, 15, tzinfo=timezone.utc),
                end=datetime(2025, 7, 13, 0, 30, tzinfo=timezone.utc),
            ),
            load_mw=4521.0,
        ),
        )
    )

    repo.add(measurements, schema=test_schema, tablename="actual_total_load")

    series = repo.get(
        start=datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc),
        end=datetime(2025, 7, 13, 0, 30, tzinfo=timezone.utc),
        bidding_zone=bidding_zone,
        schema=test_schema,
        tablename="actual_total_load",
    )

    assert series.bidding_zone == bidding_zone
    assert series.resolution == Resolution.PT15M
    assert len(series.observations) == 2
    assert series.observations[0].interval.start == datetime(2025, 7, 13, 0, 0, tzinfo=timezone.utc)
    assert series.observations[0].load_mw == 4544.0
    assert series.observations[1].interval.start == datetime(2025, 7, 13, 0, 15, tzinfo=timezone.utc)
    assert series.observations[1].load_mw == 4521.0


def test_era5_repository_roundtrip(postgres_dsn: str, test_schema: str):
    repo = Era5PostgreRepository(postgres_dsn)

    area=WeatherArea(CountryCode("AT"))
    
    repo.add(
        Era5Series(
            area=area,
            resolution=Resolution.PT1H,
            observations=(
                InstantWeatherValue(
                    area,
                    WeatherVariable.T2M,
                    datetime(2018,10,1,0,0,0, tzinfo=timezone.utc),
                    10.0
                ),
                InstantWeatherValue(
                    area,
                    WeatherVariable.T2M,
                    datetime(2018,10,2,0,0,0, tzinfo=timezone.utc),
                    10.0
                )
            ),
           variable=WeatherVariable.T2M
        ),
        schema=test_schema
    )

    repo.get(
        interval=TimeInterval(datetime(2018,10,1,0,0,0, tzinfo=timezone.utc),datetime(2018,10,1, 23,59,59, tzinfo=timezone.utc)),
        area=area,
        variable=WeatherVariable.T2M,
        schema=test_schema
        
   )
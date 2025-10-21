"""PostgreSQL repository implementations for Entsoe and Era5 data."""

from typing import List
import psycopg
import pandas as pd
from psycopg import sql

from probabilistic_load_forecast.domain.model import LoadTimeseries, LoadMeasurement


class EntsoePostgreRepository:
    """PostgreSQL repository for actual load data from ENTSO-E."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def get(self, start, end) -> LoadTimeseries:
        """Retrieve actual load data between start and end timestamps."""
        query = """
        SELECT start_ts, load_mw
        FROM actual_total_load_at
        WHERE start_ts < %s
        AND end_ts > %s
        ORDER BY start_ts;
        """
        with psycopg.connect(self.dsn) as con:
            with con.cursor() as cur:
                cur.execute(query, (end, start))
                rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=["start_ts", "load_mw"])

        df["datetime"] = pd.to_datetime(df["start_ts"], errors="raise")

        df["period"] = df["datetime"].dt.to_period("15min")
        df = df.set_index("period")
        s = pd.Series(df["load_mw"].values, index=df.index, name="actual_load_mw")
        return LoadTimeseries(data=s, bidding_zone="BZN|AT")

    def add(self, measurements: List[LoadMeasurement]) -> None:
        """Add load measurements to the repository."""
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO actual_total_load_at (start_ts, end_ts, load_mw) VALUES (%s, %s, %s)
                        ON CONFLICT ON CONSTRAINT unique_load_measurement_slot DO UPDATE 
                        SET load_mw = EXCLUDED.load_mw
                        """,
                    [(m.start_ts, m.end_ts, m.load_mw) for m in measurements],
                )


class Era5PostgreRepository:
    """PostgreSQL repository for storing ERA5 country averages."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def _create_table(
        self, tablename: str, cur: psycopg.Cursor, schema: str = "public"
    ):
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                valid_time TIMESTAMPTZ PRIMARY KEY,
                value DOUBLE PRECISION NOT NULL,
                stat TEXT NOT NULL CHECK(stat IN('sum', 'instant')),
                interval_seconds INTEGER NOT NULL DEFAULT 3600
                );
            """
            ).format(sql.Identifier(schema, tablename))
        )

    def add(
        self,
        variable,
        df: pd.DataFrame,
        country_code,
        stat,
        interval_seconds=None,
        schema: str = "public",
    ):
        """Add ERA5 country average data to the repository."""
        tablename = f"{variable}_country_avg_{country_code}"
        interval_seconds = interval_seconds or 3600

        with psycopg.connect(self.dsn) as con:
            with con.cursor() as cur:
                # Create Table if not exists
                self._create_table(tablename, cur)

                insert_sql = sql.SQL(
                    """
                    INSERT INTO {} (valid_time, value, stat, interval_seconds) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (valid_time) DO UPDATE SET
                        value = EXCLUDED.value,
                        stat = EXCLUDED.stat,
                        interval_seconds = EXCLUDED.interval_seconds;
                """
                ).format(sql.Identifier(schema, tablename))

                cur.executemany(
                    insert_sql,
                    (
                        (row.Index, row.value, stat, interval_seconds)
                        for row in df.itertuples()
                    ),
                )

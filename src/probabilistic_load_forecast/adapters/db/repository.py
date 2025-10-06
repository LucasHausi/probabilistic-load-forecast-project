from typing import List
import psycopg
from probabilistic_load_forecast.domain.model import LoadTimeseries, LoadMeasurement
import pandas as pd


class PostgreRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def get(self, start, end) -> LoadTimeseries:
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

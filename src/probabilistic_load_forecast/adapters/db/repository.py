from typing import List
import psycopg

from probabilistic_load_forecast.domain.model import Measurement
class PostgreRepository():
    def __init__(self, dsn: str):
        self.dsn = dsn

    def save(self, measurements: List[Measurement]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                        """
                        INSERT INTO actual_total_load_at (measured_at, value) VALUES (%s, %s)
                        ON CONFLICT (measured_at) DO UPDATE 
                        SET value = EXCLUDED.value
                        """,
                        [(m.timestamp, m.value) for m in measurements]
                    )

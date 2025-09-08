from typing import List
import psycopg

from probabilistic_load_forecast.domain.entities import Measurement
class PostgreRepository():
    def __init__(self, dsn: str):
        self.dsn = dsn

    def save(self, measurements: List[Measurement]) -> None:
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                        "INSERT INTO load_measurements (measured_at, value) VALUES (%s, %s)", 
                        [(m.timestamp, m.value) for m in measurements]
                    )

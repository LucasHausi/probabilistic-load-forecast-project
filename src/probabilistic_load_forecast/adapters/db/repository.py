"""PostgreSQL repository implementations for Entsoe and Era5 data."""

from typing import List
from datetime import timedelta
import psycopg
import pandas as pd
from psycopg import sql
from datetime import datetime

from probabilistic_load_forecast.domain.model import (
    LoadSeries,
    LoadMeasurement,
    BiddingZone,
    TimeInterval,
    Resolution,
    WeatherArea,
    WeatherVariable,
    IntervalStatistic,
    Era5Series,
    InstantWeatherValue,
    IntervalWeatherValue,
    CountryCode,
    VARIABLE_VALUE_KIND,
    WeatherValueKind,
)

from probabilistic_load_forecast.domain.model import resolve_bidding_zone


class EntsoePostgreRepository:
    """PostgreSQL repository for actual load data from ENTSO-E."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def _create_table(
        self, tablename: str, cur: psycopg.Cursor, schema: str = "public"
    ):
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                start_ts timestamptz NOT NULL,
                end_ts timestamptz NOT NULL,
                load_mw numeric(10, 2) NOT NULL,
                created_at timestamp DEFAULT now() NULL,
                zone_code varchar(30) DEFAULT '10YAT-APG------L'::character varying NULL,
                CONSTRAINT unique_load_measurement_slot UNIQUE (start_ts, end_ts, zone_code)
            );
            """
            ).format(sql.Identifier(schema, tablename))
        )

    def get(
        self,
        start: datetime,
        end: datetime,
        bidding_zone: BiddingZone,
        schema: str = "public",
        tablename: str = "actual_total_load",
    ) -> LoadSeries:
        """Retrieve actual load data between start and end timestamps."""
        query = sql.SQL(
            """
            SELECT start_ts, end_ts, load_mw, zone_code
            FROM {}
            WHERE zone_code = %s
            AND start_ts < %s
            AND end_ts > %s
            ORDER BY start_ts;
            """
        ).format(sql.Identifier(schema, tablename))

        with psycopg.connect(self.dsn) as con:
            with con.cursor() as cur:
                cur.execute(query, (bidding_zone.eic_code, end, start))
                rows = cur.fetchall()

        observations = tuple(
            LoadMeasurement(
                bidding_zone=resolve_bidding_zone(zone_code),
                interval=TimeInterval(start=start_ts, end=end_ts),
                load_mw=float(load_mw),
            )
            for start_ts, end_ts, load_mw, zone_code in rows
        )

        return LoadSeries(
            bidding_zone=bidding_zone,
            resolution=Resolution.PT15M,
            observations=observations,
        )

    def add(
        self,
        load_series: LoadSeries,
        schema: str = "public",
        tablename="actual_total_load",
    ) -> None:
        """Add load measurements to the repository."""

        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                self._create_table(tablename, cur, schema)
                cur.executemany(
                    sql.SQL(
                        """
                        INSERT INTO {} (start_ts, end_ts, load_mw, zone_code) VALUES (%s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT unique_load_measurement_slot DO UPDATE 
                        SET load_mw = EXCLUDED.load_mw
                        """
                    ).format(sql.Identifier(schema, tablename)),
                    [
                        (
                            m.interval.start,
                            m.interval.end,
                            m.load_mw,
                            m.bidding_zone.eic_code,
                        )
                        for m in load_series.observations
                    ],
                )


class Era5PostgreRepository:
    """PostgreSQL repository for storing ERA5 country averages."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def _resolution_to_seconds(self, resolution: Resolution) -> int:
        if resolution == Resolution.PT1H:
            return 3600
        if resolution == Resolution.PT3H:
            return 10800
        if resolution == Resolution.PT15M:
            return 900
        raise ValueError(f"Unsupported resolution: {resolution}")

    def _observation_to_row(self, series: Era5Series, observation):
        if isinstance(observation, InstantWeatherValue):
            return (
                observation.valid_at,
                observation.value,
                "instant",
                self._resolution_to_seconds(series.resolution),
                observation.area.code.value,
            )

        if isinstance(observation, IntervalWeatherValue):
            return (
                observation.interval.end,
                observation.value,
                observation.statistic.value,
                int(
                    (
                        observation.interval.end - observation.interval.start
                    ).total_seconds()
                ),
                observation.area.code.value,
            )

        raise TypeError(...)

    def _row_to_observation(self, row, variable):
        valid_time, value, stat, interval_seconds, country_code = row

        area = WeatherArea(CountryCode(country_code))
        value = float(value)

        if stat == "instant":
            if variable is None:
                raise ValueError(
                    "variable is required to build an InstantWeatherValue from a row"
                )
            return InstantWeatherValue(
                area=area,
                variable=WeatherVariable(variable),
                valid_at=valid_time,
                value=value,
            )

        if stat in ("mean", "total"):
            if variable is None:
                raise ValueError(
                    "variable is required to build an IntervalWeatherValue from a row"
                )
            statistic = (
                IntervalStatistic.MEAN if stat == "mean" else IntervalStatistic.TOTAL
            )
            return IntervalWeatherValue(
                area=area,
                variable=WeatherVariable(variable),
                interval=TimeInterval(
                    start=valid_time - timedelta(seconds=interval_seconds),
                    end=valid_time,
                ),
                statistic=statistic,
                value=value,
            )

        raise ValueError(f"unsupported stat value: {stat}")

    def _create_table(
        self, tablename: str, cur: psycopg.Cursor, schema: str = "public"
    ):
        cur.execute(
            sql.SQL(
                """
            CREATE TABLE IF NOT EXISTS {} (
                valid_time TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                stat TEXT NOT NULL CHECK (stat IN ('total', 'mean', 'instant')),
                interval_seconds INTEGER NOT NULL DEFAULT 3600,
                country_code VARCHAR(5) NOT NULL,
                PRIMARY KEY (country_code, valid_time)
            );
            """
            ).format(sql.Identifier(schema, tablename))
        )

    def add(
        self,
        weather_series: Era5Series,
        interval_seconds=None,
        schema: str = "public",
    ):
        """Add ERA5 country average data to the repository."""
        tablename = f"{weather_series.variable}_country_avg"
        interval_seconds = interval_seconds or 3600

        with psycopg.connect(self.dsn) as con:
            with con.cursor() as cur:
                # Create Table if not exists
                self._create_table(tablename, cur, schema)

                insert_sql = sql.SQL(
                    """
                    INSERT INTO {} (valid_time, value, stat, interval_seconds, country_code) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (country_code, valid_time) DO UPDATE SET
                        value = EXCLUDED.value,
                        stat = EXCLUDED.stat,
                        interval_seconds = EXCLUDED.interval_seconds;
                """
                ).format(sql.Identifier(schema, tablename))

                cur.executemany(
                    insert_sql,
                    (
                        self._observation_to_row(weather_series, observation)
                        for observation in weather_series.observations
                    ),
                )

    def get(
        self,
        interval: TimeInterval,
        area: WeatherArea,
        variable: WeatherVariable,
        schema: str = "public",
    ) -> Era5Series:
        tablename = f"{variable}_country_avg"
        with psycopg.connect(self.dsn) as con:
            with con.cursor() as cur:
                value_kind = VARIABLE_VALUE_KIND[variable]

                if value_kind is WeatherValueKind.INSTANT:
                    time_predicate = sql.SQL("valid_time >= %s AND valid_time < %s")
                    params = (area.code.value, interval.start, interval.end)
                else:
                    time_predicate = sql.SQL("valid_time > %s AND valid_time <= %s")
                    params = (area.code.value, interval.start, interval.end)

                select_stmt = sql.SQL(
                    """
                    SELECT valid_time, value, stat, interval_seconds, country_code
                    FROM {}
                    WHERE country_code = %s
                    AND {}
                    ORDER BY valid_time
                    """
                ).format(
                    sql.Identifier(schema, tablename),
                    time_predicate,
                )

                cur.execute(select_stmt, params)
                rows = cur.fetchall()

                observations = tuple(
                    self._row_to_observation(row, variable) for row in rows
                )

                return Era5Series(
                    area=area,
                    resolution=Resolution.PT1H,
                    observations=observations,
                    variable=variable,
                )

                # df = pd.DataFrame(data=rows, columns=["valid_time", "value"])
                # df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)

                # if stat == "integrated_flux":
                #     df["valid_time"] = df["valid_time"] - pd.Timedelta(hours=1)
                #     df["valid_time"] = (
                #         df["valid_time"].dt.tz_convert(None).dt.to_period("h")
                #     )  # dropping the tz infromation before converting to a period
                #     df.drop(
                #         df.index[0], inplace=True
                #     )  # drop the first value that represents the not selected hour

                # df.set_index("valid_time", inplace=True)
                # return df


class ForecastMetadataRepository:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.timestamp_sources = [
            ("ssrd_country_avg", "valid_time"),
            ("actual_total_load", "start_ts"),
            ("t2m_country_avg", "valid_time"),
            ("tp_country_avg", "valid_time"),
            ("u10_country_avg", "valid_time"),
            ("v10_country_avg", "valid_time"),
        ]

    def get_latest_common_timestamp(self, schema: str = "public") -> datetime | None:
        least_parts = [
            sql.SQL("(SELECT MAX({}) FROM {})").format(
                sql.Identifier(column),
                sql.Identifier(schema, table_name),
            )
            for table_name, column in self.timestamp_sources
        ]

        query = sql.SQL(
            """
            WITH last_common_hour AS (
                SELECT LEAST({}) AS ts
            )
            SELECT MAX(atl.start_ts)
            FROM {} atl
            CROSS JOIN last_common_hour lch
            WHERE atl.end_ts <= lch.ts
            """
        ).format(
            sql.SQL(", ").join(least_parts),
            sql.Identifier(schema, "actual_total_load"),
        )

        with psycopg.connect(self.dsn) as con:
            with con.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()

        return row[0] if row else None

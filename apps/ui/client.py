from datetime import datetime
import requests
import streamlit as st
from dataclasses import dataclass
from requests.exceptions import JSONDecodeError
import numpy as np
import pandas as pd

BASE_URL = st.secrets["api"]["base_url"]

WEATHER_VARIABLE_META = {
    "t2m": {
        "title": "2 m Temperature",
        "y_label": "Temperature [K]",
        "color": "#d94801",
    },
    "ssrd": {
        "title": "Surface Solar Radiation Downwards",
        "y_label": "Radiation [J/m^2]",
        "color": "#e6ab02",
    },
    "tp": {
        "title": "Total Precipitation",
        "y_label": "Precipitation [m]",
        "color": "#1f78b4",
    },
    "u10": {
        "title": "10 m U Wind Component",
        "y_label": "Wind Speed [m/s]",
        "color": "#33a02c",
    },
    "v10": {
        "title": "10 m V Wind Component",
        "y_label": "Wind Speed [m/s]",
        "color": "#6a3d9a",
    },
}

@dataclass(frozen=True)
class LoadDataQuery:
    start: datetime
    end: datetime
    eic_code: str

    def to_params(self) -> dict[str, str]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "eic_code": self.eic_code,
        }


@dataclass(frozen=True)
class LoadSeries:
    total_load: np.ndarray
    timestamp: pd.DatetimeIndex

    @property
    def empty(self):
        return len(self.total_load) == 0 or len(self.timestamp) == 0

    def to_frame(self) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "timestamp": self.timestamp,
                "load_mw": self.total_load,
            }
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp")
        df["load_mw"] = df["load_mw"].astype("float64")
        return df


@dataclass(frozen=True)
class WeatherDataQuery:
    start: datetime
    end: datetime
    variable: str
    area_code: str

    def to_params(self) -> dict[str, str]:
        return {
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "variable": self.variable,
            "area_code": self.area_code,
        }


@dataclass(frozen=True)
class WeatherSeries:
    weather_variable: str
    value: np.ndarray
    timestamp: pd.DatetimeIndex

    @property
    def empty(self):
        return len(self.weather_variable) == 0 or len(self.timestamp) == 0

    def to_frame(self) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                "timestamp": self.timestamp,
                self.weather_variable: self.value,
            }
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp")
        df[self.weather_variable] = df[self.weather_variable].astype("float64")
        return df


def get_observation_timestamp(obs: dict) -> str:
    if "interval" in obs:
        return obs["interval"]["start"]
    return obs["valid_at"]


@st.cache_data
def get_weather_data(query: WeatherDataQuery) -> WeatherSeries:
    response = requests.get(
        f"{BASE_URL}/weather-data",
        params=query.to_params(),
        timeout=30,
    )
    print(query.to_params())
    response.raise_for_status()

    try:
        weather_data = response.json()
        observations = weather_data["observations"]
        variable = weather_data["variable"]
        timestamp = pd.to_datetime(
            [get_observation_timestamp(obs) for obs in observations],
            utc=True,
        )

        value = np.array(
            [obs["value"] for obs in observations],
            dtype=np.float64,
        )
    except Exception as ex:
        raise ValueError("Could not decode JSON into a WeatherSeries", ex) from ex

    return WeatherSeries(weather_variable=variable, value=value, timestamp=timestamp)


@st.cache_data
def get_load_data(query: LoadDataQuery) -> LoadSeries:
    response = requests.get(
        f"{BASE_URL}/load-data",
        params=query.to_params(),
        timeout=30,
    )
    response.raise_for_status()

    try:
        load_data = response.json()
        observations = load_data["observations"]
        timestamp = pd.to_datetime(
            [obs["interval"]["start"] for obs in observations],
            utc=True
        )

        actual_total_load = np.array(
            [obs["load_mw"] for obs in observations],
            dtype=np.float64,
        )
    except Exception as ex:
        raise ValueError("Could not decode JSON into a LoadSeries") from ex

    return LoadSeries(timestamp=timestamp, total_load=actual_total_load)


@st.cache_data
def get_latest_common_ts() -> datetime | None:
    response = requests.get(
        f"{BASE_URL}/latest-common-timestamp",
        timeout=30,
    )
    response.raise_for_status()
    try:
        data = response.json()
    except JSONDecodeError as ex:
        raise ValueError("Could not decode JSON") from ex

    timestamp = data.get("timestamp")
    if timestamp is None:
        return None

    return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

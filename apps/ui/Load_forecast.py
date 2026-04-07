from enum import StrEnum
from datetime import datetime, timedelta, timezone
import json
import plotly.express as px
import streamlit as st
import numpy as np
import mlflow
import torch
import client
import pandas as pd
import holidays
from zoneinfo import ZoneInfo
from pathlib import Path
import pickle

from sklearn.preprocessing import RobustScaler


local_tz = ZoneInfo("Europe/Vienna")
LSTM_ARTIFACT_DIR = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "processed"
    / "ml_data"
    / "fs_06_load_calendar_future_weather"
)
LSTM_FORECAST_FREQ = "15min"
BAYESIAN_MC_SAMPLES = 190
BAYESIAN_INTERVAL_Z = 1.96


class ModelType(StrEnum):
    LSTM = "lstm"
    PROPHET = "prophet"


def to_local_naive(dt: datetime) -> datetime:
    return dt.astimezone(local_tz).replace(tzinfo=None)


def widget_value_to_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=local_tz).astimezone(timezone.utc)

def add_features(X: pd.DataFrame) -> pd.DataFrame:
    X = X.copy()
    X["wind_speed_future"] = (X["u10_future"] ** 2 + X["v10_future"] ** 2) ** 0.5
    X["ssrd_future"] = X["ssrd_future"] / 3600
    X["tp_future"] = X["tp_future"] / 3600
    X.drop(columns=["u10_future", "v10_future"], inplace=True)

    idx = pd.DatetimeIndex(pd.to_datetime(X.index, utc=True)).tz_convert(local_tz)

    X["is_weekday"] = idx.weekday < 5

    # Austria public holidays
    years = range(idx.min().year, idx.max().year + 1)
    at_holidays = holidays.country_holidays(st.session_state["area_code"], years=years)
    X["is_holiday"] = idx.map(lambda ts: ts.date() in at_holidays).astype("int8")
    return X

def make_prophet_frame(df: pd.DataFrame, forecast_start: datetime, target: str) -> pd.DataFrame:
    # The prophet prediction df shall only include the last 24h window (96 x 15min)
    pred_index = pd.date_range(
        start=forecast_start,
        periods=96,
        freq="15min",
        tz="UTC",
    )
    prophet_df = df[df.index < forecast_start].drop(columns=target).tail(96).copy()
    prophet_df.index = pred_index

    prophet_df = add_features(prophet_df)

    prophet_df["ds"] = prophet_df.index
    prophet_df["ds"] = prophet_df["ds"].dt.tz_convert(None) # remove as prophet cant handle tz
    return prophet_df

def run_forcast_prophet(data: pd.DataFrame, forecast_start: datetime) -> client.LoadSeries:
    # Run Prophet forecast
    # st.write(data)
    prophet_df = make_prophet_frame(df=data, forecast_start=forecast_start, target="load_mw")
    # st.write(prophet_df)
    prophet_pred = prophet_model.predict(prophet_df)["yhat"].to_numpy(dtype=np.float64)
    timestamp = pd.DatetimeIndex(pd.to_datetime(prophet_df.index, utc=True))

    prophet_series = client.LoadSeries(
        timestamp=timestamp,
        total_load=prophet_pred
    )
    st.session_state["prophet_forecast"] = prophet_series.to_frame()

def prepare_lstm_features(data: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    feature_df = data.copy().rename(columns={"load_mw": "actual_load_mw"})
    feature_df = add_features(feature_df)

    return feature_df.loc[:, feature_names]


def run_forcast_lstm(
    data: pd.DataFrame, scaler: RobustScaler, forecast_start: datetime
) -> tuple[client.LoadSeries, client.LoadSeries]:
    meta = load_lstm_meta()
    feature_names = meta["feature_names"]
    window_size = int(meta["window_size"])
    forecast_horizon = int(meta["forecast_horizon"])

    feature_df = prepare_lstm_features(data, feature_names)
    feature_df = feature_df[feature_df.index < forecast_start]
    if len(feature_df) < window_size:
        raise ValueError(
            f"Need at least {window_size} complete rows for LSTM inference, got {len(feature_df)}."
        )

    input_window = feature_df.tail(window_size)
    scaled_window = x_scaler.transform(input_window)
    model_input = torch.tensor(
        scaled_window[np.newaxis, :, :],
        dtype=torch.float32,
        device=torch.device(st.session_state["device"]),
    )

    vanilla_lstm.eval()
    with torch.no_grad():
        vanilla_pred_scaled = vanilla_lstm(model_input).detach().cpu().numpy()

    bayesian_lstm.train()
    with torch.no_grad():
        bayesian_samples = torch.stack(
            [bayesian_lstm(model_input) for _ in range(BAYESIAN_MC_SAMPLES)],
            dim=0,
        )
        bayesian_mu_scaled, bayesian_var_raw_scaled = torch.chunk(
            bayesian_samples, 2, dim=-1
        )
        bayesian_var_scaled = torch.nn.functional.softplus(
            bayesian_var_raw_scaled
        ) + 1e-3
        bayesian_pred_mean_scaled = bayesian_mu_scaled.mean(dim=0)
        bayesian_pred_var_scaled = bayesian_var_scaled.mean(
            dim=0
        ) + bayesian_mu_scaled.var(dim=0, unbiased=False)
        bayesian_pred_std_scaled = torch.sqrt(bayesian_pred_var_scaled)
        bayesian_lower_scaled = (
            bayesian_pred_mean_scaled - BAYESIAN_INTERVAL_Z * bayesian_pred_std_scaled
        ).cpu().numpy()
        bayesian_upper_scaled = (
            bayesian_pred_mean_scaled + BAYESIAN_INTERVAL_Z * bayesian_pred_std_scaled
        ).cpu().numpy()
        bayesian_pred_scaled = bayesian_pred_mean_scaled.cpu().numpy()

    vanilla_pred = scaler.inverse_transform(
        vanilla_pred_scaled.reshape(-1, 1)
    ).reshape(-1)
    bayesian_pred = scaler.inverse_transform(
        bayesian_pred_scaled.reshape(-1, 1)
    ).reshape(-1)
    bayesian_lower = scaler.inverse_transform(
        bayesian_lower_scaled.reshape(-1, 1)
    ).reshape(-1)
    bayesian_upper = scaler.inverse_transform(
        bayesian_upper_scaled.reshape(-1, 1)
    ).reshape(-1)

    timestamp = pd.date_range(
        start=forecast_start,
        periods=forecast_horizon,
        freq=LSTM_FORECAST_FREQ,
        tz="UTC",
    )

    vanilla_forecast = client.LoadSeries(
        timestamp=timestamp,
        total_load=vanilla_pred.astype(np.float64),
    )
    bayesian_forecast = client.LoadSeries(
        timestamp=timestamp,
        total_load=bayesian_pred.astype(np.float64),
    )

    st.session_state["vanilla_forecast"] = vanilla_forecast.to_frame()
    st.session_state["bayesian_forecast"] = bayesian_forecast.to_frame()
    st.session_state["bayesian_forecast_band"] = pd.DataFrame(
        {
            "load_mw_lower": bayesian_lower.astype(np.float64),
            "load_mw_upper": bayesian_upper.astype(np.float64),
        },
        index=timestamp,
    )

    return vanilla_forecast, bayesian_forecast


def align_load_weather_series(load_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    load_df = load_df.sort_index()
    weather_df = weather_df.sort_index()

    weather_df = weather_df.shift(periods=-1, freq="D")
    weather_df = weather_df.reindex(load_df.index)

    df_combined = load_df.join(weather_df, how="left")
    df_combined[weather_df.columns] = df_combined[weather_df.columns].ffill().bfill() # needed for now because of time gaps

    return df_combined


@st.cache_resource(ttl=3000, show_spinner=False, scope="global")
def load_forecasting_model(
    registered_model_name: str, model_type: ModelType, version: str | int = "latest"
):
    model_uri = f"models:/{registered_model_name}/{version}"
    if model_type == ModelType.LSTM:
        model = mlflow.pytorch.load_model( # pyright: ignore
            model_uri, map_location=torch.device(st.session_state["device"])
        )
    elif model_type == ModelType.PROPHET:
        model = mlflow.prophet.load_model(model_uri)  # pyright: ignore
    else:
        raise ValueError("Unkown model type")
    return model

def load_scaler() -> RobustScaler:
    with open(LSTM_ARTIFACT_DIR / "scalers" / "y_scaler.pkl", "rb") as f:
        return pickle.load(f)


@st.cache_resource(ttl=3000, show_spinner=False, scope="global")
def load_x_scaler() -> RobustScaler:
    with open(LSTM_ARTIFACT_DIR / "scalers" / "X_scaler.pkl", "rb") as f:
        return pickle.load(f)


@st.cache_resource(ttl=3000, show_spinner=False, scope="global")
def load_lstm_meta() -> dict:
    with open(LSTM_ARTIFACT_DIR / "meta.json", "r", encoding="utf-8") as f:
        return json.load(f)

# TODO:
# Load the last available common data - done
# Option to select a specific model or all
# Run the forcast
# Make an interpretation section or the overlay in the plots?

# init the mlflow tracking url
mlflow.set_tracking_uri("http://127.0.0.1:5000")

# init the session state variables
if "latest_common_ts" not in st.session_state:
    st.session_state["latest_common_ts"] = client.get_latest_common_ts()

if "device" not in st.session_state:
    st.session_state["device"] = "cuda" if torch.cuda.is_available() else "cpu"

# init the models
bayesian_lstm = load_forecasting_model("BayesianLSTM", model_type=ModelType.LSTM)
vanilla_lstm = load_forecasting_model("VanillaLSTM", model_type=ModelType.LSTM)
prophet_model = load_forecasting_model("Prophet", model_type=ModelType.PROPHET)

scaler = load_scaler()
x_scaler = load_x_scaler()

area_eic_code_map = {"AT": "10YAT-APG------L"}
col1, col2, col3, col4 = st.columns(4)
area_code = col1.selectbox(label="Area", options=("AT",), key="area_code")
latest_common_ts_utc = st.session_state["latest_common_ts"]
latest_common_ts_local = to_local_naive(latest_common_ts_utc)
start = col2.datetime_input(
    label="From",
    value=latest_common_ts_local - timedelta(days=1),
    key="forecast_start",
)
end = col3.datetime_input(
    label="To", value=latest_common_ts_local, key="forecast_end"
)


if start is not None and end is not None:
    eic_code = area_eic_code_map.get(area_code)
    start_local = start.replace(tzinfo=local_tz)
    end_local = end.replace(tzinfo=local_tz)
    start_utc = widget_value_to_utc(start)
    end_utc = widget_value_to_utc(end)

    start_week_ahead = start_utc - timedelta(days=7)  # fetch 7 day window as the LSTM input

    if eic_code:
        load_data = client.get_load_data(
            client.LoadDataQuery(
                start=start_week_ahead,
                end=end_utc,
                eic_code=eic_code,
            )
        )

        load_df = load_data.to_frame()

        if not load_data.empty:
            fig = px.line(
                load_df[load_df.index >= start_utc],
                x=load_df.index[load_df.index >= start_utc],
                y="load_mw",
                title="Historical Load Data",
                labels={
                    "x": "Timestamp (UTC)",
                    "load_mw": "Load [MW]",
                },
            )
            prophet_forecast_df = st.session_state.get("prophet_forecast")
            if prophet_forecast_df is not None:
                fig.add_scatter(
                        x=prophet_forecast_df.index,
                        y=prophet_forecast_df["load_mw"],
                        mode="lines",
                        name="Prophet Forecast",
                        line=dict(color="#d97212"),
                        hovertemplate="Timestamp (UTC)=%{x}<br>Load [MW]=%{y}<extra>Prophet Forecast</extra>",
                    )
            vanilla_forecast_df = st.session_state.get("vanilla_forecast")
            if vanilla_forecast_df is not None:
                fig.add_scatter(
                        x=vanilla_forecast_df.index,
                        y=vanilla_forecast_df["load_mw"],
                        mode="lines",
                        name="Vanilla LSTM Forecast",
                        line=dict(color="#1f78b4"),
                        hovertemplate="Timestamp (UTC)=%{x}<br>Load [MW]=%{y}<extra>Vanilla LSTM Forecast</extra>",
                    )
            bayesian_forecast_band_df = st.session_state.get("bayesian_forecast_band")
            if bayesian_forecast_band_df is not None:
                fig.add_scatter(
                        x=bayesian_forecast_band_df.index,
                        y=bayesian_forecast_band_df["load_mw_upper"],
                        mode="lines",
                        name="Bayesian LSTM 95% CI",
                        line=dict(color="rgba(51, 160, 44, 0.6)", width=1),
                        hoverinfo="skip",
                        legendgroup="bayesian_ci",
                        showlegend=True,
                    )
                fig.add_scatter(
                        x=bayesian_forecast_band_df.index,
                        y=bayesian_forecast_band_df["load_mw_lower"],
                        mode="lines",
                        line=dict(color="rgba(51, 160, 44, 0)"),
                        fill="tonexty",
                        fillcolor="rgba(51, 160, 44, 0.18)",
                        hoverinfo="skip",
                        legendgroup="bayesian_ci",
                        showlegend=False,
                    )
            bayesian_forecast_df = st.session_state.get("bayesian_forecast")
            if bayesian_forecast_df is not None:
                fig.add_scatter(
                        x=bayesian_forecast_df.index,
                        y=bayesian_forecast_df["load_mw"],
                        mode="lines",
                        name="Bayesian LSTM Forecast",
                        line=dict(color="#33a02c"),
                        hovertemplate="Timestamp (UTC)=%{x}<br>Load [MW]=%{y}<extra>Bayesian LSTM Forecast</extra>",
                    )
            fig.update_layout(legend=dict(groupclick="togglegroup"))
            st.plotly_chart(fig)

            if st.button("Run forecast"):
                weather_combined = pd.DataFrame()
                for variable in client.WEATHER_VARIABLE_META:
                    weather_series = client.get_weather_data(
                        client.WeatherDataQuery(
                            start=start_week_ahead.replace(minute=0),
                            end=end_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1) + timedelta(days=1),
                            area_code=area_code,
                            variable=variable,
                        )
                    )
                    weather_df = weather_series.to_frame()
                    
                   
                    weather_combined = pd.concat([weather_combined, weather_df], axis=1, join="outer")

                # hardcode renaming columns to ***_future for now
                weather_combined = weather_combined.rename(
                    columns={
                        "t2m": "t2m_future",
                        "u10": "u10_future",
                        "v10": "v10_future",
                        "tp": "tp_future",
                        "ssrd": "ssrd_future",
                    }
                )

                aligned_df = align_load_weather_series(load_df, weather_combined)

                forecast_start_local = end_local.replace(hour=0, minute=0, second=0, microsecond=0)
                forecast_start_utc = forecast_start_local.astimezone(timezone.utc)
                run_forcast_prophet(aligned_df, forecast_start_utc)
                run_forcast_lstm(aligned_df, scaler, forecast_start_utc)
                
                st.rerun()
            

from enum import StrEnum
from datetime import datetime, timedelta, timezone
from functools import partial
import json
import plotly.express as px
import plotly.graph_objects as go
import shap
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
SHAP_BACKGROUND_STRIDE = 96
SHAP_BACKGROUND_SIZE = 8
BAYESIAN_SHAP_SAMPLES = 3
PROPHET_SHAP_BACKGROUND_SIZE = 256
INTERPRETATION_DAY_PARTS = {
    "Night": slice(0, 24),
    "Morning": slice(24, 48),
    "Afternoon": slice(48, 72),
    "Evening": slice(72, 96),
}
PROPHET_INTERPRETATION_FEATURES = [
    "t2m_future",
    "ssrd_future",
    "tp_future",
    "wind_speed_future",
    "is_weekday",
    "is_holiday",
]


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
    try:
        with st.spinner("Computing Prophet forecast interpretations..."):
            st.session_state["prophet_interpretation"] = (
                compute_prophet_interpretation_payload(prophet_df, timestamp)
            )
            clear_interpretation_error("Prophet")
    except Exception as ex:
        set_interpretation_error(
            "Prophet", f"Could not compute Prophet interpretations: {ex}"
        )
        st.session_state.pop("prophet_interpretation", None)

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
    # st.write(feature_df)
    # st.write(f"Before: {len(feature_df)}")
    feature_df = feature_df[feature_df.index < forecast_start]
    # st.write(f"After: {len(feature_df)}")
    # st.write(f"Date: {forecast_start}")
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
    try:
        with st.spinner("Computing LSTM forecast interpretations..."):
            st.session_state["vanilla_interpretation"] = (
                compute_vanilla_lstm_interpretation_payload(scaled_window, timestamp)
            )
            st.session_state["bayesian_interpretation"] = (
                compute_bayesian_lstm_interpretation_payload(scaled_window, timestamp)
            )
            clear_interpretation_error("Vanilla LSTM")
            clear_interpretation_error("Bayesian LSTM")
    except Exception as ex:
        set_interpretation_error(
            "Vanilla LSTM", f"Could not compute LSTM interpretations: {ex}"
        )
        set_interpretation_error(
            "Bayesian LSTM", f"Could not compute LSTM interpretations: {ex}"
        )
        st.session_state.pop("vanilla_interpretation", None)
        st.session_state.pop("bayesian_interpretation", None)

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


@st.cache_resource(ttl=3000, show_spinner=False, scope="global")
def load_lstm_shap_background() -> np.ndarray:
    x_train = np.load(LSTM_ARTIFACT_DIR / "X_train.npy", mmap_mode="r")
    return np.asarray(
        x_train[::SHAP_BACKGROUND_STRIDE][:SHAP_BACKGROUND_SIZE],
        dtype=np.float32,
    )


@st.cache_data(ttl=3000, show_spinner=False)
def load_prophet_shap_background(reg_cols: tuple[str, ...]) -> np.ndarray:
    meta = load_lstm_meta()
    train_start = pd.Timestamp(meta["train_date_range"]["start"])
    train_end = pd.Timestamp(meta["train_date_range"]["end"])

    raw_data = pd.read_parquet(LSTM_ARTIFACT_DIR.parents[1] / "data_combined.parquet")
    train_raw = raw_data.loc[train_start:train_end].copy()

    background_df = train_raw.loc[:, list(reg_cols)].dropna()
    if len(background_df) > PROPHET_SHAP_BACKGROUND_SIZE:
        background_df = background_df.sample(
            n=PROPHET_SHAP_BACKGROUND_SIZE,
            random_state=1234,
        )

    return background_df.to_numpy(np.float64)


def predict_lstm_for_shap(
    x_np: np.ndarray,
    model: torch.nn.Module,
    device: str,
    y_scaler: RobustScaler,
    bayesian: bool = False,
) -> np.ndarray:
    x_np = np.asarray(x_np, dtype=np.float32)
    x_t = torch.from_numpy(x_np).to(torch.device(device))

    if bayesian:
        model.train()
    else:
        model.eval()

    with torch.no_grad():
        y = model(x_t)
        if bayesian:
            y, _ = torch.chunk(y, 2, dim=-1)

    y_np = y.detach().cpu().numpy()
    _, horizon = y_np.shape
    return y_scaler.inverse_transform(y_np.reshape(-1, 1)).reshape(-1, horizon)


def set_interpretation_error(model_name: str, message: str) -> None:
    errors = dict(st.session_state.get("interpretation_errors", {}))
    errors[model_name] = message
    st.session_state["interpretation_errors"] = errors


def clear_interpretation_error(model_name: str) -> None:
    errors = dict(st.session_state.get("interpretation_errors", {}))
    errors.pop(model_name, None)
    if errors:
        st.session_state["interpretation_errors"] = errors
    else:
        st.session_state.pop("interpretation_errors", None)


class FeatureGroupedTimeSeriesMasker:
    def __init__(self, background_X: np.ndarray):
        self.bg = np.asarray(background_X, dtype=np.float32)
        if self.bg.ndim != 3:
            raise ValueError("background_X must be (n_bg, T, F)")

        self.T = self.bg.shape[1]
        self.F = self.bg.shape[2]
        self.shape = (len(self.bg), self.F)

    def __call__(self, mask, x):
        x = np.asarray(x, dtype=np.float32)
        if x.shape != (self.T, self.F):
            raise ValueError(f"x must be (T,F)=({self.T},{self.F}), got {x.shape}")

        mask = np.asarray(mask, dtype=bool).reshape(-1)
        if mask.shape != (self.F,):
            raise ValueError(f"mask must flatten to ({self.F},), got {mask.shape}")

        masked = np.repeat(x[None, :, :], len(self.bg), axis=0)
        for feature_idx in range(self.F):
            if not mask[feature_idx]:
                masked[:, :, feature_idx] = self.bg[:, :, feature_idx]
        return masked

    def mask_shapes(self, x):
        return [(self.F,)]


def compute_vanilla_lstm_interpretation_payload(
    scaled_window: np.ndarray,
    timestamps: pd.DatetimeIndex,
) -> dict:
    feature_names = load_lstm_meta()["feature_names"]
    masker = FeatureGroupedTimeSeriesMasker(load_lstm_shap_background())
    explainer = shap.Explainer(
        lambda x: predict_lstm_for_shap(
            x,
            model=vanilla_lstm,
            device=st.session_state["device"],
            y_scaler=scaler,
            bayesian=False,
        ),
        masker,
        algorithm="exact",
    )
    shap_values = explainer(scaled_window[np.newaxis, :, :])

    return {
        "placeholder": False,
        "feature_names": feature_names,
        "timestamps": timestamps,
        "values": np.asarray(shap_values.values[0], dtype=np.float64),
        "lower": None,
        "upper": None,
        "base_values": np.asarray(shap_values.base_values[0], dtype=np.float64),
    }


def compute_bayesian_lstm_interpretation_payload(
    scaled_window: np.ndarray,
    timestamps: pd.DatetimeIndex,
) -> dict:
    feature_names = load_lstm_meta()["feature_names"]
    masker = FeatureGroupedTimeSeriesMasker(load_lstm_shap_background())
    explainer = shap.Explainer(
        lambda x: predict_lstm_for_shap(
            x,
            model=bayesian_lstm,
            device=st.session_state["device"],
            y_scaler=scaler,
            bayesian=True,
        ),
        masker,
        algorithm="exact",
    )

    shap_samples = []
    base_value_samples = []
    for _ in range(BAYESIAN_SHAP_SAMPLES):
        shap_values = explainer(scaled_window[np.newaxis, :, :])
        shap_samples.append(np.asarray(shap_values.values[0], dtype=np.float64))
        base_value_samples.append(
            np.asarray(shap_values.base_values[0], dtype=np.float64)
        )

    shap_stack = np.stack(shap_samples, axis=0)
    base_stack = np.stack(base_value_samples, axis=0)

    return {
        "placeholder": False,
        "feature_names": feature_names,
        "timestamps": timestamps,
        "values": shap_stack.mean(axis=0),
        "lower": np.percentile(shap_stack, 2.5, axis=0),
        "upper": np.percentile(shap_stack, 97.5, axis=0),
        "base_values": base_stack.mean(axis=0),
    }


def predict_prophet_for_shap(
    x_np: np.ndarray | pd.DataFrame, input_cols: list[str], ds: pd.Timestamp
) -> np.ndarray:
    if isinstance(x_np, pd.DataFrame):
        x_df = x_np.copy()
    else:
        x_df = pd.DataFrame(x_np, columns=input_cols)
    x_df["ds"] = pd.to_datetime(np.repeat(ds, len(x_df)))
    return prophet_model.predict(x_df)["yhat"].to_numpy(dtype=np.float64)


def compute_prophet_interpretation_payload(
    prophet_df: pd.DataFrame, timestamps: pd.DatetimeIndex
) -> dict:
    reg_cols = [c for c in prophet_df.columns if c not in ["ds", "y"]]

    shap_values_per_step = []
    base_values_per_step = []
    X_reg_df = prophet_df[reg_cols].copy()
    X_reg = prophet_df[reg_cols].to_numpy(np.float64)
    bg_reg = load_prophet_shap_background(tuple(reg_cols))

    for i in range(len(X_reg)):
        ds = pd.Timestamp(prophet_df["ds"].iloc[i])
        predict_fn = partial(predict_prophet_for_shap, ds=ds, input_cols=reg_cols)
        explainer = shap.Explainer(predict_fn, masker=bg_reg, algorithm="exact")
        shap_values = explainer(X_reg[[i]])
        shap_values_per_step.append(np.asarray(shap_values.values[0], dtype=np.float64))
        base_values_per_step.append(float(np.asarray(shap_values.base_values[0])))

    values = np.stack(shap_values_per_step, axis=1)
    base_values = np.asarray(base_values_per_step, dtype=np.float64)

    return {
        "feature_names": list(X_reg_df.columns),
        "timestamps": timestamps,
        "values": values,
        "lower": None,
        "upper": None,
        "base_values": base_values,
    }


def get_interpretation_feature_names(model_name: str) -> list[str]:
    if model_name == "Prophet":
        return PROPHET_INTERPRETATION_FEATURES
    return load_lstm_meta()["feature_names"]



def get_interpretation_payload(model_name: str) -> dict:
    session_key_map = {
        "Vanilla LSTM": "vanilla_interpretation",
        "Bayesian LSTM": "bayesian_interpretation",
        "Prophet": "prophet_interpretation",
    }
    payload = st.session_state.get(session_key_map[model_name])
    if payload is None:
        raise ValueError(f"Could not find the Interpretation Payload for the model: {model_name}")
    return payload


def make_aggregated_interpretation_figure(
    payload: dict, horizon_slice: slice, title: str
) -> go.Figure:
    values = payload["values"][:, horizon_slice]
    plot_df = pd.DataFrame(
        {
            "feature": payload["feature_names"],
            "importance": np.mean(np.abs(values), axis=1),
            "direction": np.mean(values, axis=1),
        }
    ).sort_values("importance", ascending=True)
    plot_df["effect"] = np.where(
        plot_df["direction"] >= 0, "Supports Higher Load", "Supports Lower Load"
    )

    fig = px.bar(
        plot_df,
        x="importance",
        y="feature",
        color="effect",
        orientation="h",
        color_discrete_map={
            "Supports Higher Load": "#d94801",
            "Supports Lower Load": "#1f78b4",
        },
        title=title,
        labels={
            "importance": "Mean Absolute Contribution",
            "feature": "",
        },
        custom_data=["direction"],
    )
    fig.update_traces(
        hovertemplate=(
            "Feature=%{y}<br>"
            "Mean Absolute Contribution=%{x:.1f}<br>"
            "Mean Signed Contribution=%{customdata[0]:.1f}<extra></extra>"
        )
    )
    fig.update_layout(height=420, margin=dict(l=10, r=10, t=60, b=10))
    return fig


def make_timestep_interpretation_figure(payload: dict, timestep_idx: int) -> go.Figure:
    plot_df = pd.DataFrame(
        {
            "feature": payload["feature_names"],
            "contribution": payload["values"][:, timestep_idx],
        }
    ).sort_values("contribution", ascending=True)
    plot_df["effect"] = np.where(
        plot_df["contribution"] >= 0, "Supports Higher Load", "Supports Lower Load"
    )

    fig = px.bar(
        plot_df,
        x="contribution",
        y="feature",
        color="effect",
        orientation="h",
        color_discrete_map={
            "Supports Higher Load": "#d94801",
            "Supports Lower Load": "#1f78b4",
        },
        title=(
            "Single Timestep Interpretation: "
            f"{payload['timestamps'][timestep_idx].tz_convert(local_tz):%Y-%m-%d %H:%M}"
        ),
        labels={
            "contribution": "Contribution",
            "feature": "",
        },
    )

    if payload.get("lower") is not None and payload.get("upper") is not None:
        lower = payload["lower"][:, timestep_idx]
        upper = payload["upper"][:, timestep_idx]
        for i, row in plot_df.reset_index(drop=True).iterrows():
            feature_idx = payload["feature_names"].index(row["feature"])
            fig.add_trace(
                go.Scatter(
                    x=[lower[feature_idx], upper[feature_idx]],
                    y=[row["feature"], row["feature"]],
                    mode="lines",
                    line=dict(color="rgba(51, 160, 44, 0.65)", width=3),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

    fig.update_layout(height=420, margin=dict(l=10, r=10, t=60, b=10))
    return fig


def render_interpretation_panel() -> None:
    available_models = []
    if st.session_state.get("bayesian_interpretation") is not None:
        available_models.append("Bayesian LSTM")
    if st.session_state.get("vanilla_interpretation") is not None:
        available_models.append("Vanilla LSTM")
    if st.session_state.get("prophet_interpretation") is not None:
        available_models.append("Prophet")

    if not available_models:
        return

    st.divider()
    st.subheader("Interpret Forecast")

    control_col1, control_col2 = st.columns([1.2, 1.2])
    model_name = control_col1.radio(
        "Model",
        options=available_models,
        horizontal=True,
        key="interpretation_model",
    )
    view_mode = control_col2.radio(
        "View",
        options=("Overview", "Day Part", "Timestep"),
        horizontal=True,
        key="interpretation_view",
    )

    payload = get_interpretation_payload(model_name)

    if view_mode == "Overview":
        fig = make_aggregated_interpretation_figure(
            payload,
            slice(0, len(payload["timestamps"])),
            "Whole Forecast Day Interpretation",
        )
        st.plotly_chart(fig, width="stretch")
    elif view_mode == "Day Part":
        day_part = st.radio(
            "Period",
            options=tuple(INTERPRETATION_DAY_PARTS),
            horizontal=True,
            key="interpretation_day_part",
        )
        fig = make_aggregated_interpretation_figure(
            payload,
            INTERPRETATION_DAY_PARTS[day_part],
            f"{day_part} Interpretation",
        )
        st.plotly_chart(fig, width="stretch")
    else:
        timestamp_labels = [
            ts.tz_convert(local_tz).strftime("%Y-%m-%d %H:%M")
            for ts in payload["timestamps"]
        ]
        selected_label = st.select_slider(
            "Forecast timestep",
            options=timestamp_labels,
            value=timestamp_labels[min(24, len(timestamp_labels) - 1)],
            key="interpretation_timestep",
        )
        timestep_idx = timestamp_labels.index(selected_label)
        metric_col1, metric_col2 = st.columns(2)
        metric_col1.metric("Selected Forecast Time", selected_label)
        metric_col2.metric("Model", model_name)
        fig = make_timestep_interpretation_figure(payload, timestep_idx)
        st.plotly_chart(fig, width="stretch")

def render_load_figure():
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

# load the scaler for the lstm input scaling
scaler = load_scaler()
x_scaler = load_x_scaler()

# init the widgets
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
    # Start by initializing the date conversions
    eic_code = area_eic_code_map.get(area_code)
    start_local = start.replace(tzinfo=local_tz)
    end_local = end.replace(tzinfo=local_tz)
    start_utc = widget_value_to_utc(start)
    end_utc = widget_value_to_utc(end)

    # The forecast date marks the one day before of the last data
    # To ensure all data for a day is available e.g., 07-04-2026T14:13 becomes 06-04-2026T00:00
    forecast_start_local = end_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    forecast_start_utc = forecast_start_local.astimezone(timezone.utc)

    # fetch 7 day window as the LSTM input
    start_week_ahead = forecast_start_utc - timedelta(days=7, minutes=15) 

    
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
            # display the load data and forecast traces in a plotly figure
            render_load_figure()

            # display if there were some errors in the interpretation calculations
            interpretation_errors = st.session_state.get("interpretation_errors", {})
            for error_message in interpretation_errors.values():
                st.error(error_message)
            render_interpretation_panel()

            if st.button("Run forecast"):
                weather_combined = pd.DataFrame()
                # fetch the data for each variable and combine them into 
                # a single dataframe
                for variable in client.WEATHER_VARIABLE_META:
                    weather_series = client.get_weather_data(
                        client.WeatherDataQuery(
                            start=start_week_ahead,#.replace(minute=0),
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

                # combine the load series with the weather series and ffill the 
                # single hours into the 15min intervals
                aligned_df = align_load_weather_series(load_df, weather_combined)

                run_forcast_prophet(aligned_df, forecast_start_utc)
                run_forcast_lstm(aligned_df, scaler, forecast_start_utc)
                
                st.rerun()
            

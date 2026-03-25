from enum import StrEnum
from datetime import datetime, timedelta, timezone
import plotly.express as px
import streamlit as st
import numpy as np
import mlflow
import torch
import client
import pandas as pd
import holidays

from sklearn.preprocessing import RobustScaler, FunctionTransformer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer

class ModelType(StrEnum):
    LSTM = "lstm"
    PROPHET = "prophet"


def add_features(X: pd.DataFrame) -> pd.DataFrame:
    pass

def add_features(X: pd.DataFrame) -> pd.DataFrame:
    X = X.copy()
    X["wind_speed"] = (X["u10"] ** 2 + X["v10"] ** 2) ** 0.5
    X["ssrd"] = X["ssrd"] / 3600
    X["tp"] = X["tp"] / 3600
    X.drop(columns=["u10", "v10"], inplace=True)

    idx = pd.DatetimeIndex(pd.to_datetime(X.index, utc=True))

    X["is_weekday"] =idx.weekday < 5
    # Austria public holidays
    years = range(idx.min().year, idx.max().year + 1)
    at_holidays = holidays.country_holidays(st.session_state["area_code"], years=years)
    X["is_holiday"] = idx.map(lambda ts: ts.date() in at_holidays).astype("int8")
    return X

def make_prophet_frame(df: pd.DataFrame, target: str) -> pd.DataFrame:
    df = add_features(df)
    prophet_df = df.rename(columns={target: "y"})
    prophet_df["ds"] = prophet_df.index
    prophet_df["ds"]=prophet_df["ds"].dt.tz_convert(None) # remove as prophet cant handle tz
    return prophet_df

def run_forcast(data: pd.DataFrame) -> client.LoadSeries:
    # Run Prophet forecast
    prophet_df = make_prophet_frame(df=data, target="load_mw")
    prophet_pred = prophet_model.predict(prophet_df.drop(columns=["y"]))["yhat"].to_numpy(dtype=np.float64)
    timestamp = pd.DatetimeIndex(pd.to_datetime(data.index, utc=True))

    return client.LoadSeries(
        timestamp=timestamp,
        total_load=prophet_pred
    )
    # Run LSTM Forecasts
    # feature_builder = FunctionTransformer(add_features, validate=False)

    # preprocessor = Pipeline([
    #     ("feature_builder", feature_builder),
    #     ("columns", ColumnTransformer(
    #         transformers=[
    #         ("scale_num", RobustScaler(), ["load_mw", "t2m", "wind_speed", "tp_rate", "ssrd_rate"])
    #     ],
    #     remainder="passthrough",
    #     verbose_feature_names_out=False
    #     ))
    # ])
    # preprocessor.set_output(transform="pandas")
    # st.write(preprocessor.fit_transform(data))


def align_load_weather_series(load_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    df_combined = pd.concat([load_df, weather_df], axis=1, join="outer")
    df_combined.ffill(axis=0, inplace=True)
    return df_combined.iloc[1:]


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
    pass

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
# bayesian_lstm = load_forecasting_model("BayesianLSTM", model_type=ModelType.LSTM)
# vanilla_lstm = load_forecasting_model("VanillaLSTM", model_type=ModelType.LSTM)
prophet_model = load_forecasting_model("Prophet", model_type=ModelType.PROPHET)



area_eic_code_map = {"AT": "10YAT-APG------L"}
col1, col2, col3, col4 = st.columns(4)
area_code = col1.selectbox(label="Area", options=("AT",), key="area_code")
start = col2.datetime_input(
    label="From",
    value=st.session_state["latest_common_ts"] - timedelta(days=1),
    key="forecast_start",
)
end = col3.datetime_input(
    label="To", value=st.session_state["latest_common_ts"], key="forecast_end"
)


if start is not None and end is not None:
    eic_code = area_eic_code_map.get(area_code)
    start = start.replace(tzinfo=timezone.utc)
    end = end.replace(tzinfo=timezone.utc)

    start_week_ahead = start - timedelta(days=7)  # fetch 7 day window as the LSTM input

    if eic_code:
        load_data = client.get_load_data(
            client.LoadDataQuery(
                start=start_week_ahead,
                end=end,
                eic_code=eic_code,
            )
        )

        load_df = load_data.to_frame()

        if not load_data.empty:
            fig = px.line(
                load_df[load_df.index >= start],
                x=load_df.index[load_df.index >= start],
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
                        line=dict(color="#d97212")
                    )
            st.plotly_chart(fig)

            if st.button("Run forecast"):
                weather_combined = pd.DataFrame()
                for variable in client.WEATHER_VARIABLE_META:
                    weather_series = client.get_weather_data(
                        client.WeatherDataQuery(
                            start=start_week_ahead.replace(minute=0),
                            end=end.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1),
                            area_code=area_code,
                            variable=variable,
                        )
                    )
                    weather_df = weather_series.to_frame()
                    weather_combined = pd.concat([weather_combined, weather_df], axis=1, join="outer")
                aligned_df = align_load_weather_series(load_df, weather_combined)
                forecast_series = run_forcast(aligned_df)

                if "prophet_forecast" not in st.session_state:
                    st.session_state["prophet_forecast"] = forecast_series.to_frame()
                    st.rerun()
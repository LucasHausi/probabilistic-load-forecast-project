import streamlit as st
import requests
from datetime import datetime, timedelta, time, timezone
from dataclasses import dataclass
import plotly.express as px
import numpy as np

import client

BASE_URL = st.secrets["api"]["base_url"]


col1, col2, col3, col4 = st.columns(4)
area_code = col1.selectbox(label="Area", options=("AT",), key="area_code")
start = col2.date_input(label="From", value="today")
end = col3.date_input(label="To", value=datetime.today() + timedelta(days=+1))
weather_variable = col4.selectbox(
    label="Variable",
    options=tuple(client.WEATHER_VARIABLE_META),
    key="weather_variable",
)


weather_data = client.get_weather_data(
    query=client.WeatherDataQuery(
        start=datetime.combine(start, time.min, tzinfo=timezone.utc),
        end=datetime.combine(end, time.max, tzinfo=timezone.utc),
        area_code=area_code,
        variable=weather_variable,
    )
)


if not weather_data.empty:
    selected_variable = client.WEATHER_VARIABLE_META[weather_variable]

    fig = px.line(
        x=weather_data.timestamp,
        y=weather_data.value,
        title=selected_variable["title"],
        labels={
            "x": "Timestamp (UTC)",
            "y": selected_variable["y_label"],
        },
        color_discrete_sequence=[selected_variable["color"]],
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig)

from pathlib import Path
import streamlit as st

st.title("Interpretable Load Forecast")


page = st.navigation(
    pages={
        "Data": [
            st.Page(page=Path("./Weather_data.py"), title="Weather Data", icon=":material/weather_mix:"),
            st.Page(page=Path("./Load_data.py"), title="Load Data", icon=":material/electric_bolt:"),
        ],
        "Forecasts": [
            st.Page(page=Path("./Load_forecast.py"), title="Load Forecast", icon=":material/online_prediction:"),
        ]
    }
)

page.run()

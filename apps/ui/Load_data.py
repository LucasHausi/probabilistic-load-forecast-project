import streamlit as st
from datetime import datetime, timedelta, time, timezone
import plotly.express as px
import numpy as np

import client


col1, col2, col3 = st.columns(3)
eic_code = col1.selectbox(label="Zone Code", options=("10YAT-APG------L"))
start = col2.date_input(label="From", value="today")
end = col3.date_input(label="From", value=datetime.today() + timedelta(days=+1))


load_data = client.get_load_data(
    query=client.LoadDataQuery(
        start=datetime.combine(start, time.min, tzinfo=timezone.utc),
        end=datetime.combine(end, time.max, tzinfo=timezone.utc),
        eic_code=eic_code,
    )
)

if not load_data.empty:
    fig = px.line(
        x=load_data.timestamp,
        y=load_data.total_load,
        title="Historical Load Data",
        labels={
            "x": "Timestamp (UTC)",
            "y": "Load [MW]",
        },
    )
    st.plotly_chart(fig)

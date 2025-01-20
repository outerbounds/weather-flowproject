import streamlit as st
from metaflow import Flow, Run
from weather import forecastviz


@st.cache_data
def load_data(pathspec):
    data = Run(pathspec).data
    return data.forecasts, data.timestamp


run = Flow("ForecastFlow").latest_successful_run
st.markdown(f"🌈 Loading data from **{run.pathspec}**")
forecasts, updated_at = load_data(run.pathspec)
st.markdown(
    f"✨ Loaded forecasts for **{len(forecasts)}** zipcodes, updated at **{updated_at}**"
)
zipcode = st.selectbox(label="Choose a zipcode", options=list(forecasts))

st.markdown(f"### Forecast for {zipcode}")
st.markdown("**Blue** is historical data, **orange** is our forecast")
past, future = forecasts[zipcode]
st.vega_lite_chart(spec=forecastviz.vegaspec(past, future))

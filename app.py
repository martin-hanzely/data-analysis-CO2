from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

import dash
import pandas as pd
import plotly.express as px
import sentry_sdk
from data.conf import get_app_settings
from data.loaders.influxdb_loader import InfluxDBLoader

if TYPE_CHECKING:
    from plotly.graph_objs import Figure


logger = logging.getLogger(__name__)

settings = get_app_settings()

sentry_sdk.init(
    debug=settings.debug,
    dsn=settings.sentry_dsn,
    enable_tracing=True,
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)

external_stylesheets = [{
    "href": "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css",
    "rel": "stylesheet",
    "integrity": "sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH",
    "crossorigin": "anonymous"
}]

app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
)

server = app.server

loader = InfluxDBLoader(settings=settings)

base_date = dt.date(2024, 1, 1)
date_range = 7

# Load `df` in global state
logger.info("Loading data from InfluxDB")
df = loader.retrieve_dataframe(
    dt_from=pd.Timestamp(base_date, tz="UTC"),
    dt_to=pd.Timestamp(base_date + dt.timedelta(date_range), tz="UTC"),
)
logger.info("Data loaded")

# TODO: Move this to ETL pipeline!
logger.info("Performing aggregation")
df["latitude"] = df["latitude"].apply(lambda x: round(x))
df["longitude"] = df["longitude"].apply(lambda x: round(x))
df["date"] = df["_time"].apply(lambda x: x.date())
df = df.groupby(["latitude", "longitude", "date"]).mean(numeric_only=True).reset_index()

xco2_min = df["xco2"].min()
xco2_max = df["xco2"].max()

app.layout = dash.html.Div([
    dash.html.H1(children="Data analysis CO2", style={"textAlign": "center"}),
    dash.html.Div([
        dash.dcc.Graph(id="graph-content"),
        dash.dcc.RangeSlider(
            min=0,
            max=date_range,
            step=1,
            value=[0, date_range],
            id="range-slider",
        ),
    ])
])


@dash.callback(
    dash.Output(component_id="graph-content", component_property="figure"),
    dash.Input(component_id="range-slider", component_property="value"),
)
def update_output_div(value: list[int]) -> Figure:
    try:
        date_from = base_date + dt.timedelta(value[0])
        date_to = base_date + dt.timedelta(value[1])
    except (TypeError, IndexError) as e:
        logger.error("Invalid date range", exc_info=e)
        raise dash.exceptions.PreventUpdate

    _df = df[df["date"].between(date_from, date_to)]

    fig = px.scatter_geo(
        _df,
        lat="latitude",
        lon="longitude",
        color="xco2",
        hover_data=["xco2"],
        range_color=[xco2_min, xco2_max],
    )
    return fig

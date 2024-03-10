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

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server

loader = InfluxDBLoader(settings=settings)

app.layout = dash.html.Div([
    dash.html.H1(children="Data analysis CO2", style={"textAlign": "center"}),
    dash.html.Div([
        dash.html.Label("Input"),
        dash.dcc.DatePickerSingle(
            id="date-picker",
            min_date_allowed=dt.date(2024, 1, 1),
            max_date_allowed=dt.date(2024, 1, 31),
            initial_visible_month=dt.date(2024, 1, 1),
            date=dt.date(2024, 1, 1),
            display_format="YYYY-MM-DD",
        )
    ]),
    dash.html.Div([
        dash.dcc.Graph(id="graph-content"),
    ])
])


@dash.callback(
    dash.Output(component_id="graph-content", component_property="figure"),
    dash.Input(component_id="date-picker", component_property="date")
)
def update_output_div(date: str) -> Figure:
    _d = dt.datetime.strptime(date, "%Y-%m-%d")
    logger.info("Input value: %s", _d)
    df = loader.retrieve_dataframe(
        dt_from=pd.Timestamp(_d, tz="UTC"),
        dt_to=pd.Timestamp(_d, tz="UTC") + pd.Timedelta(days=1),
    )

    df["date"] = df["_time"].apply(lambda x: x.date())

    # Aggregate data per degree
    df["latitude"] = df["latitude"].apply(lambda x: round(x))
    df["longitude"] = df["longitude"].apply(lambda x: round(x))
    df = df.groupby(["latitude", "longitude", "date"]).mean(numeric_only=True).reset_index()

    fig = px.scatter_geo(
        df,
        lat="latitude",
        lon="longitude",
        color="xco2",
        hover_data=["xco2"],
        animation_frame="date",
    )
    return fig

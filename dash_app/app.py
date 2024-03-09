import pandas as pd
import plotly.express as px
import sentry_sdk
from dash import Dash, html, dcc

from data.conf import get_app_settings
from data.loaders.local_csv_loader import LocalCSVLoader


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

app = Dash(__name__, external_stylesheets=external_stylesheets)

_l = LocalCSVLoader()
df = _l.retrieve_dataframe()

df["datetime"] = pd.to_datetime(df["datetime"])
df["date"] = df["datetime"].apply(lambda x: x.date())

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

app.layout = html.Div([
    html.H1(children="Data analysis CO2", style={"textAlign": "center"}),
    dcc.Graph(id="graph-content", figure=fig),
])

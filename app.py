from __future__ import annotations

import datetime as dt
import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable, TypedDict

import dash
import pandas as pd
import plotly.express as px
import sentry_sdk

from data.conf import get_app_settings
from data.loaders.local_parquet_loader import LocalParquetLoader

if TYPE_CHECKING:
    from plotly.graph_objs import Figure


class CallbackManagerCacheSettings(TypedDict):
    cache_by: list[Callable[[], Any]]
    expire: int


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

# Enable background callback processing and cache.
launch_uid = uuid.uuid4()
callback_manager_cache_settings: CallbackManagerCacheSettings = {"cache_by": [lambda: launch_uid], "expire": 60}

if settings.celery_enabled:
    from data.celery import app as celery_app

    background_callback_manager = dash.CeleryManager(celery_app, **callback_manager_cache_settings)
else:
    from diskcache import Cache

    cache = Cache("cache")
    background_callback_manager = dash.DiskcacheManager(cache, **callback_manager_cache_settings)

app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    background_callback_manager=background_callback_manager,
)

server = app.server

_today = dt.date.today()

loader = LocalParquetLoader()
# loader = S3ParquetLoader(settings=settings)

df = loader.retrieve_dataframe("out_/oco2_daily_avg.gzip")
df["date"] = pd.to_datetime(df["date"], errors="coerce")
# TODO: Pass datetime columns to the loader and let it handle the conversion.

# FIGURE 1
df["normalized_date"] = pd.to_datetime(df["date"].dt.strftime("%d-%b-1900"), errors="coerce")
df["year"] = df["date"].dt.year

fig1 = px \
    .line(df, x="normalized_date", y="average", color="year") \
    .update_layout(xaxis={"dtick": "M1", "tickformat": "%d-%b"})

# FIGURE 2
fig2 = px.line(df, x="date", y="average", title="OCO-2")
fig2.update_xaxes(rangeslider_visible=True)

# FIGURE 3
fig3 = px.scatter(df, x="date", y=df.columns[1:4], title="OCO-2")

# FIGURE 4
df_mlo = loader.retrieve_dataframe("out_/mlo.gzip")
df_mlo["date"] = pd.to_datetime(df_mlo["date"], errors="coerce")

fig4 = px.line(df_mlo, x="date", y="value", title="MLO")

# FIGURE 5 SLIDER
df_avg_per_month_lat_lon = loader.retrieve_dataframe("out_/monthly_avg_per_lat_lon.gzip")

app.layout = dash.html.Div(
    className="container-fluid",
    children=[
        dash.html.H1("Bakalárska práca", className="h3"),
        dash.html.P("Analýza a vizualizácia obsahu oxidu uhličitého v zemskej atmosfére na základe verejne dostupných dát v súvislosti so zmenami klímy."),
        dash.html.Div(
            className="row row-cols-2",
            children=[
                dash.html.Div(dash.dcc.Graph(figure=fig1), className="col"),
                dash.html.Div(dash.dcc.Graph(figure=fig2), className="col"),
                dash.html.Div(dash.dcc.Graph(figure=fig3), className="col"),
                dash.html.Div(dash.dcc.Graph(figure=fig4), className="col"),
            ]
        ),
        dash.html.Div(
            children=[
                dash.dcc.Graph(id="graph-content5", style={"height": "100vh"}),
                dash.dcc.Slider(
                    min=2024,
                    max=2023,
                    step=1,
                    value=2023,
                    marks={2023: "Rok 2023", 2024: "Rok 2024"},
                    id="year-slider"
                ),
                dash.dcc.Slider(
                    min=1,
                    max=12,
                    step=1,
                    value=1,
                    marks={1: "1", 12: "12"},
                    id="month-slider"
                ),
            ],
        ),
        dash.html.Div(
            className="py-4",
            children=[
                dash.html.H5("Zdroje dát"),
                dash.html.P([
                    f"OCO-2/OCO-3 Science Team, Vivienne Payne, Abhishek Chatterjee (2022), OCO-2 Level 2 geolocated XCO2 retrievals results, physical model V11, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: {_today.isoformat()}, ",
                    dash.html.A(href="https://disc.gsfc.nasa.gov/datacollection/OCO2_L2_Standard_11.html", children="Link")
                ]),
                dash.html.P([
                    f"OCO-2/OCO-3 Science Team, Vivienne Payne, Abhishek Chatterjee (2022), OCO-2 Level 2 bias-corrected XCO2 and other select fields from the full-physics retrieval aggregated as daily files, Retrospective processing V11.1r, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: {_today.isoformat()}, ",
                    dash.html.A(href="https://doi.org/10.5067/8E4VLCK16O6Q", children="10.5067/8E4VLCK16O6Q")
                ]),
                dash.html.P([
                    f"Lan, X., J.W. Mund, A.M. Crotwell, M.J. Crotwell, E. Moglia, M. Madronich, D. Neff and K.W. Thoning (2023), Atmospheric Carbon Dioxide Dry Air Mole Fractions from the NOAA GML Carbon Cycle Cooperative Global Air Sampling Network, 1968-2022, Version: 2023-08-28, Accessed: {_today.isoformat()}, ",
                    dash.html.A(href="https://doi.org/10.15138/wkgj-f215", children="10.15138/wkgj-f215")
                ]),
            ],
        )
    ]
)


@dash.callback(
    dash.Output(component_id="graph-content5", component_property="figure"),
    dash.Input(component_id="year-slider", component_property="value"),
    dash.Input(component_id="month-slider", component_property="value"),
    background=True,
    running=[
        (dash.Output(component_id="year-slider", component_property="disabled"), True, False),
        (dash.Output(component_id="month-slider", component_property="disabled"), True, False),
    ]
)
def update_graph(
        year: int,
        month: int,
        # loader_: BaseLoader = loader
) -> Figure:
    # 5
    df_ = df_avg_per_month_lat_lon.loc[
        (df_avg_per_month_lat_lon["year"] == year) & (df_avg_per_month_lat_lon["month"] == month)
    ]
    fig5 = px.scatter_geo(
        df_,
        lat="latitude",
        lon="longitude",
        color="xco2",
        hover_data=["xco2"],
    )
    fig5.update_geos(projection_type="orthographic")

    return fig5


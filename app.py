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
from data.loaders.s3_parquet_loader import S3ParquetLoader

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

# region CACHE
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
# endregion

app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    background_callback_manager=background_callback_manager,
)

server = app.server

loader = S3ParquetLoader(settings=settings)

# region FIGURES
df = loader.retrieve_dataframe("out_/oco2_daily_avg.gzip")
df["date"] = pd.to_datetime(df["date"], errors="coerce")
# TODO: Pass datetime columns to the loader and let it handle the conversion.

# Global daily average 2023 and 2024
df["normalized_date"] = pd.to_datetime(df["date"].dt.strftime("%d-%b-1900"), errors="coerce")
df["year"] = df["date"].dt.year
global_daily_avg_2023_2024 = px \
    .line(
        df,
        x="normalized_date",
        y="average",
        color="year",
        title="Porovnanie priemerných denných hodnôt XCO2 v roku 2023 a 2024",
        labels={"normalized_date": "Dátum", "average": "XCO2 (ppm)", "year": "Rok"},
    ) \
    .update_layout(xaxis={"dtick": "M1", "tickformat": "%d-%b"})

# Global daily average since 2023-01-01
global_daily_avg_since_2023 = px \
    .line(
        df,
        x="date",
        y="average",
        title="Priemerná denná hodnota XCO2 od 2023-01-01",
        labels={"date": "Dátum", "average": "XCO2 (ppm)"},
    ) \
    .update_xaxes(rangeslider_visible=True)

# Daily average in 2023 global / EU / SK
daily_avg_glob_eu_sk = px.scatter(
    df,
    x="date",
    y=df.columns[1:4],
    title="Porovnanie priemerných denných hodnôt XCO2 v roku 2023",
    labels={"date": "Dátum", "value": "XCO2 (ppm)", "variable": "Región"},
)

# Daily average MLO since 1975-01-01
df_mlo = loader.retrieve_dataframe("out_/mlo.gzip")
df_mlo["date"] = pd.to_datetime(df_mlo["date"], errors="coerce")
mlo_since_1975 = px.line(
    df_mlo,
    x="date",
    y="value",
    title="Priemerná denná hodnota CO2 na MLO od 1975-01-01",
    labels={"date": "Dátum", "value": "CO2 (ppm)"},
)

# Global monthly average per whole latitude and longitude
df_avg_per_month_lat_lon = loader.retrieve_dataframe("out_/monthly_avg_per_lat_lon.gzip")
_min = df_avg_per_month_lat_lon["xco2"].min()
_max = df_avg_per_month_lat_lon["xco2"].max()

_min_year = df_avg_per_month_lat_lon["year"].min()
_max_year = df_avg_per_month_lat_lon["year"].max()
# endregion

# region LAYOUT
app.layout = dash.html.Div(
    className="container-fluid",
    children=[
        dash.html.Div(  # Header.
            className="my-3",
            children=[
                dash.html.H1("Bakalárska práca"),
                dash.html.P("Analýza a vizualizácia obsahu oxidu uhličitého v zemskej atmosfére na základe verejne dostupných dát v súvislosti so zmenami klímy."),
            ],
        ),
        dash.html.Div(  # Graphs.
            className="row row-cols-1 row-cols-lg-2",
            children=[
                dash.html.Div(dash.dcc.Graph(figure=global_daily_avg_2023_2024), className="col"),
                dash.html.Div(dash.dcc.Graph(figure=global_daily_avg_since_2023), className="col"),
                dash.html.Div(dash.dcc.Graph(figure=daily_avg_glob_eu_sk), className="col"),
                dash.html.Div(dash.dcc.Graph(figure=mlo_since_1975), className="col"),
            ]
        ),
        dash.html.Div(  # Globe.
            className="mx-3",
            children=[
                dash.dcc.Graph(id="avg-per-month-lat-lon", style={"height": "100vh"}),
                dash.html.Div(
                    className="row",
                    children=[
                        dash.dcc.Slider(
                            className="col-12 col-lg-10",
                            min=1,
                            max=12,
                            step=1,
                            value=1,
                            marks={_m: dt.datetime.strftime(dt.date(1900, _m, 1), "%B") for _m in range(1, 13)},
                            id="month-slider"
                        ),
                        dash.dcc.Slider(
                            className="col-12 col-lg-2",
                            min=_min_year,
                            max=_max_year,
                            step=1,
                            value=_max_year,
                            marks={_y: str(_y) for _y in range(_min_year, _max_year + 1)},
                            id="year-slider"
                        ),
                    ],
                ),
            ],
        ),
        dash.html.Div(  # Data sources.
            className="my-5",
            style={"font-size": "0.6rem"},
            children=[
                dash.html.P([
                    f"OCO-2/OCO-3 Science Team, Vivienne Payne, Abhishek Chatterjee (2022), OCO-2 Level 2 geolocated XCO2 retrievals results, physical model V11, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: 2024-05-01, ",
                    dash.html.A(href="https://disc.gsfc.nasa.gov/datacollection/OCO2_L2_Standard_11.html", children="Link")
                ]),
                dash.html.P([
                    f"OCO-2/OCO-3 Science Team, Vivienne Payne, Abhishek Chatterjee (2022), OCO-2 Level 2 bias-corrected XCO2 and other select fields from the full-physics retrieval aggregated as daily files, Retrospective processing V11.1r, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: 2024-05-01, ",
                    dash.html.A(href="https://doi.org/10.5067/8E4VLCK16O6Q", children="10.5067/8E4VLCK16O6Q")
                ]),
                dash.html.P([
                    f"Lan, X., J.W. Mund, A.M. Crotwell, M.J. Crotwell, E. Moglia, M. Madronich, D. Neff and K.W. Thoning (2023), Atmospheric Carbon Dioxide Dry Air Mole Fractions from the NOAA GML Carbon Cycle Cooperative Global Air Sampling Network, 1968-2022, Version: 2023-08-28, Accessed: 2024-05-01, ",
                    dash.html.A(href="https://doi.org/10.15138/wkgj-f215", children="10.15138/wkgj-f215")
                ]),
            ],
        )
    ]
)
# endregion


@dash.callback(
    dash.Output(component_id="avg-per-month-lat-lon", component_property="figure"),
    dash.Input(component_id="year-slider", component_property="value"),
    dash.Input(component_id="month-slider", component_property="value"),
    background=True,
    running=[
        (dash.Output(component_id="year-slider", component_property="disabled"), True, False),
        (dash.Output(component_id="month-slider", component_property="disabled"), True, False),
    ]
)
def update_graph(year: int, month: int) -> Figure:
    df_ = df_avg_per_month_lat_lon.loc[
        (df_avg_per_month_lat_lon["year"] == year) &
        (df_avg_per_month_lat_lon["month"] == month)
    ]
    fig = px.scatter_geo(
        df_,
        lat="latitude",
        lon="longitude",
        color="xco2",
        range_color=[_min, _max],
        color_continuous_scale="jet",
        hover_data=["xco2"],
        title="Priemerná mesačná hodnota XCO2 na celé číslo zemepisnej šírky a dĺžky",
        labels={"xco2": "XCO2 (ppm)"},
    )
    fig.update_geos(projection_type="orthographic")

    return fig

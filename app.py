from __future__ import annotations

import time
import datetime as dt
import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable, TypedDict

import dash
import pandas as pd
import plotly.express as px
import sentry_sdk

from data.conf import get_app_settings
from data.loaders.influxdb_loader import InfluxDBLoader

if TYPE_CHECKING:
    from plotly.graph_objs import Figure

    from data.loaders.base_loader import BaseLoader


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

_min_date = "2024-01-01"
_today = dt.date.today()

app.layout = dash.html.Div(
    className="container-fluid",
    children=[
        dash.html.Div(
            className="row",
            children=[
                dash.html.Div(
                    className="col-3 p-5 bg-body-tertiary border-end",
                    children=[
                        dash.html.H1("Bakalárska práca", className="h3"),
                        dash.html.P("Analýza a vizualizácia obsahu oxidu uhličitého v zemskej atmosfére na základe verejne dostupných dát v súvislosti so zmenami klímy."),
                        dash.html.P("Rozsah zobrazených záznamov: "),
                        dash.dcc.DatePickerRange(
                            id='date-picker-range',
                            start_date=_today - dt.timedelta(days=7),
                            end_date=_today,
                            min_date_allowed=_min_date,
                            max_date_allowed=_today,
                            initial_visible_month=_today,
                            display_format="YYYY-MM-DD",
                            first_day_of_week=1,
                            updatemode="bothdates",
                            style={"border-radius": "1rem", "width": "100%"},
                            className="mb-3",
                        ),
                        dash.html.P(["Počet záznamov: ", dash.html.Span(id="results-count")]),
                        dash.html.P([
                            "Vypracoval: Martin Hanzely ",
                            dash.html.A(
                                href="https://github.com/martin-hanzely/data-analysis-CO2",
                                children="GitHub",
                            ),
                        ]),
                        dash.html.H5("Zdroje dát"),
                        dash.html.P([
                            f"OCO-2/OCO-3 Science Team, Vivienne Payne, Abhishek Chatterjee (2022), OCO-2 Level 2 geolocated XCO2 retrievals results, physical model V11, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: {_today.isoformat()}, ",
                            dash.html.A(
                                href="https://disc.gsfc.nasa.gov/datacollection/OCO2_L2_Standard_11.html",
                                children="Link",
                            )
                        ]),
                        dash.html.P([
                            f"OCO-2/OCO-3 Science Team, Vivienne Payne, Abhishek Chatterjee (2022), OCO-2 Level 2 bias-corrected XCO2 and other select fields from the full-physics retrieval aggregated as daily files, Retrospective processing V11.1r, Greenbelt, MD, USA, Goddard Earth Sciences Data and Information Services Center (GES DISC), Accessed: {_today.isoformat()}, ",
                            dash.html.A(
                                href="https://doi.org/10.5067/8E4VLCK16O6Q",
                                children="10.5067/8E4VLCK16O6Q",
                            )
                        ]),
                    ],
                ),
                dash.html.Div(
                    [dash.dcc.Graph(id="graph-content", style={"height": "100vh"})],
                    className="col-9"
                )
            ]
        ),
    ]
)

loader = InfluxDBLoader(settings=settings)


@dash.callback(
    dash.Output(component_id="graph-content", component_property="figure"),
    dash.Output(component_id="results-count", component_property="children"),
    dash.Input(component_id="date-picker-range", component_property="start_date"),
    dash.Input(component_id="date-picker-range", component_property="end_date"),
    background=True,
    running=[(dash.Output(component_id="date-picker-range", component_property="disabled"), True, False)]
)
def update_graph(
        start_date: str,
        end_date: str,
        loader_: BaseLoader = loader
) -> tuple[Figure, int]:
    try:
        date_from = dt.date.fromisoformat(start_date)
        date_to = dt.date.fromisoformat(end_date)
    except ValueError as e:
        logger.error("Invalid date range", exc_info=e)
        raise dash.exceptions.PreventUpdate

    _time = time.time()
    df = loader_.retrieve_dataframe(
        dt_from=pd.Timestamp(date_from, tz="UTC"),
        dt_to=pd.Timestamp(date_to, tz="UTC"),
    )
    logger.info("Data loaded in %s seconds", time.time() - _time)
    logger.info("Data shape: %s", df.shape)

    fig = px.scatter_geo(
        df,
        lat="latitude",
        lon="longitude",
        color="xco2",
        hover_data=["xco2", "_time"],
    )
    fig.update_geos(projection_type="orthographic")

    return fig, df.shape[0]

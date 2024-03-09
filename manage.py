import logging

import typer


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",  # Include datetime for easier execution time tracking.
)

app = typer.Typer()


@app.command()
def invoke_etl() -> None:
    """
    Invoke ETL pipeline.
    """
    import datetime

    from data.conf import get_app_settings
    from data.etl.etl_pipeline import ETLPipeline
    from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP
    from data.loaders.influxdb_loader import InfluxDBLoader
    from data.utils.opendap import OpendapClient

    settings = get_app_settings()
    opendap_client = OpendapClient()
    extractor = OpendapExtractorL2LiteFP(settings=settings, client=opendap_client)
    loader = InfluxDBLoader(settings=settings)
    etl_pipeline = ETLPipeline(extract_strategy=extractor, load_strategy=loader)

    etl_pipeline.invoke([datetime.date(2024, 1, 15)])


@app.command()
def dash_app() -> None:
    """
    Run dash app.
    """
    from dash_app.app import app as dash_app_

    dash_app_.run(debug=True)


if __name__ == "__main__":
    app()

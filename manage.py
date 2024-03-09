import logging
from typing_extensions import Annotated

import typer


logging.basicConfig(
    level=logging.INFO,
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
    from data.loaders.local_csv_loader import LocalCSVLoader
    from data.utils.opendap import OpendapClient

    settings = get_app_settings()
    opendap_client = OpendapClient()
    extractor = OpendapExtractorL2LiteFP(settings=settings, client=opendap_client)
    loader = LocalCSVLoader()
    etl_pipeline = ETLPipeline(extract_strategy=extractor, load_strategy=loader)

    base_date = datetime.date(2024, 1, 15)
    days_range = 16
    etl_pipeline.invoke(
        (base_date + datetime.timedelta(days=i) for i in range(days_range))
    )


@app.command()
def dash_app() -> None:
    """
    Run dash app.
    """
    from dash_app.app import app as dash_app_

    dash_app_.run(debug=True)


if __name__ == "__main__":
    app()

import logging
from typing_extensions import Annotated

import typer


logging.basicConfig(
    level=logging.DEBUG,  # TODO: Set to INFO for production.
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",  # Include datetime for easier execution time tracking.
)

app = typer.Typer()


@app.command()
def invoke_etl() -> None:
    """
    Invoke ETL pipeline.
    """
    from data.etl.etl_pipeline import ETLPipeline
    from data.loaders.local_csv_loader import LocalCSVLoader

    _l = LocalCSVLoader()
    etl_pipeline = ETLPipeline(load_strategy=_l)
    etl_pipeline.invoke()


@app.command()
def dash_app(debug: Annotated[bool, typer.Option(help="Debug mode.")] = False) -> None:
    """
    Run dash app.
    """
    from dash_app.app import app

    app.run(debug=debug)


@app.command()
def debug_command() -> None:
    """
    Debug command.
    """
    import datetime
    from data.conf import get_app_settings
    from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP

    settings = get_app_settings()
    extractor = OpendapExtractorL2LiteFP(settings)
    for df in extractor.extract_date_range([datetime.date(2024, 1, 15)]):
        print(df)


if __name__ == "__main__":
    app()

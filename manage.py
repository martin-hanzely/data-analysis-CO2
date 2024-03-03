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
    from data.extractors.opendap_extractor import OpendapExtractor

    extractor = OpendapExtractor()
    extractor.extract_for_date(datetime.date(2024, 2, 1))


if __name__ == "__main__":
    app()

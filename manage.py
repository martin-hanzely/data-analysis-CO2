import logging

import typer
from typing_extensions import Annotated

from data.extractors.utils import OpendapExtractorChoices


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",  # Include datetime for easier execution time tracking.
)

app = typer.Typer()


@app.command()
def invoke_etl(
        extractor_class: OpendapExtractorChoices,
        date_from: Annotated[str, typer.Argument(help="Date from in format YYYY-MM-DD")],
        date_to: Annotated[str, typer.Argument(help="Date to in format YYYY-MM-DD")],
) -> None:
    """
    Invoke ETL pipeline.
    """
    import datetime

    from data.etl.utils import pipeline_factory

    _date_from = datetime.date.fromisoformat(date_from)
    _date_to = datetime.date.fromisoformat(date_to)
    _date_range = (_date_from + datetime.timedelta(days=i) for i in range((_date_to - _date_from).days + 1))

    pipeline = pipeline_factory(extractor_class)
    pipeline.invoke(_date_range)


@app.command()
def dash_app() -> None:
    """
    Run dash app in debug mode.
    """
    from app import app as dash_app_

    dash_app_.run(debug=True)


@app.command()
def move_influxdb_to_s3() -> None:
    """
    Move InfluxDB data to S3.
    """
    from data.utils import move_influxdb_to_s3 as move_influxdb_to_s3_

    move_influxdb_to_s3_()


if __name__ == "__main__":
    app()

import logging

from data.celery import app


logger = logging.getLogger(__name__)


# noinspection PyPep8Naming
@app.task(name="daily_etl_task")
def daily_etl_task(extractor_class: str, days_before_today: int) -> None:
    """
    Invoke ETL pipeline task.
    :param extractor_class: Extractor class name.
    :param days_before_today: Number of days before today to extract data for (1 means yesterday).
    :return:
    """
    import datetime

    from data.etl.utils import pipeline_factory

    _date = datetime.date.today() - datetime.timedelta(days=days_before_today)

    logger.info("Invoking ETL pipeline with %s for %s", extractor_class, _date)

    pipeline = pipeline_factory(extractor_class)
    pipeline.invoke([_date])


@app.task(name="debug_task")
def debug_task() -> int:
    """
    Debug task.
    :return: 0
    """
    return 0

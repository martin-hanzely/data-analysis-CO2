import logging

from data.celery import app


logger = logging.getLogger(__name__)


@app.task(name="debug_task")
def debug_task() -> int:
    """
    Debug task.
    :return: 0
    """
    logger.info("Debug task executed")
    return 0

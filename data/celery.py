import sentry_sdk
from celery import Celery, signals
from celery.schedules import crontab
from sentry_sdk.integrations.celery import CeleryIntegration

from data.conf import get_app_settings


settings = get_app_settings()

app = Celery(
    "data",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["data.tasks", "app"],
)

app.conf.beat_schedule = {
    "daily_etl_task_L2_Standard": {
        "task": "daily_etl_task",
        "schedule": crontab(minute="0", hour="4"),  # 04:00
        "kwargs": {
            # L2 Standard data are available 1 day after acquisition.
            "extractor_class": "opendap_L2_Standard",
            "days_before_today": 2
        },
    },
}

app.conf.timezone = settings.celery_timezone


@signals.celeryd_init.connect
def init_sentry(**_kwargs):
    sentry_sdk.init(
        debug=settings.debug,
        dsn=settings.sentry_dsn,
        enable_tracing=True,
        integrations=[CeleryIntegration(monitor_beat_tasks=True)],
    )

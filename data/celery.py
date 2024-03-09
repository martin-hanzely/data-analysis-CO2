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
    include=["data.tasks"],
)

app.conf.beat_schedule = {
    "debug_task": {
        "task": "debug_task",
        "schedule": crontab(minute="*/1"),  # every minute
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

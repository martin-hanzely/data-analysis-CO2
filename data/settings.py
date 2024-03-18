from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings.
    """
    model_config = SettingsConfigDict(extra="ignore")

    debug: bool = False

    # EARTHDATA
    earthdata_base_url: str
    earthdata_username: str
    earthdata_password: str

    # CELERY
    celery_enabled: bool
    celery_broker_url: str
    celery_result_backend: str
    celery_timezone: str = "Europe/Bratislava"

    # INFLUXDB
    influxdb_url: str
    influxdb_token: str
    influxdb_org: str
    influxdb_bucket: str

    # SENTRY
    sentry_dsn: str

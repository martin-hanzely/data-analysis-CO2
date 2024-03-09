from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings.
    """
    earthdata_base_url: str
    earthdata_username: str
    earthdata_password: str

    # CELERY
    celery_broker_url: str
    celery_result_backend: str

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings.
    """
    earthdata_username: str
    earthdata_password: str

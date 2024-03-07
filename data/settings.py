from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings.
    """
    earthdata_base_url: str
    earthdata_username: str
    earthdata_password: str

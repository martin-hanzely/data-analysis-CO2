from data.settings import Settings


def get_app_settings() -> Settings:
    """
    Get application settings.
    :return:
    """
    return Settings(_env_file=".env", _env_file_encoding="utf-8")

import pandas as pd
import pytest

from data.settings import Settings


@pytest.fixture(scope="session")
def dummy_settings() -> Settings:
    """
    Settings override for testing purposes.
    :return:
    """
    return Settings(
        debug=True,
        earthdata_base_url="https://testbaseurl.com",
        earthdata_username="testuser",
        earthdata_password="testpassword",

        celery_enabled=False,
        celery_broker_url="",
        celery_result_backend="",

        influxdb_url="http://localhost:8086",
        influxdb_token="dummy-token",
        influxdb_org="dummy-org",
        influxdb_bucket="data-analysis-CO2",

        sentry_dsn="",
    )


@pytest.fixture
def dummy_df() -> pd.DataFrame:
    """
    Simple `pandas.DataFrame` simulating real extracted data.
    :return:
    """
    return pd.DataFrame({
        "_time": pd.to_datetime(["2024-01-01T01:00", "2024-01-01T02:01", "2024-01-01T03:02"], utc=True),
        "latitude": [-0.1, 0.0, 0.1],
        "longitude": [0.1, 0.0, -0.1],
        "xco2": [420.1, 420.0, 420.1],
        "country": ["NA", "SK", "NA"],
    })

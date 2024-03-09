import pytest

from data.loaders.dummy_loader import DummyLoader
from data.settings import Settings


@pytest.fixture
def dummy_settings():
    return Settings(
        earthdata_base_url="https://testbaseurl.com",
        earthdata_username="testuser",
        earthdata_password="testpassword",
    )


@pytest.fixture
def dummy_loader():
    return DummyLoader()

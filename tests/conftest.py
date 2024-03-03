import pytest

from data.loaders.dummy_loader import DummyLoader


@pytest.fixture
def dummy_loader():
    return DummyLoader()

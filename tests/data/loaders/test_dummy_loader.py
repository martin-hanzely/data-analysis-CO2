import pandas as pd
import pytest

from data.loaders.dummy_loader import DummyLoader
from data.loaders.exceptions import LoaderError


class TestDummyLoader:
    @pytest.fixture
    def dummy_loader(self) -> DummyLoader:
        return DummyLoader()

    # noinspection DuplicatedCode
    def test_loader_workflow(self, dummy_df, dummy_loader):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        expected_df = pd.DataFrame({
            "_time": pd.to_datetime(["2024-01-01T02:01"], utc=True),
            "latitude": [0.0],
            "longitude": [0.0],
            "xco2": [420.0],
            "country": ["SK"],
        })

        dummy_loader.save_dataframe(dummy_df)
        loaded_df = dummy_loader.retrieve_dataframe(
            dt_from=pd.to_datetime("2024-01-01T01:30", utc=True),
            dt_to=pd.to_datetime("2024-01-01T02:30", utc=True),
        )
        loaded_df = loaded_df.reset_index(drop=True)

        pd.testing.assert_frame_equal(loaded_df, expected_df)

    def test_retrieve_dataframe__raises_error_if_no_dataframe(self, dummy_loader):
        assert dummy_loader._df is None, "Test setup is incorrect"  # Sanity check

        with pytest.raises(LoaderError):
            dummy_loader.retrieve_dataframe(
                dt_from=pd.to_datetime("2024-01-01T01:30", utc=True),
                dt_to=pd.to_datetime("2024-01-01T02:30", utc=True),
            )

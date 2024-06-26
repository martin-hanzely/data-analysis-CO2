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

        dummy_loader.save_dataframe(dummy_df, file_name="2024-01-01")
        loaded_df = dummy_loader.retrieve_dataframe(file_name="2024-01-01")
        loaded_df = loaded_df.reset_index(drop=True)

        pd.testing.assert_frame_equal(loaded_df, dummy_df)

    def test_retrieve_dataframe__raises_error_if_no_dataframe(self, dummy_loader):
        assert dummy_loader._df is None, "Test setup is incorrect"  # Sanity check

        with pytest.raises(LoaderError):
            dummy_loader.retrieve_dataframe(file_name="2024-01-01")

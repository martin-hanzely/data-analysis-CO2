import pandas as pd
import pytest

from data.loaders.local_csv_loader import LocalCSVLoader


class TestLocalCSVLoader:
    @pytest.fixture
    def local_csv_loader(self) -> LocalCSVLoader:
        return LocalCSVLoader(in_memory=True)

    def test_loader_workflow(self, dummy_df, local_csv_loader):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        local_csv_loader.save_dataframe(dummy_df)

        df_ = local_csv_loader.retrieve_dataframe()

        pd.testing.assert_frame_equal(df_, dummy_df)

import pandas as pd
import pytest

from data.loaders.local_csv_loader import LocalCSVLoader


class TestLocalCSVLoader:
    @pytest.fixture
    def local_csv_loader(self) -> LocalCSVLoader:
        return LocalCSVLoader(in_memory=True)

    # noinspection DuplicatedCode
    def test_loader_workflow(self, dummy_df, local_csv_loader):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        local_csv_loader.save_dataframe(dummy_df, file_name="2024-01-01.csv")
        loaded_df = local_csv_loader.retrieve_dataframe(file_name="2024-01-01.csv")
        loaded_df = loaded_df.reset_index(drop=True)

        pd.testing.assert_frame_equal(loaded_df, dummy_df)

import collections
import contextlib
import datetime

import pandas as pd
import pytest

from data.extractors.opendap_extractor_L2_Standard import OpendapExtractorL2Standard
from data.utils.opendap import OpendapClient


class TestOpendapExtractorL2Standard:

    @pytest.fixture
    def dummy_client(self) -> OpendapClient:
        return DummyClient()

    def test_get_thredds_catalog_url_for_date(self, dummy_settings, dummy_client):
        date = datetime.date(2024, 3, 2)  # Day 62
        _e = OpendapExtractorL2Standard(dummy_settings, dummy_client)
        expected = "https://testbaseurl.com/opendap/OCO2_L2_Standard.11/2024/062/catalog.xml"

        url = _e.get_thredds_catalog_url_for_date(date)

        assert url == expected

    def test_get_dataframe_from_opendap_url(self, dummy_settings, dummy_client):
        url = "https://testbaseurl.com/file.nc4.nc4"
        _e = OpendapExtractorL2Standard(dummy_settings, dummy_client)

        df = _e.get_dataframe_from_opendap_url(url)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert set(df.columns.tolist()) == {"xco2", "_time", "longitude", "latitude"}

    def test_clean_dataframe(self):
        df = pd.DataFrame({
            "xco2": [0.0001, 0.0002, 0.0003],
            "RetrievalResults_outcome_flag": [0, 1, 0]
        })
        expected = pd.DataFrame({"xco2": [200.0]})

        clean_df = OpendapExtractorL2Standard.clean_dataframe(df)
        clean_df.reset_index(inplace=True, drop=True)  # Reset index to compare.

        pd.testing.assert_frame_equal(clean_df, expected)


class DummyClient(OpendapClient):

    def get_thredds_catalog_xml(self, *args, **kwargs):
        raise NotImplementedError  # Override to avoid network calls.

    @contextlib.contextmanager
    def get_file_from_opendap_url(self, *args, **kwargs):
        TempFile = collections.namedtuple("NamedTemporaryFile", ["name"])
        yield TempFile("tests/oco2_L2StdGL_test.h5.nc4")

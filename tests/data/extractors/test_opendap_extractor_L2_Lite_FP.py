import collections
import contextlib

import pandas as pd
import pytest

from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP
from data.utils.opendap import THREDDSCatalogError, OpendapClient


class TestOpendapExtractorL2LiteFP:

    @pytest.fixture
    def dummy_client(self) -> OpendapClient:
        return DummyClient()

    def test_get_thredds_catalog_url_for_year(self, dummy_settings, dummy_client):
        year = 2024
        _e = OpendapExtractorL2LiteFP(dummy_settings, dummy_client)
        expected = "https://testbaseurl.com/opendap/OCO2_L2_Lite_FP.11.1r/2024/catalog.xml"

        url = _e.get_thredds_catalog_url_for_year(year)

        assert url == expected

    def test_get_dataframe_from_opendap_url(self, dummy_settings, dummy_client):
        url = "https://testbaseurl.com/file.nc4.nc4"
        _e = OpendapExtractorL2LiteFP(dummy_settings, dummy_client)

        df = _e.get_dataframe_from_opendap_url(url)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert set(df.columns.tolist()) == {"xco2", "_time", "longitude", "latitude"}

    def test_get_opendap_urls_dict_for_year(self, dummy_settings, dummy_client):
        year = 2024
        _e = OpendapExtractorL2LiteFP(dummy_settings, dummy_client)
        expected = {
            "240101": "https://testbaseurl.com/opendap/hyrax/OCO2_L2_Lite_FP.11.1r/2024/oco2_LtCO2_240101_B11100Ar_240229181002s.nc4.nc4?xco2,time,longitude,latitude,xco2_quality_flag",
        }

        urls_dict = _e.get_opendap_urls_dict_for_year(year)

        assert urls_dict == expected

    def test_get_opendap_urls_dict_for_year__catalog_error(self, dummy_settings, dummy_client, caplog):
        year = 2024
        _e = OpendapExtractorL2LiteFP(dummy_settings, dummy_client)

        # noinspection PyUnusedLocal
        def raise_error(*args, **kwargs):
            raise THREDDSCatalogError("Catalog error")
        _e._client.get_thredds_catalog_xml = raise_error

        with pytest.raises(THREDDSCatalogError):
            _e.get_opendap_urls_dict_for_year(year)
        assert "Catalog error" in caplog.text

    def test_get_opendap_urls_dict_for_year__invalid_url(self, dummy_settings, dummy_client, caplog):
        year = 2024
        _e = OpendapExtractorL2LiteFP(dummy_settings, dummy_client)

        # noinspection PyUnusedLocal
        def raise_error(*args, **kwargs):
            raise ValueError("Invalid OPeNDAP URL")
        _e.opendap_url_to_date_str = raise_error

        with pytest.raises(ValueError):
            _e.get_opendap_urls_dict_for_year(year)
        assert "Invalid OPeNDAP URL" in caplog.text

    def test_opendap_url_to_date_str(self):
        url = "https://testbaseurl.com/opendap/OCO2_L2_Lite_FP.11.1r/2024/oco2_LtCO2_240102_B11100Ar_240229181145s.nc4"
        expected = "240102"

        date_str = OpendapExtractorL2LiteFP.opendap_url_to_date_str(url)

        assert date_str == expected

    def test_opendap_url_to_date_str__invalid_url(self):
        url = "2024/invalid.nc4"

        with pytest.raises(ValueError):
            OpendapExtractorL2LiteFP.opendap_url_to_date_str(url)

    def test_clean_dataframe(self):
        df = pd.DataFrame({"a": [1, 2, 3], "xco2_quality_flag": [0, 1, 0]})
        expected = pd.DataFrame({"a": [1, 3]})

        clean_df = OpendapExtractorL2LiteFP.clean_dataframe(df)
        clean_df.reset_index(inplace=True, drop=True)  # Reset index to compare.

        pd.testing.assert_frame_equal(clean_df, expected)


class DummyClient(OpendapClient):

    def get_thredds_catalog_xml(self, *args, **kwargs):
        return b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
    <thredds:dataset name="/OCO2_L2_Lite_FP.11.1r/2024" ID="/opendap/hyrax/OCO2_L2_Lite_FP.11.1r/2024/">
        <thredds:dataset name="oco2_LtCO2_240101_B11100Ar_240229181002s.nc4" ID="/opendap/hyrax/OCO2_L2_Lite_FP.11.1r/2024/oco2_LtCO2_240101_B11100Ar_240229181002s.nc4">
            <thredds:access serviceName="dap" urlPath="/OCO2_L2_Lite_FP.11.1r/2024/oco2_LtCO2_240101_B11100Ar_240229181002s.nc4"/>
        </thredds:dataset>
    </thredds:dataset>
</thredds:catalog>"""

    @contextlib.contextmanager
    def get_file_from_opendap_url(self, *args, **kwargs):
        TempFile = collections.namedtuple("NamedTemporaryFile", ["name"])
        yield TempFile("tests/oco2_LtCO2_test.nc4.nc4")

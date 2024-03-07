import datetime

from data.extractors.opendap_extractor_L2_Lite_FP import OpendapExtractorL2LiteFP


class TestOpendapExtractorL2LiteFP:

    def test_get_thredds_catalog_url_for_year(self, test_settings):
        year = 2024
        _e = OpendapExtractorL2LiteFP(test_settings)
        expected = "https://testbaseurl.com/opendap/OCO2_L2_Lite_FP.11.1r/2024/catalog.xml"

        url = _e.get_thredds_catalog_url_for_year(year)

        assert url == expected

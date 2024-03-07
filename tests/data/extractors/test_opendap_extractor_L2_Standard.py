import datetime

from data.extractors.opendap_extractor_L2_Standard import OpendapExtractorL2Standard


class TestOpendapExtractorL2Standard:

    def test_get_thredds_catalog_url_for_date(self, test_settings):
        date = datetime.date(2024, 3, 2)  # Day 62
        _e = OpendapExtractorL2Standard(test_settings)
        expected = "https://testbaseurl.com/opendap/OCO2_L2_Standard.11/2024/062/catalog.xml"

        url = _e.get_thredds_catalog_url_for_date(date)

        assert url == expected

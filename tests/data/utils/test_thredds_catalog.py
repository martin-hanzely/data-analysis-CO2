import datetime

import pytest
import requests

from data.utils.thredds_catalog import (
    THREDDSCatalogError,
    get_thredds_catalog_url_for_date,
    get_thredds_catalog_xml,
    get_opendap_urls,
)


class TestGetThreddsCatalogUrlForDate:

    def test_get_thredds_catalog_url_for_date(self):
        date = datetime.date(2024, 3, 2)
        expected = "https://oco2.gesdisc.eosdis.nasa.gov/opendap/OCO2_L2_Standard.11/2024/062/catalog.xml"
        url = get_thredds_catalog_url_for_date(date)
        assert url == expected


class TestGetThreddsCatalogXml:

    def test_get_thredds_catalog_xml(self, monkeypatch):
        # noinspection PyUnusedLocal
        def mock_requests_get(*args, **kwargs):
            class MockResponse:
                def raise_for_status(self):
                    pass

                @property
                def content(self):
                    return b'<?xml version="1.0" encoding="UTF-8"?>'

            return MockResponse()

        monkeypatch.setattr(requests, "get", mock_requests_get)
        url = "https://oco2.gesdisc.eosdis.nasa.gov/opendap/OCO2_L2_Standard.11/2024/062/catalog.xml"
        xml = get_thredds_catalog_xml(url)
        assert xml == b'<?xml version="1.0" encoding="UTF-8"?>'

    def test_get_thredds_catalog_xml__error_response(self, monkeypatch):
        # noinspection PyUnusedLocal
        def mock_requests_get(*args, **kwargs):
            class MockResponse:
                def raise_for_status(self):
                    raise requests.exceptions.HTTPError("HTTP Error")

                @property
                def content(self):
                    return b""

            return MockResponse()

        monkeypatch.setattr(requests, "get", mock_requests_get)
        with pytest.raises(THREDDSCatalogError) as e:
            get_thredds_catalog_xml("https://invalidurl.com")
        assert str(e.value) == "THREDDS catalog request https://invalidurl.com error HTTP Error"


class TestGetOpendapUrls:

    def test_get_opendap_urls(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
    <thredds:service name="file" serviceType="HTTPServer" base="/opendap/hyrax"/>
    <thredds:service name="WCS-coads" serviceType="WCS" base="/opendap/wcs"/>
    <thredds:dataset name="/OCO2_L2_Standard.11/2024/062" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/">
        <thredds:dataset name="oco2_L2StdND_51418a_240302_B11008_240303020002.h5" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5">
            <thredds:dataSize units="bytes">2906562</thredds:dataSize>
            <thredds:date type="modified">2024-03-03T05:32:25Z</thredds:date>
            <thredds:access serviceName="dap" urlPath="/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5"/>
        </thredds:dataset>
        <thredds:dataset name="oco2_L2StdND_51418a_240302_B11008_240303020002.h5.xml" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5.xml">
            <thredds:dataSize units="bytes">8200</thredds:dataSize>
            <thredds:date type="modified">2024-03-03T05:33:08Z</thredds:date>
            <thredds:access serviceName="file" urlPath="/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5.xml"/>
        </thredds:dataset>
        <thredds:dataset name="oco2_L2StdND_51420a_240302_B11008_240303021304.h5" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51420a_240302_B11008_240303021304.h5">
            <thredds:dataSize units="bytes">24313652</thredds:dataSize>
            <thredds:date type="modified">2024-03-03T05:32:24Z</thredds:date>
            <thredds:access serviceName="dap" urlPath="/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51420a_240302_B11008_240303021304.h5"/>
        </thredds:dataset>
        <thredds:dataset name="oco2_L2StdND_51420a_240302_B11008_240303021304.h5.xml" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51420a_240302_B11008_240303021304.h5.xml">
            <thredds:dataSize units="bytes">11429</thredds:dataSize>
            <thredds:date type="modified">2024-03-03T05:33:07Z</thredds:date>
            <thredds:access serviceName="file" urlPath="/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51420a_240302_B11008_240303021304.h5.xml"/>
        </thredds:dataset>
    </thredds:dataset>
</thredds:catalog>"""
        urls = list(get_opendap_urls(xml))
        assert urls == [
            "https://oco2.gesdisc.eosdis.nasa.gov/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5",
            "https://oco2.gesdisc.eosdis.nasa.gov/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51420a_240302_B11008_240303021304.h5",
        ]

    def test_get_opendap_urls__file_suffix(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
    <thredds:dataset name="/OCO2_L2_Standard.11/2024/062" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/">
        <thredds:dataset name="oco2_L2StdND_51418a_240302_B11008_240303020002.h5" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5">
            <thredds:access serviceName="dap" urlPath="/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5"/>
        </thredds:dataset>
        </thredds:dataset>
</thredds:catalog>"""
        urls = list(get_opendap_urls(xml, file_suffix=".nc4"))
        assert urls == [
            "https://oco2.gesdisc.eosdis.nasa.gov/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5.nc4",
        ]

    def test_get_opendap_urls__variables(self):
            xml = b"""\
    <thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
        <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
        <thredds:dataset name="/OCO2_L2_Standard.11/2024/062" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/">
            <thredds:dataset name="oco2_L2StdND_51418a_240302_B11008_240303020002.h5" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5">
                <thredds:access serviceName="dap" urlPath="/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5"/>
            </thredds:dataset>
            </thredds:dataset>
    </thredds:catalog>"""
            urls = list(get_opendap_urls(
                xml,
                variables=["RetrievalGeometry_retrieval_latitude", "RetrievalGeometry_retrieval_longitude"]
            ))
            assert urls == [
                "https://oco2.gesdisc.eosdis.nasa.gov/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5?RetrievalGeometry_retrieval_latitude,RetrievalGeometry_retrieval_longitude",
            ]

    def test_get_opendap_urls__invalid_xml(self):
        xml = b"invalid xml"
        with pytest.raises(THREDDSCatalogError) as e:
            list(get_opendap_urls(xml))
        assert str(e.value).startswith("THREDDS catalog parsing error")

    def test_get_opendap_urls__missing_dap_service(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="file" serviceType="HTTPServer" base="/opendap/hyrax"/>
    <thredds:service name="WCS-coads" serviceType="WCS" base="/opendap/wcs"/>
</thredds:catalog>"""
        with pytest.raises(THREDDSCatalogError) as e:
            list(get_opendap_urls(xml))
        assert str(e.value) == "OPeNDAP service not found in THREDDS catalog"

    def test_get_opendap_urls__missing_service_base(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP"/>
    <thredds:service name="file" serviceType="HTTPServer" base="/opendap/hyrax"/>
    <thredds:service name="WCS-coads" serviceType="WCS" base="/opendap/wcs"/>
</thredds:catalog>"""
        with pytest.raises(THREDDSCatalogError) as e:
            list(get_opendap_urls(xml))
        assert str(e.value) == "OPeNDAP service base not found in THREDDS catalog"

    def test_get_opendap_urls__missing_top_level_dataset(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
</thredds:catalog>"""
        with pytest.raises(THREDDSCatalogError) as e:
            list(get_opendap_urls(xml))
        assert str(e.value) == "THREDDS catalog top level dataset not found"

    def test_get_opendap_urls__missing_dataset_access(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
    <thredds:dataset name="/OCO2_L2_Standard.11/2024/062" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/">
        <thredds:dataset name="oco2_L2StdND_51418a_240302_B11008_240303020002.h5" ID="/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5">
            <thredds:dataSize units="bytes">2906562</thredds:dataSize>
            <thredds:date type="modified">2024-03-03T05:32:25Z</thredds:date>
            <!-- MISSING ACCESS -->
        </thredds:dataset>
    </thredds:dataset>
</thredds:catalog>"""
        urls = list(get_opendap_urls(xml))
        assert urls == []

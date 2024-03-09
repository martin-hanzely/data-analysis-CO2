import os

import pytest
import requests

from data.utils.opendap import THREDDSCatalogError, OpendapClient


class TestOpendapClient:
    _cl = OpendapClient()

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
        url = "https://validurl.com/opendap/OCO2_L2_Standard.11/2024/062/catalog.xml"
        xml = self._cl.get_thredds_catalog_xml(url)
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
            self._cl.get_thredds_catalog_xml("https://invalidurl.com")
        assert str(e.value) == "THREDDS catalog request https://invalidurl.com error HTTP Error"

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
        urls = list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com"))
        assert urls == [
            "https://someurl.com/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5",
            "https://someurl.com/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51420a_240302_B11008_240303021304.h5",
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
        urls = list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com", file_suffix=".nc4"))
        assert urls == [
            "https://someurl.com/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5.nc4",
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
            urls = list(self._cl.get_opendap_urls(
                xml,
                base_url="https://someurl.com",
                variables=["RetrievalGeometry_retrieval_latitude", "RetrievalGeometry_retrieval_longitude"]
            ))
            assert urls == [
                "https://someurl.com/opendap/hyrax/OCO2_L2_Standard.11/2024/062/oco2_L2StdND_51418a_240302_B11008_240303020002.h5?RetrievalGeometry_retrieval_latitude,RetrievalGeometry_retrieval_longitude",
            ]

    def test_get_opendap_urls__invalid_xml(self):
        xml = b"invalid xml"
        with pytest.raises(THREDDSCatalogError) as e:
            list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com"))
        assert str(e.value).startswith("THREDDS catalog parsing error")

    def test_get_opendap_urls__missing_dap_service(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="file" serviceType="HTTPServer" base="/opendap/hyrax"/>
    <thredds:service name="WCS-coads" serviceType="WCS" base="/opendap/wcs"/>
</thredds:catalog>"""
        with pytest.raises(THREDDSCatalogError) as e:
            list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com"))
        assert str(e.value) == "OPeNDAP service not found in THREDDS catalog"

    def test_get_opendap_urls__missing_service_base(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP"/>
    <thredds:service name="file" serviceType="HTTPServer" base="/opendap/hyrax"/>
    <thredds:service name="WCS-coads" serviceType="WCS" base="/opendap/wcs"/>
</thredds:catalog>"""
        with pytest.raises(THREDDSCatalogError) as e:
            list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com"))
        assert str(e.value) == "OPeNDAP service base not found in THREDDS catalog"

    def test_get_opendap_urls__missing_top_level_dataset(self):
        xml = b"""\
<thredds:catalog xmlns:thredds="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:bes="http://xml.opendap.org/ns/bes/1.0#">
    <thredds:service name="dap" serviceType="OPeNDAP" base="/opendap/hyrax"/>
</thredds:catalog>"""
        with pytest.raises(THREDDSCatalogError) as e:
            list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com"))
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
        urls = list(self._cl.get_opendap_urls(xml, base_url="https://someurl.com"))
        assert urls == []

    def test_get_file_from_opendap_url(self, monkeypatch):
        # noinspection PyUnusedLocal
        def mock_session_get(*args, **kwargs):
            class MockResponse:
                def raise_for_status(self):
                    pass

                @property
                def content(self):
                    return b"file content"

            return MockResponse()

        monkeypatch.setattr(requests.Session, "get", mock_session_get)
        url = "https://someurl.com/opendap/file.nc4"
        username = "username"
        password = "password"

        with (
            self._cl.get_file_from_opendap_url(url, username, password) as _f,
            open(_f.name, "rb") as open_file,
        ):
            assert open_file.read() == b"file content"

        assert _f.closed
        assert not os.path.exists(_f.name)

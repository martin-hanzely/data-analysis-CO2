from __future__ import annotations

from typing import TYPE_CHECKING
from xml.etree import ElementTree

import requests

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


class THREDDSCatalogError(Exception):
    """
    Exception raised for errors in THREDDS catalog processing.
    """


def get_thredds_catalog_xml(catalog_url: str) -> str | bytes:
    """
    Get THREDDS catalog XML from given URL.
    :param catalog_url: URL of THREDDS catalog.
    :return: THREDDS catalog XML.
    :raises THREDDSCatalogError: If the request to the THREDDS catalog URL fails.
    """
    response = requests.get(catalog_url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise THREDDSCatalogError(f"THREDDS catalog request {catalog_url} error {e}") from e

    return response.content


def get_opendap_urls(
        catalog_xml: str | bytes,
        base_url: str,
        file_suffix: str = "",
        variables: Iterable[str] | None = None,
) -> Iterator[str]:
    """
    Get list of OPeNDAP urls from given THREDDS catalog XML.
    https://docs.unidata.ucar.edu/tds/current/userguide/basic_client_catalog.html
    :param catalog_xml: THREDDS catalog XML.
    :param base_url: Base URL of the OPeNDAP server.
    :param file_suffix: Additional file suffix indicating dataset format.
    :param variables: List of requested variables.
    :return: Iterable of OPeNDAP urls.
    :raises THREDDSCatalogError: If the XML is not a valid THREDDS catalog with OPeNDAP service.
    """
    # noinspection HttpUrlsUsage
    xml_ns = {"thredds": "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"}
    service_name = "dap"

    try:
        catalog = ElementTree.fromstring(catalog_xml)
    except ElementTree.ParseError as e:
        raise THREDDSCatalogError(f"THREDDS catalog parsing error {e}") from e

    dap_service = catalog.find(f"thredds:service[@name='{service_name}']", xml_ns)
    if dap_service is None:
        raise THREDDSCatalogError("OPeNDAP service not found in THREDDS catalog")

    service_base = dap_service.attrib.get("base")  # /opendap/hyrax
    if service_base is None:
        raise THREDDSCatalogError("OPeNDAP service base not found in THREDDS catalog")

    top_level_dataset = catalog.find("thredds:dataset", xml_ns)
    if top_level_dataset is None:
        raise THREDDSCatalogError("THREDDS catalog top level dataset not found")

    if variables:  # Handle both empty list and None.
        variables_suffix = f'?{",".join(variables)}'
    else:
        variables_suffix = ""

    for dataset in top_level_dataset.findall("thredds:dataset", xml_ns):
        access = dataset.find(f"thredds:access[@serviceName='{service_name}']", xml_ns)
        if access is not None:
            url_path = access.attrib.get("urlPath")

            # TODO: Handle slashes in url components.
            yield f'{base_url}{service_base}{url_path}{file_suffix}{variables_suffix}'

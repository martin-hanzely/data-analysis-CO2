import datetime

from typing import Iterator, Iterable
from xml.etree import ElementTree

import requests


BASE_URL = "https://oco2.gesdisc.eosdis.nasa.gov"


class THREDDSCatalogError(Exception):
    """
    Exception raised for errors in THREDDS catalog processing.
    """


def get_thredds_catalog_url_for_date(date: datetime.date) -> str:
    """
    Get THREDDS catalog URL for given date from OCO2_L2_Standard.11
    directory on the NASA Earth Data GES DISC OPeNDAP server.
    https://oco2.gesdisc.eosdis.nasa.gov/opendap/OCO2_L2_Standard.11/contents.html
    :param date: Date for which to get THREDDS catalog URL.
    :return: THREDDS catalog URL.
    """
    home_dir = "opendap/OCO2_L2_Standard.11"
    year = date.year
    doy = date.timetuple().tm_yday
    return f"{BASE_URL}/{home_dir}/{year}/{doy:03}/catalog.xml"


def get_thredds_catalog_xml(catalog_url: str) -> str | bytes:
    """
    Get THREDDS catalog XML from given URL.
    :param catalog_url: URL of THREDDS catalog.
    :return: THREDDS catalog XML.
    """
    response = requests.get(catalog_url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise THREDDSCatalogError(f"THREDDS catalog request {catalog_url} error {e}") from e

    return response.content


def get_opendap_urls(
        catalog_xml: str | bytes,
        file_suffix: str = "",
        variables: Iterable[str] | None = None,
) -> Iterator[str]:
    """
    Get list of OPeNDAP urls from given THREDDS catalog XML.
    https://docs.unidata.ucar.edu/tds/current/userguide/basic_client_catalog.html
    :param catalog_xml: THREDDS catalog XML.
    :param file_suffix: File suffix indicating dataset format.
    :param variables: List of variables to filter datasets.
    :return: List of OPeNDAP urls.
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
        try:
            if dataset.attrib["name"].endswith(".h5"):
                access = dataset.find(f"thredds:access[@serviceName='{service_name}']", xml_ns)
                if access is not None:
                    yield f'{BASE_URL}{service_base}{access.attrib["urlPath"]}{file_suffix}{variables_suffix}'
        except KeyError:
            continue  # Skip dataset without access

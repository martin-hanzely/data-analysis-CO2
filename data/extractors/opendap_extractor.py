from __future__ import annotations

import datetime
import logging

import pandas as pd
from pydap.client import open_url

from data.extractors.base_extractor import BaseExtractor
from data.utils.thredds_catalog import (
    THREDDSCatalogError,
    get_thredds_catalog_url_for_date,
    get_thredds_catalog_xml,
    get_opendap_urls,
)


logger = logging.getLogger(__name__)


class OpendapExtractor(BaseExtractor):
    """
    Extractor class for data from the NASA Earth Data GES DISC OPeNDAP server.
    """
    def extract_for_date(self, date: datetime.date) -> pd.DataFrame:
        try:
            catalog_url = get_thredds_catalog_url_for_date(date)
            catalog_xml = get_thredds_catalog_xml(catalog_url)
            opendap_urls = get_opendap_urls(catalog_xml)
        except THREDDSCatalogError as e:
            logger.error(e)
            raise

        df = pd.DataFrame()
        for url in opendap_urls:
            dataset = open_url(url)
            pass

        return df

from __future__ import annotations

import datetime
import logging
import tempfile
import os
from typing import TYPE_CHECKING

# noinspection PyPep8Naming
import netCDF4 as nc
import pandas as pd
import requests

from data.extractors.base_extractor import BaseExtractor
from data.utils.thredds_catalog import (
    THREDDSCatalogError,
    get_thredds_catalog_xml,
    get_opendap_urls,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from data.settings import Settings


logger = logging.getLogger(__name__)


class OpendapExtractorL2Standard(BaseExtractor):
    """
    Extractor class for data from the NASA Earth Data GES DISC OPeNDAP server.
    """
    _settings: Settings

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def extract_date_range(
            self,
            date_range: Iterable[datetime.date]
    ) -> Iterator[pd.DataFrame]:
        for date in date_range:
            yield self.extract_date(date)

    def extract_date(self, date: datetime.date) -> pd.DataFrame:
        try:
            catalog_url = self.get_thredds_catalog_url_for_date(date)
            catalog_xml = get_thredds_catalog_xml(catalog_url)
            opendap_urls = list(get_opendap_urls(
                catalog_xml,
                file_suffix=".nc4",
                variables=[
                    "RetrievalGeometry_retrieval_latitude",
                    "RetrievalGeometry_retrieval_longitude",
                    "RetrievalHeader_retrieval_time_string",
                    "RetrievalResults_xco2",
                    "PreprocessingResults_fluorescence_qual_flag_idp",
                ]
            ))
        except THREDDSCatalogError as e:
            logger.error(e)
            raise

        df = pd.DataFrame()
        for url in opendap_urls:
            df = df.append(self.get_dataframe_from_opendap_url(url), ignore_index=True)
        return df

    def get_thredds_catalog_url_for_date(self, date: datetime.date) -> str:
        home_dir = "opendap/OCO2_L2_Standard.11"
        year = date.year
        doy = date.timetuple().tm_yday
        return f"{self._settings.earthdata_base_url}/{home_dir}/{year}/{doy:03}/catalog.xml"

    def get_dataframe_from_opendap_url(self, url: str) -> pd.DataFrame:
        # Named temporary file is required here because netCDF4 cannot read from a file-like object.
        _f = tempfile.NamedTemporaryFile(delete=False)
        with requests.Session() as session:
            session.auth = (self._settings.earthdata_username, self._settings.earthdata_password)
            response = session.get(url)
            _f.write(response.content)
            _f.close()

        with nc.Dataset(_f.name, mode="r") as ds:
            retrieval_time_string = nc.chartostring(ds["RetrievalHeader_retrieval_time_string"][:])
            df = pd.DataFrame({
                "retrieval_datetime": pd.to_datetime(retrieval_time_string, format="%Y-%m-%dT%H:%M:%S.%fZ"),
                "retrieval_latitude": ds["RetrievalGeometry_retrieval_latitude"][:],
                "retrieval_longitude": ds["RetrievalGeometry_retrieval_longitude"][:],
                "xco2": ds["RetrievalResults_xco2"][:],
                "fluorescence_qual_flag_idp": ds["PreprocessingResults_fluorescence_qual_flag_idp"][:],
            })

        os.unlink(_f.name)  # Delete temporary file.
        return df[df["fluorescence_qual_flag_idp"] == 0]  # Keep only fluorescence_qual_flag_idp = 0

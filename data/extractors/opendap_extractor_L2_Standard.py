from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

# noinspection PyPep8Naming
import netCDF4 as nc
import pandas as pd

from data.extractors.base_opendap_extractor import BaseOpendapExtractor
from data.utils.opendap import THREDDSCatalogError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


logger = logging.getLogger(__name__)


class OpendapExtractorL2Standard(BaseOpendapExtractor):
    """
    Extractor class for data from the NASA Earth Data GES DISC OPeNDAP server.
    """
    def extract_date_range(
            self,
            date_range: Iterable[datetime.date]
    ) -> Iterator[pd.DataFrame]:
        for date in date_range:
            yield self.extract_date(date)  # TODO: `continue` / `raise` on error?

    def extract_date(self, date: datetime.date) -> pd.DataFrame:
        try:
            catalog_url = self.get_thredds_catalog_url_for_date(date)
            catalog_xml = self._client.get_thredds_catalog_xml(catalog_url)
            opendap_urls = list(self._client.get_opendap_urls(
                catalog_xml,
                base_url=self._settings.earthdata_base_url,
                file_suffix=".nc4",
                variables=[
                    "RetrievalGeometry_retrieval_latitude",
                    "RetrievalGeometry_retrieval_longitude",
                    "RetrievalHeader_retrieval_time_string",
                    "RetrievalResults_xco2",
                    "RetrievalResults_outcome_flag",
                ]
            ))
        except THREDDSCatalogError as e:
            logger.error(e)
            raise

        df = pd.DataFrame()
        for url in opendap_urls:
            df = pd.concat([df, self.get_dataframe_from_opendap_url(url)], ignore_index=True)
        return df

    def get_thredds_catalog_url_for_date(self, date: datetime.date) -> str:
        home_dir = "opendap/OCO2_L2_Standard.11"
        year = date.year
        doy = date.timetuple().tm_yday
        return f"{self._settings.earthdata_base_url}/{home_dir}/{year}/{doy:03}/catalog.xml"

    def get_dataframe_from_opendap_url(self, url: str) -> pd.DataFrame:
        with (
            self._client.get_file_from_opendap_url(
                url,
                self._settings.earthdata_username,
                self._settings.earthdata_password
            ) as _f,
            nc.Dataset(_f.name, mode="r") as ds
        ):
            retrieval_time_string = nc.chartostring(ds["RetrievalHeader_retrieval_time_string"][:])
            df = pd.DataFrame({
                "_time": pd.to_datetime(retrieval_time_string, format="%Y-%m-%dT%H:%M:%S.%fZ", utc=True),
                "latitude": ds["RetrievalGeometry_retrieval_latitude"][:],
                "longitude": ds["RetrievalGeometry_retrieval_longitude"][:],
                "xco2": ds["RetrievalResults_xco2"][:],
                "RetrievalResults_outcome_flag": ds["RetrievalResults_outcome_flag"][:],
            })
            return self.clean_dataframe(df)

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        # Indicator of retrieval results:
        # 1 - Passed internal quality check,
        # 2 - Failed internal quality check,
        # 3 - Reached maximum allowed iterations,
        # 4 - Reached maximum allowed divergences.
        df = df \
            .loc[df["RetrievalResults_outcome_flag"] == 1] \
            .drop(columns=["RetrievalResults_outcome_flag"])

        # Dry air mole fraction to ppm.
        df["xco2"] = df["xco2"] * 1e6
        return df

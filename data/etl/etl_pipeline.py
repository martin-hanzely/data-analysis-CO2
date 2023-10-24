from __future__ import annotations

import os
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

import pandas as pd
import requests
from netCDF4 import Dataset

if TYPE_CHECKING:
    from data.loaders.base_loader import BaseLoader


class ETLPipeline:
    """
    ETL pipeline class.
    """
    # Loaders injected as dependency allow for easy testing, local and remote execution.
    _load_strategy: BaseLoader

    def __init__(self, load_strategy: BaseLoader) -> None:
        self._load_strategy = load_strategy

    def invoke(self) -> None:
        """
        Invoke ETL pipeline.
        :return: None
        """
        df = self._extract()
        df = self._transform(df)
        self._load(df)

    def _extract(self) -> pd.DataFrame:
        """
        Extract data from external sources.
        :return: Extracted data as dataframe
        """
        # Create temporary file to store data.
        file_handle = NamedTemporaryFile(delete=False)

        # Download data from NOAA.
        # See: https://gml.noaa.gov/aftp/data/trace_gases/co2/flask/surface/README_co2_surface-flask_ccgg.html
        # TODO: Move URL to config file.
        url = "https://gml.noaa.gov/aftp/data/trace_gases/co2/flask/surface/nc/co2_mlo_surface-flask_1_ccgg_event.nc"
        with requests.get(url, stream=True, timeout=30) as _r:
            _r.raise_for_status()
            for chunk in _r.iter_content(chunk_size=None):
                file_handle.write(chunk)
        # SOURCE:
        # Lan, X., J. W. Mund, A. M. Crotwell, M. J. Crotwell, E. Moglia,
        # M. Madronich, D. Neff and K. W. Thoning (2023), Atmospheric Carbon Dioxide Dry
        # Air Mole Fractions from the NOAA GML Carbon Cycle Cooperative Global
        # Air Sampling Network, 1968-2022, Version: 2023-08-28, https://doi.org/10.15138/wkgj-f215

        # Open file handle and read data.
        root_group = Dataset(file_handle.name, "r")
        _tc = root_group.variables["time_components"]
        _v = root_group.variables["value"]
        _qc = root_group.variables["qcflag"]

        # Create dataframe.
        df = pd.DataFrame({
            "value": _v[:],
            "year": _tc[:, 0],
            "qcflag": _qc[:, 0],
        })

        # Close root group.
        root_group.close()

        # Close file handle and delete file.
        file_handle.close()
        os.unlink(file_handle.name)

        return df

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data.
        :param df: Dataframe to transform
        :return: Transformed dataframe
        """
        # Filter out rows with invalid qcflag.
        df = df[df["qcflag"] == b"."]

        # Drop datetime column.
        df = df.drop(columns=["qcflag"])

        # Group by date and calculate mean.
        df = df.groupby("year").mean().reset_index()

        return df

    def _load(self, df: pd.DataFrame) -> None:
        """
        Save dataframe to persistent storage.
        :param df: Dataframe to save
        :return: None
        """
        self._load_strategy.save_dataframe(df)

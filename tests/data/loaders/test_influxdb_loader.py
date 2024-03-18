import pandas as pd
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from data.loaders.influxdb_loader import InfluxDBLoader


@pytest.mark.integration
class TestInfluxDBLoader:

    @pytest.fixture(scope="module")
    def influxdb_container(self, dummy_settings) -> DockerContainer:  # Do not use `Iterator` here!
        container = DockerContainer("influxdb:2.7.4-alpine") \
            .with_env("DOCKER_INFLUXDB_INIT_MODE", "setup") \
            .with_env("DOCKER_INFLUXDB_INIT_USERNAME", "dummy-user") \
            .with_env("DOCKER_INFLUXDB_INIT_PASSWORD", "dummy-password") \
            .with_env("DOCKER_INFLUXDB_INIT_ORG", dummy_settings.influxdb_org) \
            .with_env("DOCKER_INFLUXDB_INIT_BUCKET", dummy_settings.influxdb_bucket) \
            .with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", dummy_settings.influxdb_token) \
            .with_env("INFLUXD_UI_DISABLED", "true") \
            .with_bind_ports(8086, 8086)

        with container:
            wait_for_logs(container, "Listening", timeout=10)
            yield container

    # noinspection DuplicatedCode
    def test_loader_workflow(self, influxdb_container, dummy_settings, dummy_df):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        expected_df = pd.DataFrame({
            "_time": pd.to_datetime(["2024-01-01T02:01"], utc=True),
            "latitude": [0.0],
            "longitude": [0.0],
            "xco2": [420.0],
        })

        loader = InfluxDBLoader(settings=dummy_settings)
        loader.save_dataframe(df=dummy_df)
        loaded_df = loader.retrieve_dataframe(
            dt_from=pd.to_datetime("2024-01-01T01:30", utc=True),
            dt_to=pd.to_datetime("2024-01-01T02:30", utc=True),
        )
        loaded_df = loaded_df.reset_index(drop=True)

        pd.testing.assert_frame_equal(loaded_df, expected_df, check_like=True)

    def test_retrieve_dataframe__works_with_empty_result(self, influxdb_container, dummy_settings, dummy_df):
        assert isinstance(dummy_df, pd.DataFrame) and len(dummy_df) > 0  # Sanity check

        loader = InfluxDBLoader(settings=dummy_settings)
        loader.save_dataframe(df=dummy_df)
        loaded_df = loader.retrieve_dataframe(
            dt_from=pd.to_datetime("2020-01-01T00:00", utc=True),
            dt_to=pd.to_datetime("2020-01-01T01:00", utc=True),
        )
        loaded_df = loaded_df.reset_index(drop=True)

        assert loaded_df.empty

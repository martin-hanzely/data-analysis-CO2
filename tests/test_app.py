import dash
import pytest

from data import conf
from data.loaders.dummy_loader import DummyLoader


def test_update_graph(dummy_df, caplog, dummy_settings, monkeypatch):
    monkeypatch.setattr(conf, "get_app_settings", lambda: dummy_settings)

    from app import update_graph

    loader = DummyLoader()
    loader.save_dataframe(dummy_df)
    start_date = "2024-01-01"
    end_date = "2024-01-02"

    figure, results_count = update_graph(start_date, end_date, loader)

    assert figure is not None
    assert results_count == 3


def test_update_graph__invalid_date_range(caplog, dummy_settings, monkeypatch):
    monkeypatch.setattr(conf, "get_app_settings", lambda: dummy_settings)

    from app import update_graph

    loader = DummyLoader()
    start_date = "2024-01-01"
    end_date = "invalid-date"

    with pytest.raises(dash.exceptions.PreventUpdate):
        update_graph(start_date, end_date, loader)

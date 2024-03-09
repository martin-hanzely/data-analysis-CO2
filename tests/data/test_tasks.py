from data.tasks import debug_task


def test_debug_task(caplog):
    assert debug_task() == 0
    assert "Debug task executed" in caplog.text

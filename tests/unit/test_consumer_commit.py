from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from polyhorizon.ingestion.finnhub_consumer.consumer import FinnhubConsumer


def consumer():
    item = object.__new__(FinnhubConsumer)
    item._consumer = Mock()
    item._message_handlers = []
    item._batch_handlers = []
    item._error_handlers = []
    item._symbols_seen = set()
    item._partition_offsets = {}
    item._messages_consumed = 0
    item._messages_processed = 0
    item._messages_failed = 0
    item._consecutive_errors = 0
    item._last_message_time = 0
    item.logger = Mock()
    item.config = SimpleNamespace(max_consecutive_errors=10, error_backoff_seconds=0)
    return item


def message(price=100):
    return SimpleNamespace(value={"symbol": "NVDA", "price": price}, key="NVDA", partition=0, offset=1)


def test_success_commits_after_handler():
    item = consumer()
    item.add_message_handler(lambda _: item._consumer.commit.assert_not_called())
    item._process_message(message())
    item._consumer.commit.assert_called_once()
    assert item._messages_processed == 1


def test_failed_handler_does_not_commit():
    item = consumer()
    def fail(_):
        raise RuntimeError("downstream failed")
    item.add_message_handler(fail)
    with pytest.raises(RuntimeError, match="downstream failed"):
        item._process_message(message())
    item._consumer.commit.assert_not_called()
    assert item._messages_processed == 0
    assert item._messages_failed == 1


def test_invalid_trade_does_not_commit():
    item = consumer()
    with pytest.raises(ValueError, match="Invalid trade"):
        item._process_message(message(price="invalid"))
    item._consumer.commit.assert_not_called()


def test_batch_failure_does_not_commit():
    item = consumer()
    def fail(_):
        raise RuntimeError("batch failed")
    item.add_batch_handler(fail)
    with pytest.raises(RuntimeError, match="batch failed"):
        item._process_message_batch([message()])
    item._consumer.commit.assert_not_called()
    assert item._messages_processed == 0

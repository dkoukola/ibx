"""Offline fresh execution-query callback ownership and failure semantics."""
from ibx import EClient, EWrapper


class Reports(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.ends = []
        self.errors = []

    def exec_details(self, req_id, contract, execution):
        self.rows.append((req_id, contract.symbol, execution.shares))

    def exec_details_end(self, req_id):
        self.ends.append(req_id)

    def error(self, req_id, code, message, advanced=""):
        self.errors.append((req_id, code))


def stage(client, reports):
    for order_id in (10, 11):
        client._test_track_order(order_id, 0, "AAPL", "BUY", 1.0, 100.0)
        client._test_push_fill(0, order_id, "BUY", 100.0, 1, 0)
    client._test_dispatch_once()
    reports.rows.clear()


def test_execution_range_is_not_answered_from_cache():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    stage(client, reports)
    client.req_executions_range(7, "20261003-00:00:00", "20261005-12:00:00")
    client._test_dispatch_once()
    assert reports.rows == []
    assert reports.ends == []
    client._test_publish_execution_range(7, None)
    client._test_dispatch_once()
    assert reports.rows == [(7, "AAPL", 1.0)] * 2
    assert reports.ends == [7]
    client.disconnect()


def test_execution_range_failure_and_invalid_interval_never_end():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client.req_executions_range(7, "20260230-00:00:00", "20261005-12:00:00")
    assert reports.errors == [(7, 321)]
    client._test_publish_execution_range(8, "query timeout")
    client._test_dispatch_once()
    assert reports.errors[-1] == (8, 10159)
    assert reports.ends == []
    client.disconnect()


def test_execution_range_callback_replacement_stops_old_rows_and_end():
    class Replacing(Reports):
        def exec_details(self, req_id, contract, execution):
            super().exec_details(req_id, contract, execution)
            if req_id == 7:
                client.disconnect()
                client._test_connect()

    reports = Replacing()
    client = EClient(reports)
    client._test_connect()
    stage(client, reports)
    client._test_publish_execution_range(7, None)
    client._test_dispatch_once()
    assert len(reports.rows) == 1
    assert reports.ends == []
    assert reports.errors[-1] == (7, 10159)
    client.disconnect()

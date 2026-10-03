"""Offline execution-history completion and Python callback ownership."""

from types import SimpleNamespace

from ibx import EClient, EWrapper


class Reports(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.ends = []

    def exec_details(self, req_id, contract, execution):
        self.rows.append((req_id, contract.symbol, execution.exec_id))

    def exec_details_end(self, req_id):
        self.ends.append(req_id)


def queue_fill(client, order_id, symbol):
    client._test_track_order(order_id, 0, symbol, "BUY", 1.0, 100.0)
    client._test_push_fill(0, order_id, "BUY", 100.0, 1, 0)


def test_execution_history_waits_for_exact_end_and_drains_rows_first():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client._test_begin_execution_history("history-1")
    selected = SimpleNamespace(symbol="AAA")
    client.req_executions(7, selected)
    selected.symbol = "BBB"  # The request owns the filter as submitted.
    client.req_executions(8)
    client._test_complete_execution_history("not-history-1")
    client._test_dispatch_once()
    assert reports.ends == []
    queue_fill(client, 10, "AAA")
    queue_fill(client, 11, "BBB")
    client._test_complete_execution_history("history-1")
    client._test_dispatch_once()
    assert [(rid, symbol) for rid, symbol, _ in reports.rows if rid >= 0] == [
        (7, "AAA"), (8, "AAA"), (8, "BBB")
    ]
    assert reports.ends == [7, 8]
    client._test_dispatch_once()
    assert reports.ends == [7, 8]
    client.disconnect()


def test_execution_history_requeues_after_callback_starts_new_history():
    class Reconnecting(Reports):
        def exec_details(self, req_id, contract, execution):
            super().exec_details(req_id, contract, execution)
            if req_id == 7 and not hasattr(self, "changed"):
                self.changed = True
                client._test_begin_execution_history("history-2")

    reports = Reconnecting()
    client = EClient(reports)
    client._test_connect()
    queue_fill(client, 10, "AAA")
    client.req_executions(7)
    client._test_dispatch_once()
    first = [row for row in reports.rows if row[0] == 7]
    assert len(first) == 1
    assert reports.ends == []
    client._test_dispatch_once()
    assert [row for row in reports.rows if row[0] == 7] == first
    client._test_complete_execution_history("history-2")
    client._test_dispatch_once()
    assert [row for row in reports.rows if row[0] == 7] == first * 2
    assert reports.ends == [7]
    client.disconnect()


def test_execution_history_does_not_leak_requests_into_reconnected_client():
    class Replaced(Reports):
        def exec_details(self, req_id, contract, execution):
            super().exec_details(req_id, contract, execution)
            if req_id == 7:
                client.disconnect()
                client._test_connect()

    reports = Replaced()
    client = EClient(reports)
    client._test_connect()
    queue_fill(client, 10, "AAA")
    client.req_executions(7)
    client.req_executions(8)
    client._test_dispatch_once()
    assert [row[0] for row in reports.rows if row[0] >= 0] == [7]
    assert reports.ends == []
    client.req_executions(9)
    client._test_dispatch_once()
    assert reports.ends == [9]
    client.disconnect()


def test_execution_history_engine_stop_cannot_publish_cached_end():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client.req_executions(7)
    client._test_push_disconnect_event()
    client._test_dispatch_once()
    assert reports.rows == []
    assert reports.ends == []
    assert not client.is_connected()
    client.disconnect()

"""Offline fresh-history dispatch, failure and callback ownership."""

from ibx import EClient, EWrapper


class Reports(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.ends = 0
        self.errors = []

    def completed_order(self, contract, order, state):
        self.rows.append((contract.symbol, order.order_id, state.completed_status))

    def completed_orders_end(self):
        self.ends += 1

    def error(self, req_id, code, message, advanced=""):
        self.errors.append((code, message))


def stage(client, order_id):
    client._test_push_completed_order(
        order_id, 0, "Cancelled", 0, "AAPL", "BUY", 1.0, 1.0,
        "Cancelled", "20261003-12:00:00", "USD", "", 0.0,
    )


def test_local_archive_is_not_a_fresh_completed_answer():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    stage(client, 1)
    client.req_completed_orders()
    client._test_dispatch_once()
    assert reports.rows == []
    assert reports.ends == 0
    client._test_publish_completed_history(None)
    client._test_dispatch_once()
    assert reports.rows == [("AAPL", 1, "Cancelled")]
    assert reports.ends == 1
    client.disconnect()


def test_failed_query_and_changed_connection_never_publish_end():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client.req_completed_orders()
    client._test_publish_completed_history("history timeout")
    client._test_dispatch_once()
    assert reports.errors[-1] == (10159, "history timeout")
    client._test_publish_completed_history(None)
    client._test_begin_execution_history("new-connection")
    client._test_complete_execution_history("new-connection")
    client._test_dispatch_once()
    assert reports.ends == 0
    client.disconnect()


def test_callback_reconnect_stops_old_rows_and_end():
    class Reconnecting(Reports):
        def completed_order(self, contract, order, state):
            super().completed_order(contract, order, state)
            client._test_begin_execution_history("new-connection")
            client._test_complete_execution_history("new-connection")

    reports = Reconnecting()
    client = EClient(reports)
    client._test_connect()
    stage(client, 1)
    stage(client, 2)
    client._test_publish_completed_history(None)
    client._test_dispatch_once()
    assert len(reports.rows) == 1
    assert reports.ends == 0
    client.disconnect()


def test_callback_replacement_cannot_leak_old_rows_into_new_client():
    class Replacing(Reports):
        def completed_order(self, contract, order, state):
            super().completed_order(contract, order, state)
            client.disconnect()
            client._test_connect()

    reports = Replacing()
    client = EClient(reports)
    client._test_connect()
    stage(client, 1)
    stage(client, 2)
    client._test_publish_completed_history(None)
    client._test_dispatch_once()
    assert len(reports.rows) == 1
    assert reports.ends == 0
    client.disconnect()


def test_bad_utc_range_fails_before_query_without_end():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client.req_completed_orders_range("20260230-00:00:00", "20261005-00:00:00")
    assert reports.errors[-1][0] == 321
    assert reports.ends == 0
    client.disconnect()

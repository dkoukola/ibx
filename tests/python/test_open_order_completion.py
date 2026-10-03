"""Offline open-order completion, queued statuses, and callback ownership."""

from ibx import EClient, EWrapper


class Orders(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.ends = 0

    def open_order(self, order_id, contract, order, state):
        self.rows.append((order_id, state.status))

    def open_order_end(self):
        self.ends += 1


def connected(reports):
    client = EClient(reports)
    client._test_connect()
    client._test_track_order(7, 0, "AAA", "BUY", 1.0, 100.0)
    return client


def test_open_order_reply_drains_terminal_status_even_after_wire_end():
    reports = Orders()
    client = connected(reports)
    client._test_push_order_update(7, 0, "Cancelled", 0, 1)
    client._test_set_open_orders_held(False)
    client.req_open_orders()
    assert reports.rows == [] and reports.ends == 0
    client._test_dispatch_once()
    assert reports.rows == [] and reports.ends == 1
    client.disconnect()


def test_open_order_reply_rejects_callback_aba_without_waiting_for_u72_end():
    class Reconnecting(Orders):
        def open_order(self, *args):
            super().open_order(*args)
            if len(self.rows) == 1:
                client._test_set_open_orders_held(True)
                client._test_begin_execution_history("next-link")
                client._test_set_open_orders_held(False)

    reports = Reconnecting()
    client = connected(reports)
    client.req_open_orders()
    client._test_dispatch_once()
    assert len(reports.rows) == 1 and reports.ends == 0
    client._test_dispatch_once()
    assert reports.rows[0] == reports.rows[1]
    assert reports.ends == 1  # No execution-history end was supplied.
    client.disconnect()


def test_open_order_reply_rechecks_identity_after_status_callbacks():
    class Reconnecting(Orders):
        def order_status(self, *args):
            if not hasattr(self, "changed"):
                self.changed = True
                client._test_set_open_orders_held(True)
                client._test_begin_execution_history("next-link")
                client._test_set_open_orders_held(False)

    reports = Reconnecting()
    client = connected(reports)
    client._test_push_order_update(7, 0, "Submitted", 0, 1)
    client.req_open_orders()
    client._test_dispatch_once()
    assert len(reports.rows) == 1  # Status notification only, no stale snapshot.
    assert reports.ends == 0
    client._test_dispatch_once()
    assert reports.ends == 1
    client.disconnect()


def test_open_order_old_requests_cannot_leak_into_replaced_python_client():
    class Replaced(Orders):
        def open_order(self, *args):
            super().open_order(*args)
            client.disconnect()
            client._test_connect()

    reports = Replaced()
    client = connected(reports)
    client.req_open_orders()
    client.req_all_open_orders()
    client._test_dispatch_once()
    assert len(reports.rows) == 1 and reports.ends == 0
    client._test_dispatch_once()
    assert reports.ends == 0
    client.req_open_orders()
    client._test_dispatch_once()
    assert reports.ends == 1
    client.disconnect()


def test_open_order_queued_requests_do_not_survive_engine_stop():
    reports = Orders()
    client = connected(reports)
    client.req_open_orders()
    client._test_push_disconnect_event()
    client._test_dispatch_once()
    assert reports.rows == [] and reports.ends == 0
    assert not client.is_connected()
    client.disconnect()

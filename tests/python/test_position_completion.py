"""Offline position snapshot callback ownership and reconnect coverage."""

from ibx import EClient, EWrapper


class Positions(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.ends = []

    def position(self, account, contract, quantity, cost):
        self.rows.append((-1, contract.con_id, quantity))

    def position_end(self):
        self.ends.append(-1)

    def position_multi(self, request, account, model, contract, quantity, cost):
        self.rows.append((request, contract.con_id, quantity))

    def position_multi_end(self, request):
        self.ends.append(request)


def connected(reports):
    client = EClient(reports)
    client._test_connect()
    client._test_set_position(1, 1, 100)
    client._test_set_position(2, 1, 100)
    client._test_account_download_complete()
    return client


def test_position_snapshot_reconnect_stops_old_rows_and_end():
    class Reconnecting(Positions):
        def position(self, *args):
            super().position(*args)
            if len(self.rows) == 1:
                client._test_invalidate_position_snapshot()
                client._test_begin_execution_history("next-link")
                client._test_set_position(1, 2, 100)
                client._test_account_download_complete()

    reports = Reconnecting()
    client = connected(reports)
    client.req_positions()
    assert reports.rows == [(-1, 1, 1)]
    assert reports.ends == []
    client._test_dispatch_once()
    assert reports.rows == [(-1, 1, 1), (-1, 1, 2), (-1, 2, 1)]
    assert reports.ends == [-1]  # No U72 execution end was supplied.
    client._test_dispatch_once()
    assert reports.ends == [-1]
    client.disconnect()


def test_position_multi_reconnect_replays_all_prepared_subscriptions():
    class Reconnecting(Positions):
        def position_multi(self, *args):
            super().position_multi(*args)
            if len(self.rows) == 1:
                client._test_invalidate_position_snapshot()
                client._test_begin_execution_history("next-link")
                client._test_account_download_complete()

    reports = Reconnecting()
    client = connected(reports)
    client._test_invalidate_position_snapshot()
    client.req_positions_multi(1, "", "")
    client.req_positions_multi(2, "", "")
    client._test_account_download_complete()
    client._test_dispatch_once()
    assert reports.rows == [(1, 1, 1)]
    assert reports.ends == []
    client._test_dispatch_once()
    assert reports.rows == [(1, 1, 1), (1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)]
    assert reports.ends == [1, 2]
    client.disconnect()


def test_position_snapshot_does_not_leak_into_replaced_python_client():
    class Replaced(Positions):
        def position(self, *args):
            super().position(*args)
            if len(self.rows) == 1:
                client.disconnect()
                client._test_connect()
                client._test_set_position(9, 3, 100)
                client._test_account_download_complete()

    reports = Replaced()
    client = connected(reports)
    client.req_positions()
    assert reports.rows == [(-1, 1, 1)]
    assert reports.ends == []
    client._test_dispatch_once()
    assert reports.rows == [(-1, 1, 1)] and reports.ends == []
    client.req_positions()
    assert reports.rows[-1] == (-1, 9, 3)
    assert reports.ends == [-1]
    client.disconnect()


def test_position_subscriptions_wait_until_new_image_after_loss():
    reports = Positions()
    client = connected(reports)
    client.req_positions()
    assert reports.ends == [-1]
    client._test_invalidate_position_snapshot()
    client._test_begin_execution_history("next-link")
    client._test_set_position(1, 3, 100)
    client._test_dispatch_once()
    assert len(reports.rows) == 2 and reports.ends == [-1]
    client._test_account_download_complete()
    client._test_dispatch_once()
    assert reports.rows[-2:] == [(-1, 1, 3), (-1, 2, 1)]
    assert reports.ends == [-1, -1]
    client._test_push_disconnect_event()
    client._test_dispatch_once()
    assert not client.is_connected()
    assert reports.ends == [-1, -1]
    client.disconnect()


def test_old_dispatch_cannot_advance_replacement_client_subscription():
    class Replaced(Positions):
        def position(self, *args):
            super().position(*args)
            if len(self.rows) == 1:
                client.disconnect()
                client._test_connect()
                client._test_set_position(9, 3, 100)
                client.req_positions_multi(9, "", "")

    reports = Replaced()
    client = connected(reports)
    client._test_invalidate_position_snapshot()
    client.req_positions()
    client._test_account_download_complete()
    client._test_dispatch_once()
    assert reports.rows == [(-1, 1, 1)] and reports.ends == []
    client._test_account_download_complete()
    client._test_dispatch_once()
    assert reports.rows == [(-1, 1, 1), (9, 9, 3)]
    assert reports.ends == [9]
    client.disconnect()

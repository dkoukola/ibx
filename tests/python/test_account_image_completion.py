"""Offline account-image identity, reconnect, and callback ownership."""

from ibx import EClient, EWrapper


class Account(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.ends = []

    def update_account_value(self, key, value, currency, account):
        self.rows.append((-1, key, value))

    def account_download_end(self, account):
        self.ends.append(-1)

    def account_update_multi(self, request, account, model, key, value, currency):
        self.rows.append((request, key, value))

    def account_update_multi_end(self, request):
        self.ends.append(request)


def connected(reports):
    client = EClient(reports)
    client._test_connect()
    client._test_set_account_row("First", "1")
    client._test_set_account_row("Second", "2")
    return client


def test_account_image_waits_after_loss_then_sends_one_fresh_image():
    reports = Account()
    client = connected(reports)
    client.req_account_updates(True, "")
    client._test_dispatch_once()
    assert reports.ends == [-1]
    client._test_invalidate_account_image()
    client._test_dispatch_once()
    client._test_begin_account_image("AR.5")
    client._test_dispatch_once()
    assert len(reports.rows) == 2 and reports.ends == [-1]
    client._test_set_account_row("Replacement", "3")
    client._test_dispatch_once()
    assert reports.rows[-1] == (-1, "Replacement", "3")
    assert reports.ends == [-1, -1]
    client._test_dispatch_once()
    assert len(reports.rows) == 3 and reports.ends == [-1, -1]
    client.disconnect()


def test_account_callback_reconnect_never_finishes_old_image():
    class Reconnect(Account):
        def update_account_value(self, *args):
            super().update_account_value(*args)
            if len(self.rows) == 1:
                client._test_begin_account_image("AR.5")
                client._test_set_account_row("Replacement", "3")

    reports = Reconnect()
    client = connected(reports)
    client.req_account_updates(True, "")
    client._test_dispatch_once()
    assert reports.rows == [(-1, "First", "1")] and reports.ends == []
    client._test_dispatch_once()
    assert reports.rows[-1] == (-1, "Replacement", "3")
    assert reports.ends == [-1]
    client.disconnect()


def test_account_callback_replaced_client_cannot_receive_old_end():
    class Replaced(Account):
        def update_account_value(self, *args):
            super().update_account_value(*args)
            if len(self.rows) == 1:
                client.disconnect()
                client._test_connect()
                client._test_set_account_row("NewClient", "4")

    reports = Replaced()
    client = connected(reports)
    client.req_account_updates(True, "")
    client._test_dispatch_once()
    assert reports.rows == [(-1, "First", "1")] and reports.ends == []
    client.req_account_updates(True, "")
    client._test_dispatch_once()
    assert reports.rows[-1] == (-1, "NewClient", "4")
    assert reports.ends == [-1]
    client.disconnect()


def test_account_multi_reconnect_replays_every_prepared_subscription():
    class Reconnect(Account):
        def account_update_multi(self, *args):
            super().account_update_multi(*args)
            if len(self.rows) == 1:
                client._test_begin_account_image("AR.5")
                client._test_set_account_row("Replacement", "3")

    reports = Reconnect()
    client = connected(reports)
    client._test_invalidate_account_image()
    client.req_account_updates_multi(1, "", "")
    client.req_account_updates_multi(2, "", "")
    client._test_begin_account_image("AR.1")
    client._test_set_account_row("First", "1")
    client._test_set_account_row("Second", "2")
    client._test_dispatch_once()
    assert reports.rows == [(1, "First", "1")] and reports.ends == []
    client._test_dispatch_once()
    assert reports.rows == [(1, "First", "1"), (1, "Replacement", "3"), (2, "Replacement", "3")]
    assert reports.ends == [1, 2]
    client.disconnect()

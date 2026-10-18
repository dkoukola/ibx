"""Offline full-revision callback and latest-authority parity with Rust."""

from ibx import EClient, EWrapper


class Reports(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []

    def exec_details(self, req_id, contract, execution):
        self.rows.append((req_id, contract.con_id, execution.exec_id,
                          execution.shares, execution.price))


def test_terminal_corrections_keep_full_values_and_latest_replay():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client._test_push_execution_revision(1, "terminal.02", 2.0, 3.0)
    client._test_push_execution_revision(1, "terminal.03", 0.0, 3.0)
    client._test_dispatch_once()
    assert reports.rows == [(-1, 1, "terminal.02", 2.0, 3.0),
                            (-1, 1, "terminal.03", 0.0, 3.0)]
    client.req_executions(9)
    client._test_dispatch_once()
    assert reports.rows[-1] == (9, 1, "terminal.03", 0.0, 3.0)
    client._test_push_execution_revision(2, "replayed.02", 1.0, 2.0,
                                         replayed=True)
    client._test_dispatch_once()
    assert len(reports.rows) == 3
    client.disconnect()


def test_tracked_revision_keeps_each_queued_contract_identity():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client._test_push_execution_revision(1, "changed.01", 2.0, 3.0,
                                         tracked=True)
    client._test_push_execution_revision(2, "changed.02", 1.0, 4.0,
                                         tracked=True)
    client._test_dispatch_once()
    assert reports.rows == [(-1, 1, "changed.01", 2.0, 3.0),
                            (-1, 2, "changed.02", 1.0, 4.0)]
    client.disconnect()

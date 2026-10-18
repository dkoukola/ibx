"""Native combo execution component forwarding and replay."""

from ibx import EClient, EWrapper


class Reports(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []

    def exec_details(self, req_id, contract, execution):
        self.rows.append(
            (
                req_id,
                execution.exec_id,
                execution.combo_role,
                execution.combo_leg_index,
                execution.combo_leg_count,
                execution.combo_parent_execution_id,
                execution.combo_raw_descriptor,
            )
        )


def test_combo_components_reach_live_callbacks_and_latest_replay():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client._test_push_execution_revision(1, "ordinary.01", 1.0, 1.0)
    client._test_push_execution_revision(
        2,
        "parent.01",
        1.0,
        -0.05,
        combo_role="parent",
        combo_leg_count=2,
    )
    client._test_push_execution_revision(
        3,
        "leg.01",
        1.0,
        0.0,
        combo_role="leg",
        combo_leg_index=1,
        combo_leg_count=2,
        combo_parent_execution_id="parent.01",
    )
    client._test_push_execution_revision(
        4,
        "unsupported.01",
        1.0,
        1.0,
        combo_role="unsupported",
        combo_parent_execution_id="parent.01",
        combo_raw_descriptor="unexpected",
    )

    client._test_dispatch_once()

    expected = [
        (-1, "ordinary.01", "ordinary", None, None, None, None),
        (-1, "parent.01", "parent", None, 2, None, None),
        (-1, "leg.01", "leg", 1, 2, "parent.01", None),
        (
            -1,
            "unsupported.01",
            "unsupported",
            None,
            None,
            "parent.01",
            "unexpected",
        ),
    ]
    assert reports.rows == expected

    client.req_executions(9)
    client._test_dispatch_once()
    assert reports.rows[4:] == [(9, *row[1:]) for row in expected]
    client.disconnect()

"""Passive option-lifecycle observation forwarding."""

from ibx import EClient, EWrapper


class Reports(EWrapper):
    def __init__(self):
        super().__init__()
        self.lifecycle = []

    def option_lifecycle(
        self,
        raw_value,
        classification,
        account,
        contract_id,
        execution_id,
        order_id,
        api_order_id,
    ):
        self.lifecycle.append(
            (
                raw_value,
                classification,
                account,
                contract_id,
                execution_id,
                order_id,
                api_order_id,
            )
        )


def test_unknown_option_lifecycle_preserves_raw_and_optional_identity():
    reports = Reports()
    client = EClient(reports)
    client._test_connect()
    client._test_push_unknown_option_lifecycle(
        "vendor-extension", contract_id=882133170, order_id="C42.7"
    )

    client._test_dispatch_once()

    assert reports.lifecycle == [
        (
            "vendor-extension",
            "Unknown",
            None,
            882133170,
            None,
            "C42.7",
            None,
        )
    ]
    client._test_dispatch_once()
    assert len(reports.lifecycle) == 1
    client.disconnect()

"""The native basket metadata extension survives Python callback conversion."""

from ibx import ContractDetails, EClient, EWrapper


class Recorder(EWrapper):
    def __init__(self):
        super().__init__()
        self.rows = []

    def contract_details(self, request_id, details):
        self.rows.append(details)


def test_native_basket_composition_reaches_python_without_interpretation():
    assert ContractDetails().basket_composition is None
    wrapper = Recorder()
    client = EClient(wrapper)
    client._test_connect("TEST123")
    native = "USD,CASH,0.0517;558869874,STK,0.46"
    frame = (
        "35=d|55=SOUN2|167=STK|207=BASKET|6008=921601438|15=USD|"
        "146=1|6038=Y|6019=1|6344=1|6008=921601438|6470=BASKET|"
        f"6799={native}|6046=BASKET|"
    ).replace("|", "\x01")
    client._test_push_reply_row(1, frame, 921601438, "BASKET", False, [], [], None, 0)
    client._test_dispatch_once()
    assert len(wrapper.rows) == 1
    assert wrapper.rows[0].basket_composition == native
    assert wrapper.rows[0].contract.con_id == 921601438

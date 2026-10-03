# Completed-order history fixture

`paper_cancelled.jsonl` contains decoded FIX reports from a direct paper-account STANDARD history request (`35=H`, `6533=1`) on 2026-10-03. The account owner authorized the test: submit one AAPL share at a USD 1 DAY limit, then cancel the exact acknowledged order. Cancellation was confirmed without an observed fill. A separate connection requested same-day history afterward.

The first record is the cancelled-order status snapshot; the second is its request's end. In particular, the data row has `20=3` and no `6556`. The end carries `6556` and `55=*`.

`paper_amended_cancelled.jsonl` is a later fresh connection's history reply containing the original order and a second authorized order submitted at USD 1, amended to USD 2, then cancelled. The second order's revised working terms and unchanged broker identity were observed before cancellation. History returns one final cancelled row per tested order, with the amended price and broker modification version retained; no intermediate USD 1 row for that second order was returned. This does not establish reduction rules for every possible multi-report response.

Both captures have equal order-report SendingTime (52) and TransactTime (60). An earlier sanitization incorrectly changed that relationship; the corrected fixtures preserve it. The native `6699 → 60 → 52` precedence was traced in Gateway code and is covered by separate synthetic unit tests, not established by a timestamp difference in these captures.

Account, order, execution, reference and request identifiers, dates and sequence numbers are synthetic; identifier substitutions are local to each capture. Field order, order terms, modification-version suffixes and timestamp relationships are retained; body length and checksum were recomputed after substitution. Pipes represent FIX SOH separators. These are decoded report fixtures, not encrypted transport captures or proof of general multi-order history reduction.

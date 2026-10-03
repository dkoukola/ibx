# Completed-order history fixture

`paper_cancelled.jsonl` contains decoded FIX reports from a direct paper-account STANDARD history request (`35=H`, `6533=1`) on 2026-10-03. The account owner authorized the test: submit one AAPL share at a USD 1 DAY limit, then cancel the exact acknowledged order. Cancellation was confirmed without an observed fill. A separate connection requested same-day history afterward.

The first record is the cancelled-order status snapshot; the second is its request's end. In particular, the data row has `20=3` and no `6556`. The end carries `6556` and `55=*`. Sending time differs from the order's event time.

Account, order, execution, reference and request identifiers, dates and sequence numbers are synthetic. Field order and order terms are retained; body length and checksum were recomputed after substitution. Pipes represent FIX SOH separators. These are decoded report fixtures, not encrypted transport captures or proof of general multi-order history reduction.

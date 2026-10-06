# Native paper execution-history fixture

`paper_u72_replayed_fill.jsonl` is a sanitized subset of a real, read-only paper-account capture taken on 2026-10-06. The probe used the SDK's Gateway connection and a raw CCP reader, without a HotLoop, order submission, or local position accounting. After account verification it observed the initial position images, then sent an account-scoped U72 request covering the preceding paper fill and waited for the exact request end.

The broker's MAIN U75 image and separate Core model image each reported one AAPL share. The U72 execution reply reported one bought share with `97=Y`, `20=0`, and `150=2`, but **no `8080` historical flag and no `6556` request ID**. Its end carried `32=*` and the matching request ID. Before the correction, passing this response through the SDK added the already-held share a second time: the execution query returned one share while the SDK position became two.

Account, order, execution, reference, and request identifiers are synthetic. Opaque routing identifiers and unrelated initial traffic were omitted. Prices, quantities, report times, message types, replay flags, and the absence of correlation/historical tags were retained. FIX framing, sequence numbers, lengths, and checksums are rebuilt by the test from these fields; this is not an untouched wire capture.

`97=Y` means possible resend, not globally historical. The regression concerns a replay admitted to an active explicit execution query. Startup/reconnect recovery and genuine live fills must continue to update positions outside that query classification.

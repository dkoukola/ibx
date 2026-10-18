# Request Contract Details

Resolve a symbol on a paper session: connect, fire `req_contract_details` for AAPL, collect every matching contract, then disconnect when `contract_details_end` lands.

## What this shows

- Building a partial `Contract` (symbol + sec_type + exchange + currency).
- Pumping callbacks with `process_msgs` until the end-of-stream marker arrives.
- Reading `con_id`, primary exchange, and trading class out of `ContractDetails`.

## Native basket metadata

The IBX extension `ContractDetails::basket_composition` (also Python `basket_composition`) retains native field 6799 verbatim. It describes the returned contract itself. An adjusted option may instead point through `under_con_id` to a separate `BASKET` underlying; request that exact contract's details to inspect its composition. Missing composition is unknown, not a certificate of standard-share delivery.

For example, the October 18, 2026 SOUN2 capture contains `USD,CASH,0.0517;558869874,STK,0.46` on its basket underlying. Combined with the separately observed option multiplier 100, this matches [OCC memo 59806](https://infomemo.theocc.com/infomemos?number=59806): 46 SOUN shares and $5.17 per contract. The field is not parsed, normalized, or promoted into execution permission by this extension; unfamiliar component forms remain raw evidence.

The additional public Rust struct fields require explicit struct-literal callers to supply `basket_composition` or use the existing default constructor/update syntax.

## Run it

```bash
IB_USERNAME=... IB_PASSWORD=... cargo run --example hello_contract_details
```

## Source

```rust
{{#include ../../../../../examples/hello_contract_details.rs}}
```

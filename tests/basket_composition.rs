//! Reference-data retention, not deliverable qualification or order authority.

use ibx::{
    api::types::ContractDetails,
    control::contracts::{parse_secdef_records, parse_secdef_response},
};

fn details(frame: &str) -> ContractDetails {
    let definition = parse_secdef_response(frame.replace('|', "\x01").as_bytes()).unwrap();
    ContractDetails::from_definition(&definition)
}

#[test]
fn captured_adjusted_underlying_retains_exact_native_composition() {
    // Excerpts of ordinary paper contract-details replies captured 2026-10-18.
    // OCC 59806 independently specifies 46 SOUN shares + USD 5.17 per contract.
    // The native BASKET record is per underlying unit; option multiplier is 100.
    let basket = details(
        "35=d|320=PreferredReqByConid91003|55=SOUN2|167=STK|207=BASKET|6008=921601438|\
         15=USD|6058=SOUN2|146=1|6038=Y|6019=1|6344=1|6008=921601438|6470=BASKET|\
         6799=USD,CASH,0.0517;558869874,STK,0.46|6046=BASKET|",
    );
    assert_eq!(basket.contract.con_id, 921601438);
    assert_eq!(basket.contract.exchange, "BASKET");
    assert_eq!(
        basket.basket_composition.as_deref(),
        Some("USD,CASH,0.0517;558869874,STK,0.46")
    );
    let option = details(
        "35=d|55=SOUN2|167=OPT|207=BEST|6008=822553410|15=USD|6058=SOUN2|\
         541=20270115|6614=20270115|201=1|202=5|231=100|146=1|6038=Y|6019=1|\
         6344=1|6008=822553410|6346=921601438|310=STK|6855=SOUN2|6659=1|6660=D|",
    );
    assert_eq!(option.under_con_id, basket.contract.con_id as i32);
    assert_eq!(option.contract.multiplier, "100");
    assert_eq!(option.basket_composition, None);
}

#[test]
fn missing_empty_and_unparsed_composition_remain_distinct() {
    let base = "35=d|55=TEST|167=STK|207=BASKET|6008=9|146=0|6344=1|6008=9|";
    assert_eq!(details(base).basket_composition, None);
    assert_eq!(
        details(&format!("{base}6799=|"))
            .basket_composition
            .as_deref(),
        Some("")
    );
    assert_eq!(
        details(&format!("{base}6799=USD,CASH,unknown;opaque,unknown,?|"))
            .basket_composition
            .as_deref(),
        Some("USD,CASH,unknown;opaque,unknown,?")
    );
}

#[test]
fn composition_is_joined_by_its_contract_id_not_record_order_or_underlying() {
    let frame = "35=d|55=BASKET|167=STK|207=BASKET|6008=1|\
        55=OPTION|167=OPT|207=BEST|6008=2|55=OTHER|167=OPT|207=BEST|6008=3|\
        146=0|6344=3|6008=3|6346=1|6008=2|6346=1|\
        6008=1|6799=USD,CASH,0.0517;558869874,STK,0.46|";
    let definitions = parse_secdef_records(frame.replace('|', "\x01").as_bytes()).unwrap();
    assert_eq!(definitions.len(), 3);
    let rows: Vec<_> = definitions
        .iter()
        .map(ContractDetails::from_definition)
        .collect();
    assert_eq!(rows[0].contract.con_id, 1);
    assert_eq!(
        rows[0].basket_composition.as_deref(),
        Some("USD,CASH,0.0517;558869874,STK,0.46")
    );
    assert_eq!(rows[1].contract.con_id, 2);
    assert_eq!(rows[2].contract.con_id, 3);
    assert_eq!(rows[1].basket_composition, None);
    assert_eq!(rows[2].basket_composition, None);
}

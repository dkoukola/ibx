//! Pure broker-report projection. Accounting and publication stay with the caller.
use super::ccp::{hedge_letter, hedge_param_text, perm_id_from_clord_id, report_order_type};
use super::{decode_tif, parse_qty};
use crate::api::types as api;
use crate::bridge::{ComboView, RichOrderInfo};
use crate::engine::context::TrailLimitReported;
use crate::types::{Order, OrderStatus, PRICE_SCALE, Side};
#[cfg(test)]
use std::collections::HashMap;

pub(super) fn report_revision(fields: &crate::protocol::fix::FieldMap<'_>) -> Option<u32> {
    let clord = fields.get(&11)?;
    clord.split_once('.').map_or(Some(0), |(_, version)| version.parse().ok())
}

pub(super) fn report_time<'a>(fields: &crate::protocol::fix::FieldMap<'a>) -> Option<&'a str> {
    fields.get(&6699).or_else(|| fields.get(&60)).or_else(|| fields.get(&52)).copied()
}

/// Native dR checks event time before ClOrd revision; only a cancellation may
/// carry an older revision. This admits order state, never execution accounting.
pub(super) fn stale_report(
    version: Option<u32>, time: Option<&str>,
    previous_version: Option<u32>, previous_time: Option<&str>, cancelled: bool,
) -> bool {
    matches!((time, previous_time), (Some(time), Some(previous)) if time < previous)
        || (!cancelled && matches!((version, previous_version), (Some(version), Some(previous)) if version < previous))
}

pub(super) struct ReportProjection<'a> {
    pub order_id: i64,
    pub parent_id: i64,
    pub status: OrderStatus,
    pub account_id: &'a str,
    pub fallback_order: Option<&'a Order>,
    pub fallback_con_id: i64,
    pub cached_contract: Option<api::Contract>,
    pub combo: Option<&'a ComboView>,
    pub trail_limit: Option<TrailLimitReported>,
    pub combo_leg_prices: Vec<f64>,
}
pub(super) fn project_report(
    parsed: &crate::protocol::fix::FieldMap<'_>,
    input: ReportProjection<'_>,
) -> RichOrderInfo {
    let ReportProjection {
        order_id: clord_id,
        parent_id,
        status,
        account_id,
        fallback_order,
        fallback_con_id,
        cached_contract,
        combo: combo_view,
        trail_limit,
        combo_leg_prices,
    } = input;
    let commission = parsed
        .get(&12)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let exec_id = parsed.get(&17).copied().unwrap_or("");
    let last_px = parsed
        .get(&31)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let last_shares = parsed.get(&32).and_then(|s| parse_qty(s)).unwrap_or(0);
    let account = parsed.get(&1).map(|v| (*v).to_owned()).unwrap_or_default();
    let symbol = parsed.get(&55).map(|v| (*v).to_owned()).unwrap_or_default();
    // The order's routing destination, not the venue where it rests.
    let exchange = parsed.get(&6004).filter(|value| !value.is_empty())
        .map(|value| crate::control::contracts::exchange_from_fix(value).to_string())
        .or_else(|| parsed.get(&207).map(|value| (*value).to_owned()))
        .unwrap_or_default();
    let sec_type = parsed.get(&167).map(|v| (*v).to_owned()).unwrap_or_default();
    let currency = parsed.get(&15).map(|v| (*v).to_owned()).unwrap_or_default();
    let con_id: i64 = parsed.get(&6008).and_then(|s| s.parse().ok()).unwrap_or(0);
    let local_symbol = parsed.get(&6035).map(|v| (*v).to_owned()).unwrap_or_default();
    let perm_id: i64 = parsed
        .get(&11)
        .map(|s| perm_id_from_clord_id(s))
        .unwrap_or(0);
    let total_qty: f64 = parsed.get(&38).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let ord_type_tag = parsed.get(&40).copied().unwrap_or("");
    let limit_price: f64 = parsed.get(&44).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let stop_px: f64 = parsed.get(&99).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let outside_rth = parsed.get(&6433).map(|s| *s == "1").unwrap_or(false);
    let clearing_intent = parsed.get(&6419).map(|v| (*v).to_owned()).unwrap_or_default();
    let auto_cancel_date = parsed.get(&6596).map(|v| (*v).to_owned()).unwrap_or_default();
    let exec_exchange = parsed.get(&30).map(|v| (*v).to_owned()).unwrap_or_default();
    let transact_time = parsed.get(&60).map(|v| (*v).to_owned()).unwrap_or_default();
    let avg_px: f64 = parsed.get(&6).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let cum_qty: f64 = parsed.get(&14).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let last_liq: i32 = parsed.get(&851).and_then(|s| s.parse().ok()).unwrap_or(0);

    let sec_type_str = match sec_type.as_str() {
        "CS" | "COMMON" => "STK",
        "FUT" => "FUT",
        "OPT" => "OPT",
        "FOR" | "CASH" => "CASH",
        "IND" => "IND",
        "FOP" => "FOP",
        "WAR" => "WAR",
        "BAG" => "BAG",
        "BOND" => "BOND",
        "CMDTY" => "CMDTY",
        "NEWS" => "NEWS",
        "FUND" => "FUND",
        _ => &sec_type,
    };

    let order_type_str = report_order_type(ord_type_tag, parsed.get(&18).copied().unwrap_or(""));

    // As the reference: an unknown code is kept ("???"), not read
    // as DAY; each report sets the order's time in force (ibx#307).
    let tif_str = decode_tif(super::report_tif(parsed));

    let action = match parsed.get(&54).copied() {
        Some("1") => "BUY",
        Some("2") => "SELL",
        Some("5") => "SSHORT",
        _ => {
            if let Some(order) = fallback_order {
                match order.side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                    Side::ShortSell => "SSHORT",
                }
            } else {
                ""
            }
        }
    };

    let status_str = crate::client_core::order_status_str(status);

    let resolved_con_id = if con_id != 0 {
        con_id
    } else if fallback_order.is_some() {
        fallback_con_id
    } else {
        0
    };

    let contract = if let Some(view) = &combo_view {
        // A combo order shows its combo, not the report's contract
        // (55=IECombo) (ibx#470).
        view.contract.clone()
    } else if resolved_con_id != 0 {
        if let Some(mut cached) = cached_contract {
            if !symbol.is_empty() {
                cached.symbol = symbol.clone();
            }
            if !sec_type_str.is_empty() {
                cached.sec_type = sec_type_str.to_string();
            }
            if !exchange.is_empty() {
                cached.exchange = exchange.clone();
            }
            if !currency.is_empty() {
                cached.currency = currency.clone();
            }
            if !local_symbol.is_empty() {
                cached.local_symbol = local_symbol.clone();
            }
            cached
        } else {
            api::Contract {
                con_id: resolved_con_id,
                symbol: symbol.clone(),
                sec_type: sec_type_str.to_string(),
                exchange: exchange.clone(),
                currency: currency.clone(),
                local_symbol: local_symbol.clone(),
                ..Default::default()
            }
        }
    } else {
        api::Contract {
            symbol: symbol.clone(),
            sec_type: sec_type_str.to_string(),
            exchange: exchange.clone(),
            currency: currency.clone(),
            local_symbol: local_symbol.clone(),
            ..Default::default()
        }
    };

    let (fb_action, fb_ord_type) = if let Some(ctx_order) = fallback_order {
        let a = match ctx_order.side {
            crate::types::Side::Buy => "BUY",
            crate::types::Side::Sell | crate::types::Side::ShortSell => "SELL",
        };
        let o = match ctx_order.ord_type {
            b'1' => "MKT",
            b'2' => "LMT",
            b'3' => "STP",
            b'4' => "STP LMT",
            b'P' => "TRAIL",
            _ => "",
        };
        (a, o)
    } else {
        ("", "")
    };

    // Derive 3 order-dependent fields from FIX tags
    let oca_type: i32 = match parsed.get(&6209).copied() {
        Some("CancelOnFillWBlock") => 1,
        Some("ReduceOnFillWBlock") => 2,
        Some("ReduceOnFillNonBlock") => 3,
        Some("ReduceOnFillWBlockFromTotal") => 4,
        _ => 3, // default
    };
    let algo_strategy = parsed.get(&847).map(|v| (*v).to_owned()).unwrap_or_default();
    // The price management flag the server echoes, 0 without it
    // (ibx#492).
    let use_price_mgmt_algo: i32 = i32::from(parsed.get(&8339).is_some_and(|v| *v == "1"));
    let trail_stop_price: f64 = parsed
        .get(&6117)
        .and_then(|s| s.parse().ok())
        .unwrap_or(f64::MAX);

    // A TRAIL LIMIT report without its offset, limit price or stop
    // price keeps the last ones (ib-agent#194, ibx#491).

    let limit_price = match trail_limit {
        Some(r) if limit_price == 0.0 && r.limit != 0 => r.limit as f64 / PRICE_SCALE as f64,
        _ => limit_price,
    };
    let trail_stop_price = match trail_limit {
        Some(r) if trail_stop_price == f64::MAX && r.stop != 0 => {
            r.stop as f64 / PRICE_SCALE as f64
        }
        _ => trail_stop_price,
    };
    let order = api::Order {
        order_id: clord_id,
        action: if action.is_empty() {
            fb_action.to_string()
        } else {
            action.to_string()
        },
        total_quantity: total_qty,
        order_type: if order_type_str.is_empty() {
            fb_ord_type.to_string()
        } else {
            order_type_str.to_string()
        },
        lmt_price: limit_price,
        aux_price: stop_px,
        tif: tif_str.to_string(),
        good_till_date: parsed.get(&126).map(|time| format!("{} UTC", time.replace('-', " ")))
            .or_else(|| parsed.get(&432).map(|v| (*v).to_owned())).unwrap_or_default(),
        account: if account.is_empty() {
            account_id.to_string()
        } else {
            account.clone()
        },
        perm_id,
        parent_id,
        // Filled so far, not the quantity still working (ibx#309).
        filled_quantity: cum_qty,
        outside_rth,
        clearing_intent,
        auto_cancel_date,
        submitter: account_id.to_string(),
        oca_group: parsed.get(&583).map(|v| (*v).to_owned()).unwrap_or_default(),
        oca_type,
        use_price_mgmt_algo,
        trail_stop_price,
        algo_strategy,
        // The orderRef the server echoes (ibx#466).
        order_ref: parsed.get(&6010).map(|v| (*v).to_owned()).unwrap_or_default(),
        // The cash quantity the server echoes in 152, which the
        // reference reads into the order's cash quantity
        // (`jexec.fq.<init>(dk, boolean)@2005-2120`, ibx#263).
        cash_qty: parsed.get(&152).and_then(|s| s.parse().ok()).unwrap_or(0.0),
        // A TRAIL LIMIT's offset as the server reports it (ib-agent#194).
        lmt_price_offset: trail_limit.map_or(f64::MAX, |r| r.offset as f64 / PRICE_SCALE as f64),
        // A combo's per-leg prices as reported (ibx#470).
        order_combo_legs: combo_leg_prices,
        client_id: parsed.get(&6119).and_then(|s| s.parse().ok()).unwrap_or(0),
        min_compete_size: if parsed.get(&6205).copied() == Some("1") { i32::MAX } else { 100 },
        compete_against_best_offset: if parsed.get(&6205).copied() == Some("1") { f64::MAX } else { 0.02 },
        scale_auto_reset: parsed.get(&6461).copied() == Some("1"),
        hedge_type: hedge_letter(parsed.get(&6665).copied().unwrap_or("")).to_string(),
        hedge_param: parsed.get(&6703).or_else(|| parsed.get(&6666))
            .and_then(|value| value.parse::<f64>().ok()).map(hedge_param_text).unwrap_or_default(),
        hedge_max_size: parsed.get(&6690).and_then(|value| value.parse().ok()).unwrap_or(i32::MAX),
        ..Default::default()
    };

    let completed_time = if matches!(
        status,
        crate::types::OrderStatus::Filled
            | crate::types::OrderStatus::Cancelled
            | crate::types::OrderStatus::Rejected
    ) {
        // The effective event time can differ from the report's SendingTime.
        // Native eO.a(fb) selects 6699, then TransactTime, then SendingTime.
        parsed.get(&6699).or_else(|| parsed.get(&60)).or_else(|| parsed.get(&52))
            .map(|v| (*v).to_owned()).unwrap_or_default()
    } else {
        String::new()
    };
    let completed_status = match status {
        crate::types::OrderStatus::Filled => "Filled".to_string(),
        crate::types::OrderStatus::Cancelled => "Cancelled".to_string(),
        crate::types::OrderStatus::Rejected => parsed
            .get(&58)
            .map(|v| (*v).to_owned())
            .unwrap_or_else(|| "Rejected".to_string()),
        _ => String::new(),
    };

    let order_state = api::OrderState {
        status: status_str.to_string(),
        commission_and_fees: commission,
        completed_time,
        completed_status,
        ..Default::default()
    };

    let last_exec = api::Execution {
        exec_id: exec_id.to_string(),
        time: transact_time,
        acct_number: account,
        exchange: exec_exchange,
        side: if let Some(o) = fallback_order {
            match o.side {
                Side::Buy => "BOT",
                Side::Sell | Side::ShortSell => "SLD",
            }
            .to_string()
        } else {
            String::new()
        },
        shares: last_shares as f64,
        price: last_px,
        order_id: clord_id,
        cum_qty,
        avg_price: avg_px,
        last_liquidity: last_liq,
        ..Default::default()
    };

    RichOrderInfo {
        contract,
        order,
        order_state,
        last_exec,
        parent_id_known: parsed.contains_key(&6107)
            || parsed.get(&20).is_some_and(|value| *value == "3"),
        report_revision: report_revision(parsed),
        report_time: report_time(parsed).map(str::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> ReportProjection<'static> {
        ReportProjection {
            order_id: 1_791_024_000_001,
            parent_id: 1_791_024_000_000,
            status: OrderStatus::Cancelled,
            account_id: "DU_TEST",
            fallback_order: None,
            fallback_con_id: 0,
            cached_contract: None,
            combo: None,
            trail_limit: None,
            combo_leg_prices: Vec::new(),
        }
    }

    #[test]
    fn pure_report_projection_preserves_native_terms_and_identity() {
        let report: HashMap<_, _> = [
            (1, "DU_TEST"),
            (11, "1339547414.2"),
            (55, "TEST"),
            (6008, "123"),
            (167, "CS"),
            (207, "SMART"),
            (15, "USD"),
            (6035, "TEST_LOCAL"),
            (54, "2"),
            (38, "5"),
            (40, "4"),
            (44, "101.25"),
            (99, "102.5"),
            (59, "1"),
            (583, "broker.oca.17"),
            (6209, "CancelOnFillWBlock"),
            (6010, "paper-prototype"),
            (6433, "1"),
            (6419, "IB"),
            (6596, "20261003"),
            (152, "501.25"),
            (8339, "1"),
            (14, "2"),
            (12, "1.25"),
            (17, "fixture.exec.1"),
            (30, "TEST_EXCHANGE"),
            (60, "20261003-10:00:00"),
            (52, "20261003-10:01:00"),
            (31, "101.5"),
            (32, "2"),
            (6, "101.5"),
            (851, "2"),
        ]
        .into_iter()
        .map(|(tag, value)| (tag, value.to_string()))
        .collect();
        let original = report.clone();
        let projected = project_report(&crate::protocol::fix::field_view(&report), input());
        assert_eq!(report, original);
        assert_eq!(projected.contract.con_id, 123);
        assert_eq!(projected.contract.symbol, "TEST");
        assert_eq!(projected.contract.sec_type, "STK");
        assert_eq!(projected.contract.local_symbol, "TEST_LOCAL");
        assert_eq!(projected.order.order_id, 1_791_024_000_001);
        assert_eq!(projected.order.parent_id, 1_791_024_000_000);
        assert_eq!(projected.order.perm_id, 1_339_547_414);
        assert_eq!(projected.order.action, "SELL");
        assert_eq!(projected.order.order_type, "STP LMT");
        assert_eq!(projected.order.total_quantity, 5.0);
        assert_eq!(projected.order.lmt_price, 101.25);
        assert_eq!(projected.order.aux_price, 102.5);
        assert_eq!(projected.order.tif, "GTC");
        assert_eq!(projected.order.oca_group, "broker.oca.17");
        assert_eq!(projected.order.oca_type, 1);
        assert_eq!(projected.order.order_ref, "paper-prototype");
        assert!(projected.order.outside_rth);
        assert_eq!(projected.order.cash_qty, 501.25);
        assert_eq!(projected.order.filled_quantity, 2.0);
        assert_eq!(projected.order.use_price_mgmt_algo, 1);
        assert_eq!(projected.order_state.completed_time, "20261003-10:00:00");
        assert_eq!(projected.order_state.completed_status, "Cancelled");
        assert_eq!(projected.order_state.commission_and_fees, 1.25);
        assert_eq!(projected.last_exec.exec_id, "fixture.exec.1");
        assert_eq!(projected.last_exec.order_id, projected.order.order_id);
        assert_eq!(projected.last_exec.price, 101.5);
        // Preserve the existing projection's fixed-point quantity contract.
        assert_eq!(
            projected.last_exec.shares,
            (2 * crate::types::QTY_SCALE) as f64
        );
        assert_eq!(projected.last_exec.last_liquidity, 2);
        assert_eq!(
            format!("{projected:?}"),
            format!("{:?}", project_report(&crate::protocol::fix::field_view(&report), input()))
        );
    }

    #[test]
    fn projection_keeps_upstream_route_hedge_and_order_type_fields() {
        let mut fields = crate::protocol::fix::FieldMap::from([
            (6004, "BEST"), (207, "ARCA"), (40, "P"), (18, "M"),
            (6665, "2"), (6666, "0.5"), (6703, "2"), (6690, "5"),
            (6461, "1"),
        ]);
        let projected = project_report(&fields, input());
        assert_eq!(projected.contract.exchange, "SMART");
        assert_eq!(projected.order.order_type, "PEG MID");
        assert_eq!(projected.order.hedge_type, "D");
        assert_eq!(projected.order.hedge_param, "2.0");
        assert_eq!(projected.order.hedge_max_size, 5);
        assert!(projected.order.scale_auto_reset);
        assert_eq!(projected.order.min_compete_size, 100);
        assert_eq!(projected.order.compete_against_best_offset, 0.02);
        fields.insert(6205, "1");
        fields.insert(40, "TSL");
        fields.remove(&6703);
        let projected = project_report(&fields, input());
        assert_eq!(projected.order.order_type, "TRAIL LIMIT");
        assert_eq!(projected.order.hedge_param, "0.5");
        assert_eq!(projected.order.min_compete_size, i32::MAX);
        assert_eq!(projected.order.compete_against_best_offset, f64::MAX);
    }

    #[test]
    fn completed_time_uses_native_event_time_precedence_only_for_terminal_orders() {
        let mut report: HashMap<u32, String> = [
            (52, "20260102-12:03:24"),
            (60, "20260102-12:02:53"),
            (6699, "20260102-12:02:52"),
        ].into_iter().map(|(tag, value)| (tag, value.to_string())).collect();
        for status in [OrderStatus::Filled, OrderStatus::Cancelled, OrderStatus::Rejected] {
            assert_eq!(project_report(&crate::protocol::fix::field_view(&report), ReportProjection { status, ..input() })
                .order_state.completed_time, "20260102-12:02:52");
        }
        assert_eq!(project_report(&crate::protocol::fix::field_view(&report), ReportProjection { status: OrderStatus::Submitted, ..input() })
            .order_state.completed_time, "");
        report.remove(&6699);
        assert_eq!(project_report(&crate::protocol::fix::field_view(&report), input()).order_state.completed_time, "20260102-12:02:53");
        report.remove(&60);
        assert_eq!(project_report(&crate::protocol::fix::field_view(&report), input()).order_state.completed_time, "20260102-12:03:24");
        report.remove(&52);
        assert_eq!(project_report(&crate::protocol::fix::field_view(&report), input()).order_state.completed_time, "");
    }

    #[test]
    fn captured_completed_history_preserves_identity_terms_and_event_time() {
        // Direct paper STANDARD H reply captured after an acknowledged cancel.
        // Identifiers and dates are sanitized; data rows have no query tag.
        let fixture = include_str!("../../../tests/fixtures/completed_history/paper_cancelled.jsonl");
        let row: serde_json::Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
        let wire = row["fix"].as_str().unwrap().replace('|', "\x01");
        let report = crate::protocol::fix::fix_parse(wire.as_bytes());
        assert!(!report.contains_key(&6556));
        assert_eq!(report.get(&20).map(String::as_str), Some("3"));
        let projected = project_report(&crate::protocol::fix::field_view(&report), input());
        assert_eq!(projected.order.perm_id, 1_234_567_890);
        assert_eq!(projected.order.order_ref, "sanitized-paper-history-fixture");
        assert_eq!(projected.contract.con_id, 265598);
        assert_eq!(projected.order.total_quantity, 1.0);
        assert_eq!(projected.order.lmt_price, 1.0);
        assert_eq!(projected.order_state.completed_time, "20260102-12:02:53");
        assert_eq!(projected.order_state.completed_status, "Cancelled");
    }

    #[test]
    fn pure_report_projection_uses_explicit_fallbacks_without_mutating_them() {
        let fallback = Order::new(77, 0, Side::ShortSell, 3, 0, b'P', b'1', 0);
        let cached = api::Contract {
            con_id: 321,
            symbol: "CACHED".into(),
            sec_type: "OPT".into(),
            strike: 125.0,
            ..Default::default()
        };
        let combo = ComboView {
            contract: api::Contract {
                symbol: "PAIR".into(),
                sec_type: "BAG".into(),
                ..Default::default()
            },
            leg_prices: vec![12.5, f64::MAX],
        };
        let report = HashMap::from([(59, "1".into())]);
        for combo_view in [None, Some(&combo)] {
            let projected = project_report(
                &crate::protocol::fix::field_view(&report),
                ReportProjection {
                    order_id: 77,
                    status: OrderStatus::Submitted,
                    fallback_order: Some(&fallback),
                    fallback_con_id: 321,
                    cached_contract: Some(cached.clone()),
                    combo: combo_view,
                    trail_limit: Some(TrailLimitReported {
                        offset: PRICE_SCALE / 4,
                        limit: 125 * PRICE_SCALE,
                        stop: 126 * PRICE_SCALE,
                    }),
                    combo_leg_prices: combo_view.map(|v| v.leg_prices.clone()).unwrap_or_default(),
                    ..input()
                },
            );
            assert_eq!(projected.order.action, "SSHORT");
            assert_eq!(projected.order.order_type, "TRAIL");
            assert_eq!(projected.order.account, "DU_TEST");
            assert_eq!(projected.order.perm_id, 0);
            assert_eq!(projected.order.lmt_price, 125.0);
            assert_eq!(projected.order.trail_stop_price, 126.0);
            assert_eq!(projected.order.lmt_price_offset, 0.25);
            assert_eq!(projected.last_exec.side, "SLD");
            assert!(projected.order_state.completed_time.is_empty());
            assert!(projected.order_state.completed_status.is_empty());
            assert_eq!(
                projected.contract,
                combo_view.map(|v| &v.contract).unwrap_or(&cached).clone()
            );
            assert_eq!(
                projected.order.order_combo_legs,
                combo_view.map(|v| v.leg_prices.clone()).unwrap_or_default()
            );
        }
        assert_eq!(cached.symbol, "CACHED");
        assert_eq!(combo.leg_prices, vec![12.5, f64::MAX]);
        assert_eq!(fallback.order_id, 77);
    }
}

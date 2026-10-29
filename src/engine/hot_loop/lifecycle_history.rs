//! Explicit lifecycle projections, isolated from ordinary fill accounting.
#[cfg(test)]
use std::collections::HashMap;

use crate::api::types::{ExecutionFilter, OptionLifecycleReport, OptionLifecycleReportKind};
use crate::bridge::SharedState;
use crate::client_core::{ClientCore, StoredExecution, format_exec_time};
use crate::protocol::fix::FieldMap;
use crate::types::{OptionLifecycleClassification, OrderStatus};

use super::ccp::{
    fill_exec_of, option_lifecycle_observation, perm_id_from_clord_id, split_exec_revision,
};
use super::report::{ReportProjection, project_report};

pub(super) fn project(
    fields: &FieldMap<'_>,
    shared: &SharedState,
    account: &str,
) -> Result<OptionLifecycleReport, String> {
    let observation = option_lifecycle_observation(fields).ok_or("Not a lifecycle report")?;
    let value = |tag| {
        fields
            .get(&tag)
            .copied()
            .ok_or("Missing lifecycle field")
    };
    let number = |tag| {
        value(tag)?
            .parse::<f64>()
            .ok()
            .filter(|n| n.is_finite() && *n >= 0.0)
            .ok_or("Invalid lifecycle quantity or price")
    };
    if value(1)? != account || value(17)?.is_empty() {
        return Err("Incomplete lifecycle account or execution identity".into());
    }
    if value(15)?.is_empty() {
        return Err("Missing lifecycle currency".into());
    }
    let perm_id = perm_id_from_clord_id(value(11)?);
    let con_id = value(6008)?
        .parse::<i64>()
        .map_err(|_| "Invalid lifecycle contract")?;
    if perm_id <= 0 || con_id <= 0 {
        return Err("Invalid lifecycle order or contract identity".into());
    }
    let requested_quantity = number(38)?;
    let last_quantity = number(32)?;
    let cumulative_quantity = number(14)?;
    let last_price = number(31)?;
    let average_price = number(6)?;
    if requested_quantity <= 0.0
        || last_quantity > cumulative_quantity
        || cumulative_quantity > requested_quantity
    {
        return Err("Inconsistent lifecycle quantities".into());
    }
    let transaction = value(20)?;
    let exec_type = value(150)?;
    let status = value(39)?;
    let kind = match (observation.classification, transaction, exec_type, status) {
        (
            OptionLifecycleClassification::Exercise
            | OptionLifecycleClassification::Lapse
            | OptionLifecycleClassification::AutoExercise
            | OptionLifecycleClassification::Expired,
            "0",
            "2",
            "2",
        ) if last_quantity > 0.0 => OptionLifecycleReportKind::EconomicExecution,
        (
            OptionLifecycleClassification::Exercise | OptionLifecycleClassification::Lapse,
            "3",
            "0",
            "0",
        )
        | (
            OptionLifecycleClassification::Exercise | OptionLifecycleClassification::Lapse,
            "3",
            "2",
            "2",
        )
        | (
            OptionLifecycleClassification::Exercise | OptionLifecycleClassification::Lapse,
            "3",
            "4",
            "4",
        )
        | (
            OptionLifecycleClassification::Exercise | OptionLifecycleClassification::Lapse,
            "3",
            "8",
            "8",
        ) if last_quantity == 0.0 => OptionLifecycleReportKind::InstructionStatus,
        _ => return Err("Unsupported option lifecycle report semantics".into()),
    };
    let side = match value(54)? {
        "1" => "BOT",
        "2" => "SLD",
        _ => return Err("Unsupported lifecycle side".into()),
    };
    let metadata = fill_exec_of(fields, value(17)?);
    let time_secs = metadata
        .execution_time_secs
        .ok_or("Missing lifecycle execution time")?;
    let projected = project_report(
        fields,
        ReportProjection {
            order_id: observation
                .api_order_id
                .filter(|id| *id > 0 && *id != i64::from(i32::MAX))
                .unwrap_or(perm_id),
            parent_id: 0,
            status: OrderStatus::Filled,
            account_id: account,
            fallback_order: None,
            fallback_con_id: 0,
            cached_contract: shared.reference.get_contract(con_id),
            combo: None,
            trail_limit: None,
            combo_leg_prices: Vec::new(),
        },
    );
    if !matches!(projected.contract.sec_type.as_str(), "STK" | "OPT") {
        return Err("Unsupported lifecycle contract type".into());
    }
    let complete_whole_first_print = last_quantity.fract() == 0.0
        && last_quantity == cumulative_quantity
        && cumulative_quantity == requested_quantity
        && split_exec_revision(value(17)?).1 == 1;
    // Captured lapse and natural expiry each dispose one complete long option
    // at zero. Neither establishes stock delivery or correction semantics.
    if matches!(
        observation.classification,
        OptionLifecycleClassification::Lapse | OptionLifecycleClassification::Expired
    ) && kind == OptionLifecycleReportKind::EconomicExecution
        && (projected.contract.sec_type != "OPT"
            || side != "SLD"
            || last_price != 0.0
            || average_price != 0.0
            || !complete_whole_first_print)
    {
        return Err("Unsupported option disposal execution semantics".into());
    }
    // The captured automatic exercise uses the same per-row economic fields
    // as manual exercise. Pairing and qualified delivery ratios belong to the
    // consumer; this decoder does not infer them from the classification.
    if observation.classification == OptionLifecycleClassification::AutoExercise
        && (!complete_whole_first_print
            || last_price != average_price
            || (projected.contract.sec_type == "OPT" && (side != "SLD" || last_price != 0.0))
            || (projected.contract.sec_type == "STK" && last_price <= 0.0))
    {
        return Err("Unsupported automatic exercise execution semantics".into());
    }
    let mut execution = projected.last_exec;
    execution.perm_id = perm_id;
    execution.acct_number = account.into();
    execution.side = side.into();
    execution.shares = last_quantity;
    execution.cum_qty = cumulative_quantity;
    execution.price = last_price;
    execution.avg_price = average_price;
    execution.time = format_exec_time(time_secs);
    execution.time_unix_seconds = Some(time_secs);
    execution.exchange = metadata.exchange;
    execution.client_id = metadata.client_id;
    execution.model_code = metadata.model_code;
    execution.order_ref = metadata.order_ref;
    Ok(OptionLifecycleReport {
        observation,
        kind,
        contract: projected.contract,
        execution,
        time_secs,
        requested_quantity,
        exec_transaction_type: transaction.into(),
        exec_type: exec_type.into(),
        order_status: status.into(),
        native_order_id: fields.get(&37).map(|value| (*value).to_owned()),
        native_report_time: fields.get(&60).map(|value| (*value).to_owned()),
        native_side: value(54)?.into(),
        commission_and_fees: None,
    })
}

pub(super) fn matches_filter(report: &OptionLifecycleReport, filter: &ExecutionFilter) -> bool {
    // Reuse the execution filter's existing time/side/client semantics.
    !ClientCore::filter_executions(
        &[StoredExecution {
            req_id: 0,
            contract: report.contract.clone(),
            execution: report.execution.clone(),
            time_secs: Some(report.time_secs),
            commission_and_fees: None,
        }],
        filter,
    )
    .is_empty()
}

#[cfg(test)]
pub(super) fn captured_manual_rows() -> Vec<HashMap<u32, String>> {
    // Shapes captured from official Gateway call+put exercise and native U72
    // replay on 2026-10-23. Only account/date/order/execution IDs are sanitized.
    [
        ("41.0", "call.01.01", "9599491", "CS", "100", "11", "1"),
        ("41.0", "call.02.01", "922376799", "OPT", "1", "0.00", "2"),
        ("42.0", "put.01.01", "9599491", "CS", "100", "12", "2"),
        ("42.0", "put.02.01", "922378222", "OPT", "1", "0.00", "2"),
    ]
    .into_iter()
    .map(|(order, exec, contract, sec_type, quantity, price, side)| {
        [
            (35, "8"),
            (97, "Y"),
            (11, order),
            (37, "opaque-native-order"),
            (17, exec),
            (150, "2"),
            (20, "0"),
            (39, "2"),
            (38, quantity),
            (32, quantity),
            (31, price),
            (14, quantity),
            (6, price),
            (54, side),
            (1, "DU123456"),
            (60, "20261003-12:00:00"),
            (6121, "1"),
            (6119, "1937"),
            (6008, contract),
            (6809, "1"),
            (167, sec_type),
            (55, "F"),
            (15, "USD"),
        ]
        .into_iter()
        .map(|(tag, value)| (tag, value.to_owned()))
        .collect()
    })
    .collect()
}

#[cfg(test)]
pub(super) fn captured_instruction(pending_lapse: bool) -> HashMap<u32, String> {
    let mut fields = captured_manual_rows().remove(1);
    for (tag, value) in [(17, "status.123.3"), (20, "3"), (32, "0"), (54, "1")] {
        fields.insert(tag, value.into());
    }
    fields.remove(&6121);
    fields.remove(&6119);
    if pending_lapse {
        for (tag, value) in [
            (17, "status.456.0"),
            (11, "43.0"),
            (6809, "2"),
            (150, "0"),
            (39, "0"),
            (14, "0"),
        ] {
            fields.insert(tag, value.into());
        }
    }
    fields
}

#[cfg(test)]
pub(super) fn captured_lapse_rows() -> [HashMap<u32, String>; 2] {
    // Official Gateway disposal at 2026-10-23 21:25:28 UTC, replayed natively
    // at 21:52:39: economic row plus completed instruction. Account, date and
    // order/execution identities are sanitized; quantities/prices/type retained.
    let execution: HashMap<_, _> = [
        (35, "8"),
        (97, "Y"),
        (11, "43.0"),
        (37, "opaque-lapse-order"),
        (17, "lapse.01.01"),
        (150, "2"),
        (20, "0"),
        (39, "2"),
        (38, "1"),
        (32, "1"),
        (31, "0.00"),
        (14, "1"),
        (6, "0"),
        (54, "2"),
        (1, "DU123456"),
        (60, "20261003-21:25:28"),
        (6121, "3"),
        (6119, "1937"),
        (6008, "920188659"),
        (6809, "2"),
        (167, "OPT"),
        (55, "F"),
        (15, "USD"),
        (200, "202610"),
        (201, "0"),
        (202, "12"),
        (231, "100"),
    ]
    .into_iter()
    .map(|(tag, value)| (tag, value.to_owned()))
    .collect();
    let mut instruction = execution.clone();
    for (tag, value) in [(17, "status.789.0"), (20, "3"), (32, "0"), (54, "1")] {
        instruction.insert(tag, value.into());
    }
    instruction.remove(&6121);
    instruction.remove(&6119);
    [execution, instruction]
}

#[cfg(test)]
pub(super) fn captured_automatic_rows() -> Vec<HashMap<u32, String>> {
    // Native startup and U72 replay captured on 2026-10-24 at 03:40 UTC;
    // economic time 02:13:01. Account/date/order/execution IDs sanitized.
    // No API order/client ID was supplied on these automatic reports.
    [
        (
            "44.0",
            "auto.02.01",
            "9599491",
            "CS",
            "100",
            "11",
            "1",
            "101",
            "auto-order",
        ),
        (
            "44.0",
            "auto.03.01",
            "920187234",
            "OPT",
            "1",
            "0",
            "2",
            "101",
            "auto-order",
        ),
        (
            "45.0",
            "expiry.01.01",
            "920187341",
            "OPT",
            "1",
            "0",
            "2",
            "102",
            "expiry-order",
        ),
    ]
    .into_iter()
    .map(
        |(order, exec, contract, sec_type, quantity, price, side, class, key)| {
            [
                (35, "8"),
                (11, order),
                (37, key),
                (17, exec),
                (150, "2"),
                (20, "0"),
                (39, "2"),
                (38, quantity),
                (32, quantity),
                (31, price),
                (14, quantity),
                (6, price),
                (54, side),
                (1, "DU123456"),
                (60, "20261004-02:13:01"),
                (6008, contract),
                (6809, class),
                (167, sec_type),
                (55, "F"),
                (15, "USD"),
            ]
            .into_iter()
            .map(|(tag, value)| (tag, value.to_owned()))
            .collect()
        },
    )
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::fix::field_view;

    #[test]
    fn captured_manual_execution_and_instruction_shapes_keep_distinct_economics() {
        let shared = SharedState::new();
        for (fields, quantity, price) in captured_manual_rows()
            .into_iter()
            .zip([100.0, 1.0, 100.0, 1.0])
            .zip([11.0, 0.0, 12.0, 0.0])
            .map(|((a, b), c)| (a, b, c))
        {
            let row = project(&field_view(&fields), &shared, "DU123456").unwrap();
            assert_eq!(row.kind, OptionLifecycleReportKind::EconomicExecution);
            assert_eq!(row.execution.shares, quantity);
            assert_eq!(row.execution.cum_qty, quantity);
            assert_eq!(row.requested_quantity, quantity);
            assert_eq!(row.execution.price, price);
            assert_eq!(row.execution.time_unix_seconds, Some(row.time_secs));
            assert_eq!(row.native_order_id.as_ref(), fields.get(&37));
            assert_eq!(row.observation.order_id.as_ref(), fields.get(&11));
            assert_eq!(row.native_report_time.as_ref(), fields.get(&60));
            assert_eq!(row.native_side, fields[&54]);
            assert_eq!(row.contract.currency, "USD");
            assert!(row.commission_and_fees.is_none());
        }
        for pending in [false, true] {
            let row = project(&field_view(&captured_instruction(pending)), &shared, "DU123456").unwrap();
            assert_eq!(row.kind, OptionLifecycleReportKind::InstructionStatus);
            assert_eq!(row.execution.shares, 0.0);
            assert_eq!(row.execution.cum_qty, if pending { 0.0 } else { 1.0 });
            assert!(row.observation.api_order_id.is_none());
        }
    }

    #[test]
    fn captured_lapse_is_an_option_disposal_not_its_completed_instruction() {
        let shared = SharedState::new();
        let [economic, instruction] = captured_lapse_rows();
        let economic = project(&field_view(&economic), &shared, "DU123456").unwrap();
        let instruction = project(&field_view(&instruction), &shared, "DU123456").unwrap();
        assert_eq!(economic.kind, OptionLifecycleReportKind::EconomicExecution);
        assert_eq!(
            economic.observation.classification,
            OptionLifecycleClassification::Lapse
        );
        assert_eq!(economic.contract.con_id, 920188659);
        assert_eq!(economic.contract.sec_type, "OPT");
        assert_eq!(economic.contract.currency, "USD");
        assert_eq!(economic.execution.exec_id, "lapse.01.01");
        assert_eq!(economic.execution.perm_id, 43);
        assert_eq!(economic.execution.side, "SLD");
        assert_eq!(economic.execution.shares, 1.0);
        assert_eq!(economic.execution.cum_qty, 1.0);
        assert_eq!(economic.requested_quantity, 1.0);
        assert_eq!(economic.execution.price, 0.0);
        assert_eq!(economic.execution.avg_price, 0.0);
        assert_eq!(economic.exec_transaction_type, "0");
        assert_eq!(
            economic.time_secs,
            super::super::ccp::fix_utc_to_unix_secs("20261003-21:25:28").unwrap()
        );
        assert_eq!(
            economic.execution.time_unix_seconds,
            Some(economic.time_secs)
        );
        assert!(economic.commission_and_fees.is_none());
        assert_eq!(
            instruction.kind,
            OptionLifecycleReportKind::InstructionStatus
        );
        assert_eq!(instruction.execution.exec_id, "status.789.0");
        assert_eq!(instruction.execution.shares, 0.0);
        assert_eq!(instruction.execution.cum_qty, 1.0);
        assert_eq!(instruction.exec_transaction_type, "3");
        assert_eq!(
            instruction.observation.order_id,
            economic.observation.order_id
        );
        assert_eq!(instruction.native_order_id, economic.native_order_id);
        assert_eq!(instruction.native_report_time, economic.native_report_time);
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(shared.orders.drain_fills_with_exec().is_empty());
    }

    #[test]
    fn lapse_does_not_infer_unobserved_delivery_partial_or_correction_semantics() {
        let shared = SharedState::new();
        let [original, _] = captured_lapse_rows();
        for (tag, value) in [
            (6809, "100"),
            (20, "1"),
            (20, "2"),
            (20, "3"),
            (150, "0"),
            (39, "0"),
            (32, "0"),
            (32, "0.5"),
            (38, "2"),
            (31, "1"),
            (6, "1"),
            (54, "1"),
            (167, "CS"),
            (17, "lapse.01.02"),
            (17, "lapse"),
        ] {
            let mut fields = original.clone();
            fields.insert(tag, value.into());
            assert!(
                project(&field_view(&fields), &shared, "DU123456").is_err(),
                "{tag}={value}"
            );
        }
        let mut fractional = original;
        for tag in [32, 14, 38] {
            fractional.insert(tag, "0.5".into());
        }
        assert!(project(&field_view(&fractional), &shared, "DU123456").is_err());
    }

    #[test]
    fn captured_automatic_economics_preserve_classification_without_api_ids_or_fees() {
        let shared = SharedState::new();
        for (fields, class, contract, side, quantity, price, permanent) in captured_automatic_rows()
            .into_iter()
            .zip([
                (
                    OptionLifecycleClassification::AutoExercise,
                    9599491,
                    "BOT",
                    100.0,
                    11.0,
                    44,
                ),
                (
                    OptionLifecycleClassification::AutoExercise,
                    920187234,
                    "SLD",
                    1.0,
                    0.0,
                    44,
                ),
                (
                    OptionLifecycleClassification::Expired,
                    920187341,
                    "SLD",
                    1.0,
                    0.0,
                    45,
                ),
            ])
            .map(
                |(fields, (class, contract, side, quantity, price, permanent))| {
                    (fields, class, contract, side, quantity, price, permanent)
                },
            )
        {
            let row = project(&field_view(&fields), &shared, "DU123456").unwrap();
            assert_eq!(row.kind, OptionLifecycleReportKind::EconomicExecution);
            assert_eq!(row.observation.classification, class);
            assert_eq!(row.observation.raw_value, fields[&6809]);
            assert_eq!(row.contract.con_id, contract);
            assert_eq!(row.execution.side, side);
            assert_eq!(row.execution.shares, quantity);
            assert_eq!(row.execution.cum_qty, quantity);
            assert_eq!(row.requested_quantity, quantity);
            assert_eq!(row.execution.price, price);
            assert_eq!(row.execution.avg_price, price);
            assert_eq!(row.execution.perm_id, permanent);
            assert_eq!(row.execution.exec_id, fields[&17]);
            assert_eq!(row.observation.order_id.as_ref(), fields.get(&11));
            assert_eq!(row.native_order_id.as_ref(), fields.get(&37));
            assert_eq!(row.execution.time_unix_seconds, Some(row.time_secs));
            assert_eq!(
                row.time_secs,
                super::super::ccp::fix_utc_to_unix_secs("20261004-02:13:01").unwrap()
            );
            assert!(row.observation.api_order_id.is_none());
            assert!(row.commission_and_fees.is_none());
        }
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(shared.orders.drain_fills_with_exec().is_empty());
    }

    #[test]
    fn automatic_reports_do_not_admit_assignment_instructions_partial_or_corrected_economics() {
        let shared = SharedState::new();
        for original in captured_automatic_rows() {
            for (tag, value) in [
                (6809, "100"),
                (6809, "103"),
                (6809, "200"),
                (20, "1"),
                (20, "2"),
                (150, "0"),
                (39, "0"),
                (32, "0"),
                (38, "200"),
                (17, "automatic.01.02"),
            ] {
                let mut fields = original.clone();
                fields.insert(tag, value.into());
                assert!(
                    project(&field_view(&fields), &shared, "DU123456").is_err(),
                    "{tag}={value}"
                );
            }
            let mut fractional = original.clone();
            for tag in [32, 14, 38] {
                fractional.insert(tag, "0.5".into());
            }
            assert!(project(&field_view(&fractional), &shared, "DU123456").is_err());
            for status in ["0", "2", "4", "8"] {
                let mut instruction = original.clone();
                for (tag, value) in [(20, "3"), (32, "0"), (150, status), (39, status)] {
                    instruction.insert(tag, value.into());
                }
                assert!(project(&field_view(&instruction), &shared, "DU123456").is_err());
            }
            for tag in [1, 11, 17, 20, 150, 39, 6008, 32, 14, 38, 31, 6, 54, 60, 15] {
                let mut missing = original.clone();
                missing.remove(&tag);
                assert!(
                    project(&field_view(&missing), &shared, "DU123456").is_err(),
                    "missing {tag}"
                );
            }
        }
        for original in captured_automatic_rows().into_iter().skip(1) {
            for (tag, value) in [(54, "1"), (31, "1"), (6, "1")] {
                let mut fields = original.clone();
                fields.insert(tag, value.into());
                assert!(
                    project(&field_view(&fields), &shared, "DU123456").is_err(),
                    "{tag}={value}"
                );
            }
        }
        let mut stock = captured_automatic_rows().remove(0);
        stock.insert(31, "0".into());
        stock.insert(6, "0".into());
        assert!(project(&field_view(&stock), &shared, "DU123456").is_err());
    }

    #[test]
    fn zero_is_not_missing_and_unknown_lifecycle_semantics_do_not_become_fills() {
        let shared = SharedState::new();
        let original = captured_manual_rows().remove(1);
        for tag in [1, 11, 17, 20, 150, 39, 6008, 32, 14, 38, 31, 6, 54, 60] {
            let mut fields = original.clone();
            fields.remove(&tag);
            assert!(
                project(&field_view(&fields), &shared, "DU123456").is_err(),
                "missing {tag}"
            );
        }
        for (tag, value) in [
            (6809, "100"),
            (6809, "-2"),
            (6809, "oops"),
            (20, "3"),
            (150, "0"),
            (39, "0"),
            (32, "0"),
            (31, "NaN"),
            (14, "0"),
            (38, "0"),
        ] {
            let mut fields = original.clone();
            fields.insert(tag, value.into());
            assert!(
                project(&field_view(&fields), &shared, "DU123456").is_err(),
                "{tag}={value}"
            );
        }
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(shared.orders.drain_fills_with_exec().is_empty());
    }

    #[test]
    fn lifecycle_currency_is_reported_not_inferred_from_cached_contract() {
        let shared = SharedState::new();
        shared.reference.cache_contract(
            922376799,
            crate::api::types::Contract {
                con_id: 922376799,
                currency: "USD".into(),
                sec_type: "OPT".into(),
                ..Default::default()
            },
        );
        for currency in [None, Some("")] {
            let mut row = captured_manual_rows().remove(1);
            row.remove(&15);
            if let Some(currency) = currency {
                row.insert(15, currency.into());
            }
            assert!(project(&field_view(&row), &shared, "DU123456").is_err());
        }
    }

    #[test]
    fn lifecycle_sending_time_alone_does_not_establish_economic_chronology() {
        let shared = SharedState::new();
        let mut fields = captured_manual_rows().remove(0);
        fields.remove(&60);
        fields.insert(52, "20261003-13:00:00".into());
        assert!(project(&field_view(&fields), &shared, "DU123456").is_err());
        fields.insert(6699, "20261003-12:00:00".into());
        let row = project(&field_view(&fields), &shared, "DU123456").unwrap();
        assert_eq!(row.execution.time_unix_seconds, Some(row.time_secs));
        assert_eq!(
            row.time_secs,
            super::super::ccp::fix_utc_to_unix_secs("20261003-12:00:00").unwrap()
        );
    }
}

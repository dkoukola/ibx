//! Native U72 range queries. Historical (8080) rows never enter live accounting.
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::api::types::{CommissionAndFeesReport, ExecutionComponent, ExecutionFilter};
use crate::bridge::{Event, ExecutionHistoryReply, SharedState};
use crate::client_core::{ClientCore, StoredExecution, format_exec_time};
use crate::config::chrono_free_timestamp;
use crate::engine::context::Context;
use crate::protocol::connection::Connection;
use crate::types::{OrderStatus, QTY_SCALE};
use crossbeam_channel::Sender;

use super::HeartbeatState;
use super::ccp::{CcpState, fill_exec_of, perm_id_from_clord_id, split_exec_revision};
use super::report::{ReportProjection, project_report};

const TIMEOUT: Duration = Duration::from_secs(30);

struct Request {
    connection: String,
    req_id: i64,
    start: String,
    end: String,
    start_secs: i64,
    end_secs: i64,
    filter: ExecutionFilter,
    deadline: Instant,
    include_lifecycles: bool,
}

struct Active {
    request: Request,
    wire_id: String,
    rows: BTreeMap<String, StoredExecution>,
    lifecycle_rows: BTreeMap<String, crate::api::types::OptionLifecycleReport>,
    commissions: HashMap<String, CollectedCommission>,
    failure: Option<String>,
    timed_out: bool,
}

struct CollectedCommission {
    report: CommissionAndFeesReport,
    // The legacy fee-marker path permits an absent amount. Lifecycle evidence
    // must distinguish that from a genuinely reported zero commission.
    lifecycle_complete: bool,
}

#[derive(Default)]
pub(super) struct ExecutionRequests {
    waiting: VecDeque<Request>,
    active: Option<Active>,
    next_id: u64,
}

fn reply(shared: &SharedState, request: &Request, result: Result<Vec<StoredExecution>, String>) {
    shared
        .orders
        .push_execution_range_reply(ExecutionHistoryReply {
            connection: request.connection.clone(),
            req_id: request.req_id,
            result,
            lifecycle_reports: Vec::new(),
        });
    shared.notify();
}

impl ExecutionRequests {
    pub(super) fn collect_option_lifecycle(&mut self, fields: &HashMap<u32, String>, shared: &SharedState, account: &str) {
        if !self.active.as_ref().is_some_and(|active| active.request.include_lifecycles) {
            self.reject_option_lifecycle(fields, account);
            return;
        }
        let Some(active) = self.active.as_mut().filter(|active| !active.timed_out) else { return };
        if fields.get(&1).is_some_and(|owner| !owner.is_empty() && owner != account) { return; }
        if let Some(time) = fill_exec_of(fields, fields.get(&17).map(String::as_str).unwrap_or("")).time_secs
            && (time < active.request.start_secs || time > active.request.end_secs) { return; }
        match super::lifecycle_history::project(fields, shared, account) {
            Ok(row) if row.kind == crate::api::types::OptionLifecycleReportKind::EconomicExecution => {
                let (base, revision) = split_exec_revision(&row.execution.exec_id);
                if active.lifecycle_rows.get(base).is_none_or(|old| split_exec_revision(&old.execution.exec_id).1 <= revision) {
                    active.lifecycle_rows.insert(base.to_owned(), row);
                }
            }
            Ok(_) => {} // Validated instruction status is not an execution.
            Err(error) => { active.failure.get_or_insert(error); }
        }
    }

    pub(super) fn queue_including_lifecycles(&mut self, connection: String, req_id: i64, start: String, end: String, filter: ExecutionFilter) -> Result<(), ExecutionHistoryReply> {
        self.queue(connection, req_id, start, end, filter)?;
        self.waiting.back_mut().unwrap().include_lifecycles = true;
        Ok(())
    }
    /// Fail the one active native U72 request when it encounters a passive
    /// option lifecycle report for its authenticated account. U72 rows carry
    /// no row-level request id, so a concurrent own-account lifecycle report
    /// cannot be omitted while still claiming a complete successful result.
    pub(super) fn reject_option_lifecycle(&mut self, fields: &HashMap<u32, String>, account: &str) {
        let Some(active) = self.active.as_mut().filter(|active| !active.timed_out) else {
            return;
        };
        if fields
            .get(&1)
            .is_some_and(|reported| !reported.is_empty() && reported != account)
        {
            return;
        }
        active.failure.get_or_insert_with(|| {
            "Execution history contains unsupported option lifecycle report".into()
        });
    }

    pub(super) fn queue(
        &mut self,
        connection: String,
        req_id: i64,
        start: String,
        end: String,
        filter: ExecutionFilter,
    ) -> Result<(), ExecutionHistoryReply> {
        let bounds = super::ccp::fix_utc_to_unix_secs(&start)
            .zip(super::ccp::fix_utc_to_unix_secs(&end))
            .filter(|_| crate::client_core::valid_completed_history_range(&start, &end));
        let Some((start_secs, end_secs)) = bounds else {
            return Err(ExecutionHistoryReply {
                connection,
                req_id,
                result: Err("Invalid execution history UTC interval".into()),
                lifecycle_reports: Vec::new(),
            });
        };
        self.waiting.push_back(Request {
            connection,
            req_id,
            start,
            end,
            start_secs,
            end_secs,
            filter,
            deadline: Instant::now() + TIMEOUT,
            include_lifecycles: false,
        });
        Ok(())
    }

    pub(super) fn disconnected(&mut self, shared: &SharedState) {
        if let Some(active) = self.active.take()
            && !active.timed_out
        {
            reply(
                shared,
                &active.request,
                Err("Execution history connection lost".into()),
            );
        }
        for request in self.waiting.drain(..) {
            reply(
                shared,
                &request,
                Err("Execution history connection lost".into()),
            );
        }
    }

    /// Consume dated history, in-range possible resends of an active explicit
    /// query, and its exact end. Other live reports continue through accounting.
    pub(super) fn report(
        &mut self,
        fields: &HashMap<u32, String>,
        shared: &SharedState,
        account: &str,
    ) -> bool {
        let historical = fields.get(&8080).is_some_and(|flag| flag.starts_with('1'));
        let Some(active) = self.active.as_mut() else {
            return historical;
        };
        if fields.get(&6556) == Some(&active.wire_id)
            && fields.get(&32).map(String::as_str) == Some("*")
        {
            let mut active = self.active.take().unwrap();
            if !active.timed_out {
                let result = if let Some(failure) = active.failure {
                    Err(failure)
                } else {
                    for (base, row) in &mut active.rows {
                        row.commission_and_fees = active
                            .commissions
                            .remove(base)
                            .map(|fee| fee.report)
                            .filter(|fee| fee.exec_id == row.execution.exec_id);
                    }
                    let rows: Vec<_> = active.rows.into_values().collect();
                    Ok(ClientCore::filter_executions(&rows, &active.request.filter))
                };
                let lifecycle_reports = if result.is_ok() {
                    active.lifecycle_rows.into_iter().filter_map(|(base, mut row)| {
                        row.commission_and_fees = active.commissions.remove(&base)
                            .filter(|fee| fee.lifecycle_complete)
                            .map(|fee| fee.report)
                            .filter(|fee| fee.exec_id == row.execution.exec_id);
                        super::lifecycle_history::matches_filter(&row, &active.request.filter).then_some(row)
                    }).collect()
                } else { Vec::new() };
                shared.orders.push_execution_range_reply(ExecutionHistoryReply {
                    connection: active.request.connection,
                    req_id: active.request.req_id,
                    result,
                    lifecycle_reports,
                });
                shared.notify();
            }
            return true;
        }
        if fields.get(&20).map(String::as_str) == Some("3")
            || fields.get(&1).is_some_and(|value| value != account)
            || !matches!(fields.get(&150).map(String::as_str), Some("F" | "1" | "2"))
        {
            return historical;
        }
        // A real paper U72 reply has 97=Y but neither 8080 nor a row-level
        // request ID. It must not rebook a fill already present in the U75
        // position image. PossResend is not globally historical: apply this
        // only to rows admitted to this explicit query's existing interval.
        let possible_resend = fields.get(&97).map(String::as_str) == Some("Y");
        let mut consumed = historical;
        match project_execution(fields, account, active.request.req_id, shared) {
            Ok(row) => {
                let time = fill_exec_of(fields, &row.execution.exec_id)
                    .time_secs
                    .unwrap();
                if time >= active.request.start_secs
                    && time <= active.request.end_secs
                    && row.execution.acct_number == account
                {
                    consumed |= possible_resend;
                    if active.timed_out {
                        return consumed;
                    }
                    let (base, revision) = split_exec_revision(&row.execution.exec_id);
                    if active
                        .rows
                        .get(base)
                        .is_none_or(|old| split_exec_revision(&old.execution.exec_id).1 <= revision)
                    {
                        active.rows.insert(base.to_owned(), row);
                    }
                }
            }
            Err(message) => {
                if !active.timed_out {
                    active.failure = Some(message);
                }
                consumed |= possible_resend;
            }
        }
        consumed
    }

    pub(super) fn commission(&mut self, fields: &HashMap<u32, String>) {
        let Some(active) = self.active.as_mut().filter(|active| !active.timed_out) else {
            return;
        };
        let Some(exec_id) = fields.get(&17).filter(|id| !id.is_empty()) else {
            return;
        };
        let commission = fields
            .get(&6378)
            .and_then(|value| value.parse::<f64>().ok());
        if commission.is_none() && !fields.contains_key(&8189) {
            return;
        }
        let (base, revision) = split_exec_revision(exec_id);
        if active
            .commissions
            .get(base)
            .is_some_and(|old| split_exec_revision(&old.report.exec_id).1 > revision)
        {
            return;
        }
        active.commissions.insert(
            base.to_owned(),
            CollectedCommission {
                lifecycle_complete: commission.is_some_and(f64::is_finite)
                    && fields.get(&6381).is_some_and(|currency| !currency.is_empty()),
                report: CommissionAndFeesReport {
                exec_id: exec_id.clone(),
                commission_and_fees: commission.unwrap_or(0.0),
                currency: fields.get(&6381).cloned().unwrap_or_default(),
                realized_pnl: fields
                    .get(&6099)
                    .and_then(|value| value.parse().ok())
                    .filter(|value| *value != 0.0)
                    .unwrap_or(f64::MAX),
                yield_amount: fields
                    .get(&236)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(f64::MAX),
                yield_redemption_date: fields
                    .get(&696)
                    .filter(|value| value.len() == 8)
                    .cloned()
                    .unwrap_or_default(),
                },
            },
        );
    }
}

fn project_execution(
    fields: &HashMap<u32, String>,
    account: &str,
    req_id: i64,
    shared: &SharedState,
) -> Result<StoredExecution, String> {
    let id = fields
        .get(&17)
        .filter(|id| !id.is_empty())
        .ok_or("Execution history row has no execution ID")?;
    let metadata = fill_exec_of(fields, id);
    let time = metadata
        .time_secs
        .ok_or("Execution history row has no valid UTC time")?;
    let side = match fields.get(&54).map(String::as_str) {
        Some("1") => "BOT",
        Some("2" | "5") => "SLD",
        _ => return Err("Execution history row has no valid side".into()),
    };
    let perm_id = fields.get(&11).map_or(0, |id| perm_id_from_clord_id(id));
    let order_id = fields
        .get(&6121)
        .and_then(|id| id.parse().ok())
        .filter(|id| *id != 0 && *id != i32::MAX as i64)
        .unwrap_or(perm_id);
    let projected = project_report(
        fields,
        ReportProjection {
            order_id,
            parent_id: 0,
            status: OrderStatus::Filled,
            account_id: account,
            fallback_order: None,
            fallback_con_id: 0,
            cached_contract: fields.get(&6008).and_then(|id| id.parse().ok())
                .and_then(|con_id| shared.reference.get_contract(con_id)),
            combo: None,
            trail_limit: None,
            combo_leg_prices: Vec::new(),
        },
    );
    let mut execution = projected.last_exec;
    execution.perm_id = perm_id;
    execution.shares /= QTY_SCALE as f64;
    execution.acct_number = projected.order.account;
    execution.side = side.into();
    execution.time = format_exec_time(time);
    execution.exchange = metadata.exchange;
    execution.client_id = metadata.client_id;
    execution.model_code = metadata.model_code;
    execution.order_ref = metadata.order_ref;
    execution.component = metadata.component;
    let reported_price = fields
        .get(&31)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite());
    let combo_price_is_valid = match &execution.component {
        ExecutionComponent::Ordinary => reported_price.is_some_and(|price| price > 0.0),
        ExecutionComponent::ComboParent { .. } if projected.contract.sec_type == "BAG" => {
            reported_price.is_some()
        }
        ExecutionComponent::ComboLeg { .. } if projected.contract.sec_type != "BAG" => {
            reported_price.is_some_and(|price| price >= 0.0)
        }
        ExecutionComponent::ComboParent { .. }
        | ExecutionComponent::ComboLeg { .. }
        | ExecutionComponent::UnsupportedCombo { .. } => {
            return Err("Execution history contains unsupported combo component metadata".into());
        }
    };
    if matches!(execution.component, ExecutionComponent::Ordinary)
        && (projected.contract.sec_type == "BAG" || !projected.contract.combo_legs.is_empty())
    {
        return Err("Execution history contains unsupported combo component metadata".into());
    }
    if perm_id <= 0
        || projected.contract.con_id <= 0
        || !execution.shares.is_finite()
        || (execution.shares <= 0.0 && !super::ccp::is_execution_print(fields))
        || !execution.price.is_finite()
        || !combo_price_is_valid
    {
        return Err("Incomplete execution history identity, quantity or price".into());
    }
    Ok(StoredExecution {
        req_id,
        contract: projected.contract,
        execution,
        time_secs: Some(time),
        commission_and_fees: None,
    })
}

impl CcpState {
    pub(super) fn progress_execution_history(
        &mut self,
        conn: &mut Option<Connection>,
        context: &mut Context,
        shared: &SharedState,
        events: &Option<Sender<Event>>,
        hb: &mut HeartbeatState,
        account: &str,
    ) {
        let now = Instant::now();
        if let Some(active) = self.execution_ranges.active.as_mut()
            && !active.timed_out
            && now >= active.request.deadline
        {
            active.timed_out = true;
            active.rows.clear();
            active.lifecycle_rows.clear();
            active.commissions.clear();
            reply(
                shared,
                &active.request,
                Err("Execution history timed out; awaiting its wire end".into()),
            );
        }
        while self
            .execution_ranges
            .waiting
            .front()
            .is_some_and(|request| now >= request.deadline)
        {
            let request = self.execution_ranges.waiting.pop_front().unwrap();
            reply(
                shared,
                &request,
                Err("Execution history could not start before its deadline".into()),
            );
        }
        if self.disconnected
            || self.execution_ranges.active.is_some()
            || shared.orders.execution_history_completion().is_none()
        {
            return;
        }
        let Some(socket) = conn.as_mut() else { return };
        let Some(request) = self.execution_ranges.waiting.pop_front() else {
            return;
        };
        if !shared.orders.execution_history_matches(&request.connection) {
            reply(
                shared,
                &request,
                Err("Execution history connection changed before send".into()),
            );
            return;
        }
        self.execution_ranges.next_id += 1;
        let wire_id = format!("IBX.Executions.{}", self.execution_ranges.next_id);
        let timestamp = chrono_free_timestamp();
        // jfix.cs.a(StringBuilder): account selector also supplies 6539.
        let fields = [
            (35, "U"),
            (52, &timestamp),
            (6040, "72"),
            (1, account),
            (6539, request.start.as_str()),
            (6536, request.start.as_str()),
            (6537, request.end.as_str()),
            (6556, wire_id.as_str()),
        ];
        if socket.send_fix(&fields).is_err() {
            reply(
                shared,
                &request,
                Err("Failed to send execution history request".into()),
            );
            self.handle_disconnect(context, shared, events);
            return;
        }
        hb.last_ccp_sent = now;
        self.execution_ranges.active = Some(Active {
            request,
            wire_id,
            rows: BTreeMap::new(),
            lifecycle_rows: BTreeMap::new(),
            commissions: HashMap::new(),
            failure: None,
            timed_out: false,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::fix;
    use crate::types::{Order, PRICE_SCALE, Side};

    fn active() -> ExecutionRequests {
        let mut requests = ExecutionRequests::default();
        requests
            .queue(
                "today4".into(),
                7,
                "20261001-00:00:00".into(),
                "20261005-12:00:00".into(),
                ExecutionFilter::default(),
            )
            .unwrap();
        requests.active = Some(Active {
            request: requests.waiting.pop_front().unwrap(),
            wire_id: "range1".into(),
            rows: BTreeMap::new(),
            lifecycle_rows: BTreeMap::new(),
            commissions: HashMap::new(),
            failure: None,
            timed_out: false,
        });
        requests
    }

    fn row(exec_id: &str, historical: bool) -> HashMap<u32, String> {
        [
            (35, "8"),
            (11, "42.0"),
            (6121, "42"),
            (17, exec_id),
            (150, "1"),
            (39, "1"),
            (20, "0"),
            (8080, if historical { "1" } else { "0" }),
            (1, "DU123456"),
            (6008, "265598"),
            (55, "AAPL"),
            (167, "CS"),
            (15, "USD"),
            (54, "1"),
            (32, "0.5"),
            (31, "100"),
            (14, "0.5"),
            (151, "2.5"),
            (38, "3"),
            (6, "100"),
            (40, "2"),
            (44, "100"),
            (59, "1"),
            (6010, "fixture-ref"),
            (100, "BEST"),
            (60, "20261003-12:00:00"),
            (6119, "0"),
        ]
        .into_iter()
        .map(|(tag, value)| (tag, value.into()))
        .collect()
    }

    #[test]
    fn option_identity_in_execution_history_comes_only_from_its_exact_cached_contract() {
        let contract = crate::api::types::Contract {
            con_id: 9, symbol: "AAPL".into(), sec_type: "OPT".into(), currency: "USD".into(),
            last_trade_date_or_contract_month: "20261014".into(), last_trade_date: "20261014".into(),
            strike: 332.5, right: "C".into(), multiplier: "100".into(), ..Default::default()
        };
        for cached in [false, true] {
            let shared = SharedState::new();
            shared.reference.cache_contract(10, crate::api::types::Contract { con_id: 10, ..contract.clone() });
            if cached {
                shared.reference.cache_contract(9, crate::api::types::Contract { con_id: 9, ..Default::default() });
                shared.reference.cache_contract(9, contract.clone());
            }
            let mut fields = row("option.01", true);
            fields.insert(6008, "9".into());
            fields.insert(167, "OPT".into());
            fields.insert(15, "EUR".into());
            // No new claim about option fields in 35=8: identity was already
            // decoded from a separate definition for this exact conId.
            let mut requests = active();
            assert!(requests.report(&fields, &shared, "DU123456"));
            let result = requests.active.as_ref().unwrap().rows.values().next().unwrap();
            let expected = if cached {
                crate::api::types::Contract { currency: "EUR".into(), ..contract.clone() }
            } else {
                crate::api::types::Contract {
                    con_id: 9, symbol: "AAPL".into(), sec_type: "OPT".into(), currency: "EUR".into(),
                    ..Default::default()
                }
            };
            assert_eq!(result.contract, expected);
            assert_eq!(result.execution.shares, 0.5, "identity retention does not rescale quantity");
            assert_eq!(result.execution.price, 100.0, "identity retention does not rescale price");
            assert!(shared.portfolio.position_infos().is_empty(), "history remains read-only");
        }
    }

    #[test]
    fn execution_range_raw_command_rejects_invalid_bounds_before_queueing() {
        let mut requests = ExecutionRequests::default();
        for (start, end) in [
            ("not-a-date", "20261005-12:00:00"),
            ("20261005-12:00:01", "20261005-12:00:00"),
            ("20260230-00:00:00", "20261005-12:00:00"),
        ] {
            let error = requests
                .queue(
                    "today4".into(),
                    7,
                    start.into(),
                    end.into(),
                    ExecutionFilter::default(),
                )
                .unwrap_err();
            assert_eq!(error.req_id, 7);
            assert_eq!(error.connection, "today4");
            assert!(error.result.is_err());
            assert!(requests.waiting.is_empty());
        }
    }

    #[test]
    fn option_lifecycle_fails_only_the_active_own_or_unattributed_u72_query() {
        for (reported_account, expect_failure) in [
            (None, true),
            (Some(""), true),
            (Some("DU123456"), true),
            (Some("OTHER"), false),
        ] {
            let shared = SharedState::new();
            let mut requests = active();
            let mut lifecycle = HashMap::from([(6809, "100".into())]);
            if let Some(reported_account) = reported_account {
                lifecycle.insert(1, reported_account.into());
            }
            requests.reject_option_lifecycle(&lifecycle, "DU123456");
            let end = HashMap::from([(6556, "range1".into()), (32, "*".into())]);
            assert!(requests.report(&end, &shared, "DU123456"));
            let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
            match (reply.result, expect_failure) {
                (Err(message), true) => assert_eq!(
                    message,
                    "Execution history contains unsupported option lifecycle report"
                ),
                (Ok(rows), false) => assert!(rows.is_empty()),
                (result, expected) => panic!("unexpected result {result:?}, failure={expected}"),
            }
        }
    }

    #[test]
    fn option_lifecycle_does_not_emit_a_second_reply_after_u72_timeout() {
        let shared = SharedState::new();
        let mut requests = active();
        requests.active.as_mut().unwrap().timed_out = true;
        requests.reject_option_lifecycle(
            &HashMap::from([(6809, "2".into()), (1, "DU123456".into())]),
            "DU123456",
        );
        let end = HashMap::from([(6556, "range1".into()), (32, "*".into())]);
        assert!(requests.report(&end, &shared, "DU123456"));
        assert!(shared.orders.drain_execution_range_replies().is_empty());
    }

    #[test]
    fn inclusive_lifecycle_range_keeps_economics_corrections_fees_and_exact_completion() {
        use super::super::lifecycle_history::{captured_instruction, captured_manual_rows};
        let shared = SharedState::new();
        let mut requests = active();
        requests.active.as_mut().unwrap().request.include_lifecycles = true;
        for row in captured_manual_rows() {
            requests.collect_option_lifecycle(&row, &shared, "DU123456");
            requests.collect_option_lifecycle(&row, &shared, "DU123456");
        }
        for pending in [false, true] {
            requests.collect_option_lifecycle(&captured_instruction(pending), &shared, "DU123456");
        }
        let mut corrected = captured_manual_rows().remove(0);
        corrected.insert(17, "call.01.02".into());
        corrected.insert(31, "11.01".into());
        requests.collect_option_lifecycle(&corrected, &shared, "DU123456");
        requests.collect_option_lifecycle(&captured_manual_rows()[0], &shared, "DU123456");
        for exec in ["call.01.01", "call.01.02"] {
            requests.commission(&[(17, exec), (6378, "0.25"), (6381, "USD")]
                .into_iter().map(|(tag, value)| (tag, value.into())).collect());
        }
        assert!(!requests.report(&HashMap::from([(6556, "other".into()), (32, "*".into())]), &shared, "DU123456"));
        assert!(shared.orders.drain_execution_range_replies().is_empty());
        assert!(requests.report(&HashMap::from([(6556, "range1".into()), (32, "*".into())]), &shared, "DU123456"));
        let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
        assert!(reply.result.unwrap().is_empty());
        assert_eq!(reply.lifecycle_reports.len(), 4);
        let corrected = reply.lifecycle_reports.iter().find(|row| row.execution.exec_id == "call.01.02").unwrap();
        assert_eq!(corrected.execution.price, 11.01);
        assert_eq!(corrected.commission_and_fees.as_ref().unwrap().exec_id, "call.01.02");
        assert_eq!(reply.lifecycle_reports.iter().filter(|row| row.commission_and_fees.is_some()).count(), 1);
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(shared.orders.drain_fills_with_exec().is_empty());
    }

    #[test]
    fn inclusive_lifecycle_range_filters_and_rejects_unknown_or_missing_semantics() {
        use super::super::lifecycle_history::captured_manual_rows;
        for include in [false, true] {
            for malformed in [false, true] {
                let shared = SharedState::new();
                let mut requests = active();
                requests.active.as_mut().unwrap().request.include_lifecycles = include;
                let mut row = captured_manual_rows().remove(1);
                if malformed { row.remove(&31); }
                requests.collect_option_lifecycle(&row, &shared, "DU123456");
                requests.report(&HashMap::from([(6556, "range1".into()), (32, "*".into())]), &shared, "DU123456");
                let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
                assert_eq!(reply.result.is_ok(), include && !malformed);
                if reply.result.is_err() { assert!(reply.lifecycle_reports.is_empty()); }
            }
        }
        let shared = SharedState::new();
        let mut requests = active();
        requests.active.as_mut().unwrap().request.include_lifecycles = true;
        requests.active.as_mut().unwrap().request.filter.sec_type = "OPT".into();
        for row in captured_manual_rows() {
            requests.collect_option_lifecycle(&row, &shared, "DU123456");
        }
        for (tag, value) in [(1, "OTHER"), (60, "20261006-12:00:00")] {
            let mut row = captured_manual_rows().remove(0);
            row.insert(tag, value.into());
            row.insert(6809, "unknown".into());
            requests.collect_option_lifecycle(&row, &shared, "DU123456");
        }
        requests.report(&HashMap::from([(6556, "range1".into()), (32, "*".into())]), &shared, "DU123456");
        let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
        assert!(reply.result.is_ok());
        assert_eq!(reply.lifecycle_reports.len(), 2);
        assert!(reply.lifecycle_reports.iter().all(|row| row.contract.sec_type == "OPT"));
        let mut requests = active();
        requests.active.as_mut().unwrap().request.include_lifecycles = true;
        let mut row = captured_manual_rows().remove(0);
        row.insert(6809, "101".into());
        requests.collect_option_lifecycle(&row, &shared, "DU123456");
        requests.report(&HashMap::from([(6556, "range1".into()), (32, "*".into())]), &shared, "DU123456");
        assert!(shared.orders.drain_execution_range_replies().pop().unwrap().result.is_err());
    }

    #[test]
    fn inclusive_lifecycle_fee_markers_never_fabricate_zero_or_nonfinite_fees() {
        use super::super::lifecycle_history::captured_manual_rows;
        for (amount, currency, expected) in [
            (None, Some("USD"), None),
            (Some("bad"), Some("USD"), None),
            (Some("NaN"), Some("USD"), None),
            (Some("inf"), Some("USD"), None),
            (Some("0"), None, None),
            (Some("0"), Some(""), None),
            (Some("0"), Some("USD"), Some(0.0)),
            (Some("0.25"), Some("USD"), Some(0.25)),
        ] {
            let shared = SharedState::new();
            let mut requests = active();
            requests.active.as_mut().unwrap().request.include_lifecycles = true;
            requests.collect_option_lifecycle(&captured_manual_rows()[0], &shared, "DU123456");
            let mut fee = HashMap::from([(17, "call.01.01".into()), (8189, "1".into())]);
            if let Some(amount) = amount { fee.insert(6378, amount.into()); }
            if let Some(currency) = currency { fee.insert(6381, currency.into()); }
            requests.commission(&fee);
            requests.report(&HashMap::from([(6556, "range1".into()), (32, "*".into())]), &shared, "DU123456");
            let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
            assert!(reply.result.is_ok());
            assert_eq!(reply.lifecycle_reports[0].commission_and_fees.as_ref().map(|fee| fee.commission_and_fees), expected);
        }
    }

    #[test]
    fn inclusive_lifecycle_replay_routes_through_ccp_without_booking_or_cached_success() {
        use super::super::lifecycle_history::captured_manual_rows;
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        for row in captured_manual_rows() {
            deliver(&mut ccp, &mut context, &shared, &row);
        }
        assert!(shared.orders.drain_execution_range_replies().is_empty());
        assert_eq!(shared.orders.drain_option_lifecycle_observations().len(), 4);
        for _ in 0..2 {
            ccp.execution_ranges = active();
            ccp.execution_ranges.active.as_mut().unwrap().request.include_lifecycles = true;
            for row in captured_manual_rows() {
                deliver(&mut ccp, &mut context, &shared, &row);
                deliver(&mut ccp, &mut context, &shared, &row);
            }
            end(&mut ccp, &mut context, &shared);
            let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
            assert!(reply.result.unwrap().is_empty());
            assert_eq!(reply.lifecycle_reports.len(), 4);
            assert!(shared.portfolio.position_infos().is_empty());
            assert!(shared.orders.drain_fills_with_exec().is_empty());
            assert!(shared.orders.drain_untracked_executions().is_empty());
            assert!(shared.portfolio.money_since_seed(&shared.reference).is_empty());
        }
    }

    #[test]
    fn inclusive_lifecycle_connection_loss_does_not_deliver_partial_success() {
        use super::super::lifecycle_history::captured_manual_rows;
        let shared = SharedState::new();
        let mut requests = active();
        requests.active.as_mut().unwrap().request.include_lifecycles = true;
        requests.collect_option_lifecycle(&captured_manual_rows()[0], &shared, "DU123456");
        requests.disconnected(&shared);
        let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
        assert!(reply.result.is_err());
        assert!(reply.lifecycle_reports.is_empty());
        requests.report(&HashMap::from([(6556, "range1".into()), (32, "*".into())]), &shared, "DU123456");
        assert!(shared.orders.drain_execution_range_replies().is_empty());
    }

    #[test]
    fn execution_range_preserves_complete_combo_parent_and_leg_identity() {
        let shared = SharedState::new();
        let mut requests = active();
        let mut parent = row("package-parent.01", true);
        parent.insert(6008, "28812380".into());
        parent.insert(167, "BAG".into());
        parent.insert(442, "3".into());
        parent.insert(6013, "-1:2".into());
        parent.insert(31, "-0.05".into());
        let mut leg = row("package-leg.01", true);
        leg.insert(6008, "756733".into());
        leg.insert(167, "OPT".into());
        leg.insert(442, "2".into());
        leg.insert(6013, "0:2".into());
        leg.insert(6400, "package-parent.01".into());
        leg.insert(31, "0".into());
        assert!(requests.report(&parent, &shared, "DU123456"));
        assert!(requests.report(&leg, &shared, "DU123456"));
        let end = HashMap::from([(6556, "range1".into()), (32, "*".into())]);
        assert!(requests.report(&end, &shared, "DU123456"));
        let rows = shared
            .orders
            .drain_execution_range_replies()
            .pop()
            .unwrap()
            .result
            .unwrap();
        assert_eq!(rows.len(), 2);
        let parent = rows
            .iter()
            .find(|row| row.execution.exec_id == "package-parent.01")
            .unwrap();
        assert_eq!(
            parent.execution.component,
            ExecutionComponent::ComboParent { leg_count: 2 }
        );
        assert_eq!(parent.execution.price, -0.05);
        let leg = rows
            .iter()
            .find(|row| row.execution.exec_id == "package-leg.01")
            .unwrap();
        assert_eq!(
            leg.execution.component,
            ExecutionComponent::ComboLeg {
                index: 0,
                leg_count: 2,
                parent_execution_id: "package-parent.01".into(),
            }
        );
        assert_eq!(leg.execution.price, 0.0);
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(
            shared
                .portfolio
                .money_since_seed(&shared.reference)
                .is_empty()
        );
    }

    #[test]
    fn execution_range_fails_instead_of_partially_returning_ambiguous_combo_rows() {
        let mut cases = Vec::new();

        let mut missing_parent_descriptor = row("ambiguous-parent.01", true);
        missing_parent_descriptor.insert(167, "BAG".into());
        missing_parent_descriptor.insert(442, "3".into());
        cases.push(missing_parent_descriptor);

        let mut parent_without_role = row("untyped-parent.01", true);
        parent_without_role.insert(167, "BAG".into());
        parent_without_role.insert(6013, "-1:2".into());
        cases.push(parent_without_role);

        let mut parent_with_leg_link = row("linked-parent.01", true);
        parent_with_leg_link.insert(167, "BAG".into());
        parent_with_leg_link.insert(442, "3".into());
        parent_with_leg_link.insert(6013, "-1:2".into());
        parent_with_leg_link.insert(6400, "unexpected".into());
        cases.push(parent_with_leg_link);

        let mut leg_without_parent = row("orphan-leg.01", true);
        leg_without_parent.insert(167, "OPT".into());
        leg_without_parent.insert(442, "2".into());
        leg_without_parent.insert(6013, "0:2".into());
        cases.push(leg_without_parent);

        let mut mismatched_leg_role = row("mismatched-leg.01", true);
        mismatched_leg_role.insert(167, "OPT".into());
        mismatched_leg_role.insert(442, "3".into());
        mismatched_leg_role.insert(6013, "0:2".into());
        mismatched_leg_role.insert(6400, "parent.01".into());
        cases.push(mismatched_leg_role);

        let mut bag_claiming_leg = row("bag-leg.01", true);
        bag_claiming_leg.insert(167, "BAG".into());
        bag_claiming_leg.insert(442, "2".into());
        bag_claiming_leg.insert(6013, "0:2".into());
        bag_claiming_leg.insert(6400, "parent.01".into());
        cases.push(bag_claiming_leg);

        let mut stock_claiming_parent = row("stock-parent.01", true);
        stock_claiming_parent.insert(442, "3".into());
        stock_claiming_parent.insert(6013, "-1:2".into());
        cases.push(stock_claiming_parent);

        let mut ordinary_bag = row("ordinary-bag.01", true);
        ordinary_bag.insert(167, "BAG".into());
        cases.push(ordinary_bag);

        for ambiguous in cases {
            let shared = SharedState::new();
            let mut requests = active();
            assert!(requests.report(&row("valid-first.01", true), &shared, "DU123456"));
            assert!(requests.report(&ambiguous, &shared, "DU123456"));
            let end = HashMap::from([(6556, "range1".into()), (32, "*".into())]);
            assert!(requests.report(&end, &shared, "DU123456"));
            assert_eq!(
                shared
                    .orders
                    .drain_execution_range_replies()
                    .pop()
                    .unwrap()
                    .result
                    .unwrap_err(),
                "Execution history contains unsupported combo component metadata"
            );
        }

        for price in [None, Some("not-a-price")] {
            let shared = SharedState::new();
            let mut requests = active();
            let mut missing_price = row("missing-price.01", true);
            missing_price.insert(167, "BAG".into());
            missing_price.insert(442, "3".into());
            missing_price.insert(6013, "-1:2".into());
            missing_price.remove(&31);
            if let Some(price) = price {
                missing_price.insert(31, price.into());
            }
            assert!(requests.report(&missing_price, &shared, "DU123456"));
            let end = HashMap::from([(6556, "range1".into()), (32, "*".into())]);
            assert!(requests.report(&end, &shared, "DU123456"));
            assert_eq!(
                shared
                    .orders
                    .drain_execution_range_replies()
                    .pop()
                    .unwrap()
                    .result
                    .unwrap_err(),
                "Incomplete execution history identity, quantity or price"
            );
        }
    }

    fn deliver(
        ccp: &mut CcpState,
        context: &mut Context,
        shared: &SharedState,
        fields: &HashMap<u32, String>,
    ) {
        let fields: Vec<_> = fields
            .iter()
            .map(|(tag, value)| (*tag, value.as_str()))
            .collect();
        ccp.process_ccp_message(
            &fix::fix_build(&fields, 1),
            &mut None,
            context,
            shared,
            &None,
            &mut HeartbeatState::new(),
            "DU123456",
        );
    }

    fn end(ccp: &mut CcpState, context: &mut Context, shared: &SharedState) {
        deliver(
            ccp,
            context,
            shared,
            &[(35, "8"), (6556, "range1"), (32, "*")]
                .into_iter()
                .map(|(tag, value)| (tag, value.into()))
                .collect(),
        );
    }

    fn captured_paper_replay() -> Vec<Vec<(u32, String)>> {
        include_str!("../../../tests/fixtures/execution_history/paper_u72_replayed_fill.jsonl")
            .lines()
            .map(|line| {
                let row: serde_json::Value = serde_json::from_str(line).unwrap();
                serde_json::from_value(row["fields"].clone()).unwrap()
            })
            .collect()
    }

    fn deliver_captured(
        ccp: &mut CcpState,
        context: &mut Context,
        shared: &SharedState,
        fields: &[(u32, String)],
    ) {
        let fields: Vec<_> = fields.iter().map(|(tag, value)| (*tag, value.as_str())).collect();
        ccp.process_ccp_message(&fix::fix_build(&fields, 1), &mut None, context, shared,
            &None, &mut HeartbeatState::new(), "DU123456");
    }

    #[test]
    fn execution_range_captured_replay_does_not_double_the_broker_position() {
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        let instrument = context.register_instrument(265598);
        let capture = captured_paper_replay();
        for image in &capture[..2] {
            deliver_captured(&mut ccp, &mut context, &shared, image);
        }
        assert_eq!(shared.portfolio.position_info(265598).unwrap().position_fixed, QTY_SCALE);
        for _ in 0..2 {
            let mut request = active();
            let end = "20261006-04:00:00";
            request.active.as_mut().unwrap().request.end = end.into();
            request.active.as_mut().unwrap().request.end_secs =
                super::super::ccp::fix_utc_to_unix_secs(end).unwrap();
            ccp.execution_ranges = request;
            for message in &capture[2..] {
                deliver_captured(&mut ccp, &mut context, &shared, message);
            }
            let rows = shared.orders.drain_execution_range_replies().pop().unwrap().result.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].execution.shares, 1.0);
            assert_eq!(rows[0].execution.order_ref, "paper-execution-history-fixture");
            assert_eq!(shared.portfolio.position_info(265598).unwrap().position_fixed, QTY_SCALE);
            assert_eq!(context.position_fixed(instrument), QTY_SCALE);
            assert!(shared.orders.drain_fills_with_exec().is_empty());
            assert!(shared.orders.drain_untracked_executions().is_empty());
            assert!(shared.portfolio.money_since_seed(&shared.reference).is_empty());
            assert!(shared.portfolio.realized_since_seed().is_empty());
            assert!(ccp.last_exec.is_none());
        }
    }

    #[test]
    fn execution_range_keeps_live_fills_and_out_of_range_resends_in_accounting() {
        for replay_flag in [None, Some("N"), Some("Y")] {
            let mut ccp = CcpState::new();
            let mut context = Context::new();
            let shared = SharedState::new();
            let instrument = context.register_instrument(265598);
            ccp.execution_ranges = active();
            let mut live = row("live.01", false);
            if let Some(flag) = replay_flag {
                live.insert(97, flag.into());
            }
            if replay_flag == Some("Y") {
                live.insert(60, "20261005-12:00:01".into());
            }
            deliver(&mut ccp, &mut context, &shared, &live);
            assert_eq!(context.position_fixed(instrument), QTY_SCALE / 2);
            assert_eq!(shared.portfolio.position_info(265598).unwrap().position_fixed, QTY_SCALE / 2);
            assert_eq!(shared.portfolio.money_since_seed(&shared.reference).get(&265598), Some(&Some(-50.0)));
            assert_eq!(shared.orders.drain_fills_with_exec().len(), 1);
            assert!(shared.orders.drain_untracked_executions().is_empty());
            end(&mut ccp, &mut context, &shared);
            let rows = shared.orders.drain_execution_range_replies().pop().unwrap().result.unwrap();
            assert_eq!(rows.len(), usize::from(replay_flag != Some("Y")));
        }
    }

    #[test]
    fn execution_range_resend_is_not_global_and_does_not_consume_status_or_other_accounts() {
        let shared = SharedState::new();
        let mut replay = row("replay.01", false);
        replay.insert(97, "Y".into());
        let mut inactive = ExecutionRequests::default();
        assert!(!inactive.report(&replay, &shared, "DU123456"));
        for (tag, value) in [(20, "3"), (1, "DU654321")] {
            let mut request = active();
            let mut unrelated = replay.clone();
            unrelated.insert(tag, value.into());
            unrelated.remove(&6008);
            assert!(!request.report(&unrelated, &shared, "DU123456"));
            assert!(request.active.as_ref().unwrap().failure.is_none());
            assert!(request.active.as_ref().unwrap().rows.is_empty());
        }
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let instrument = context.register_instrument(265598);
        deliver(&mut ccp, &mut context, &shared, &replay);
        assert_eq!(context.position_fixed(instrument), QTY_SCALE / 2);
        assert_eq!(shared.orders.drain_fills_with_exec().len(), 1);
        assert!(shared.orders.drain_untracked_executions().is_empty());
    }

    #[test]
    fn execution_range_resends_keep_partial_fill_corrections_without_live_accounting() {
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        ccp.execution_ranges = active();
        for (execution_id, shares) in [("partial.01", "0.5"), ("partial.02", "0.75"), ("partial.01", "0.5")] {
            let mut replay = row(execution_id, false);
            replay.insert(97, "Y".into());
            replay.insert(32, shares.into());
            replay.insert(14, shares.into());
            deliver(&mut ccp, &mut context, &shared, &replay);
        }
        end(&mut ccp, &mut context, &shared);
        let rows = shared.orders.drain_execution_range_replies().pop().unwrap().result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].execution.exec_id, "partial.02");
        assert_eq!(rows[0].execution.shares, 0.75);
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(shared.portfolio.money_since_seed(&shared.reference).is_empty());
        assert!(shared.orders.drain_untracked_executions().is_empty());
    }

    #[test]
    fn execution_range_keeps_zero_bust_as_latest_revision_without_live_accounting() {
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        ccp.execution_ranges = active();
        let first = row("busted.01", true);
        let mut bust = row("busted.02", true);
        bust.insert(20, "1".into());
        bust.insert(19, "busted.01".into());
        bust.insert(32, "0".into());
        bust.insert(14, "0".into());
        for row in [&first, &bust, &first] {
            deliver(&mut ccp, &mut context, &shared, row);
        }
        end(&mut ccp, &mut context, &shared);
        let rows = shared.orders.drain_execution_range_replies().pop().unwrap().result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].execution.exec_id, "busted.02");
        assert_eq!(rows[0].execution.shares, 0.0);
        assert!(ccp.exec_realized.is_empty());
        assert!(shared.portfolio.position_infos().is_empty());
        assert!(shared.portfolio.money_since_seed(&shared.reference).is_empty());
    }

    #[test]
    fn execution_range_timeout_quarantines_in_range_resends_until_exact_end() {
        let shared = SharedState::new();
        let mut request = active();
        request.active.as_mut().unwrap().timed_out = true;
        let mut replay = row("replay.01", false);
        replay.insert(97, "Y".into());
        assert!(request.report(&replay, &shared, "DU123456"));
        assert!(request.active.as_ref().unwrap().rows.is_empty());
        let mut marker = HashMap::from([(32, "*".into()), (6556, "wrong".into())]);
        assert!(!request.report(&marker, &shared, "DU123456"));
        assert!(request.active.is_some());
        marker.insert(6556, "range1".into());
        assert!(request.report(&marker, &shared, "DU123456"));
        assert!(request.active.is_none());
        assert!(shared.orders.drain_execution_range_replies().is_empty());
        assert!(!request.report(&replay, &shared, "DU123456"));
    }

    #[test]
    fn execution_range_historical_rows_are_query_local_and_live_prints_coexist() {
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        let instrument = context.register_instrument(265598);
        context.insert_order(Order::new(
            42,
            instrument,
            Side::Buy,
            3,
            100 * PRICE_SCALE,
            b'2',
            b'1',
            0,
        ));
        ccp.execution_ranges = active();
        deliver(&mut ccp, &mut context, &shared, &row("old.01", true));
        assert_eq!(context.position_fixed(instrument), 0);
        assert!(shared.orders.get_order_info(42).is_none());
        assert!(shared.orders.drain_fills_with_exec().is_empty());
        let mut live = row("live.01", false);
        live.insert(60, "20261005-11:59:00".into());
        deliver(&mut ccp, &mut context, &shared, &live);
        assert_eq!(context.position_fixed(instrument), QTY_SCALE / 2);
        assert_eq!(shared.orders.drain_fills_with_exec().len(), 1);
        let mut corrected = row("old.02", true);
        corrected.insert(32, "0.75".into());
        deliver(&mut ccp, &mut context, &shared, &corrected);
        deliver(&mut ccp, &mut context, &shared, &row("old.01", true));
        assert_eq!(context.position_fixed(instrument), QTY_SCALE / 2);
        assert!(shared.orders.drain_fills_with_exec().is_empty());
        end(&mut ccp, &mut context, &shared);
        let result = shared.orders.drain_execution_range_replies().pop().unwrap();
        assert_eq!(result.req_id, 7);
        let rows = result.result.unwrap();
        assert_eq!(rows.len(), 2);
        let old = rows
            .iter()
            .find(|row| row.execution.exec_id == "old.02")
            .unwrap();
        assert_eq!(old.execution.shares, 0.75);
        assert_eq!(old.execution.order_ref, "fixture-ref");
        assert_eq!(old.execution.perm_id, 42);
        assert_eq!(old.execution.exchange, "SMART");
        assert_eq!(old.contract.sec_type, "STK");
    }

    #[test]
    fn execution_range_order_ref_comes_from_the_fresh_row_not_the_live_cache() {
        for reference in [None, Some(""), Some("history-reference")] {
            let mut ccp = CcpState::new();
            let mut context = Context::new();
            let shared = SharedState::new();
            deliver(&mut ccp, &mut context, &shared, &row("live.01", false));
            assert_eq!(shared.orders.get_order_info(42).unwrap().order.order_ref, "fixture-ref");
            ccp.execution_ranges = active();
            let mut historical = row("history.01", true);
            historical.remove(&6010);
            if let Some(reference) = reference {
                historical.insert(6010, reference.into());
            }
            deliver(&mut ccp, &mut context, &shared, &historical);
            end(&mut ccp, &mut context, &shared);
            let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
            let rows = reply.result.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].execution.order_ref, reference.unwrap_or_default());
        }
    }

    #[test]
    fn execution_range_exact_end_and_fresh_empty_answer() {
        let shared = SharedState::new();
        let mut requests = active();
        for fields in [[(6556, "wrong"), (32, "*")], [(6556, "range1"), (32, "1")]] {
            requests.report(
                &fields
                    .into_iter()
                    .map(|(tag, value)| (tag, value.into()))
                    .collect(),
                &shared,
                "DU123456",
            );
            assert!(shared.orders.drain_execution_range_replies().is_empty());
        }
        let fields = [(6556, "range1"), (32, "*")]
            .into_iter()
            .map(|(tag, value)| (tag, value.into()))
            .collect();
        assert!(requests.report(&fields, &shared, "DU123456"));
        assert!(
            shared
                .orders
                .drain_execution_range_replies()
                .pop()
                .unwrap()
                .result
                .unwrap()
                .is_empty()
        );
        requests.report(&fields, &shared, "DU123456");
        assert!(shared.orders.drain_execution_range_replies().is_empty());
    }

    #[test]
    fn execution_range_commissions_and_filters_are_query_local() {
        let shared = SharedState::new();
        let mut requests = active();
        requests.active.as_mut().unwrap().request.filter.symbol = "AAPL".into();
        for revision in ["old.02", "old.01"] {
            requests.commission(
                &[(17, revision), (6378, "0.25"), (6381, "USD"), (6099, "3")]
                    .into_iter()
                    .map(|(tag, value)| (tag, value.into()))
                    .collect(),
            );
        }
        requests.report(&row("old.02", true), &shared, "DU123456");
        let mut outside = row("outside.01", true);
        outside.insert(60, "20260930-23:59:59".into());
        requests.report(&outside, &shared, "DU123456");
        let mut other_account = row("other.01", true);
        other_account.insert(1, "DU654321".into());
        other_account.remove(&6008);
        requests.report(&other_account, &shared, "DU123456");
        requests.report(
            &[(6556, "range1"), (32, "*")]
                .into_iter()
                .map(|(tag, value)| (tag, value.into()))
                .collect(),
            &shared,
            "DU123456",
        );
        let rows = shared
            .orders
            .drain_execution_range_replies()
            .pop()
            .unwrap()
            .result
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].commission_and_fees.as_ref().unwrap().exec_id,
            "old.02"
        );
        assert_eq!(
            rows[0]
                .commission_and_fees
                .as_ref()
                .unwrap()
                .commission_and_fees,
            0.25
        );
        assert!(shared.orders.drain_commission_reports().is_empty());
    }

    #[test]
    fn execution_range_timeout_quarantines_until_end_and_loss_fails_waiters() {
        let shared = SharedState::new();
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        ccp.execution_ranges = active();
        ccp.execution_ranges
            .active
            .as_mut()
            .unwrap()
            .request
            .deadline = Instant::now();
        ccp.execution_ranges
            .queue(
                "today4".into(),
                8,
                "20261001-00:00:00".into(),
                "20261005-12:00:00".into(),
                ExecutionFilter::default(),
            )
            .unwrap();
        ccp.progress_execution_history(
            &mut None,
            &mut context,
            &shared,
            &None,
            &mut HeartbeatState::new(),
            "DU123456",
        );
        let replies = shared.orders.drain_execution_range_replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0].result.is_err());
        assert!(ccp.execution_ranges.active.is_some());
        deliver(&mut ccp, &mut context, &shared, &row("late.01", true));
        assert!(shared.orders.drain_untracked_executions().is_empty());
        end(&mut ccp, &mut context, &shared);
        assert!(shared.orders.drain_execution_range_replies().is_empty());
        assert!(ccp.execution_ranges.active.is_none());
        ccp.execution_ranges.disconnected(&shared);
        let reply = shared.orders.drain_execution_range_replies().pop().unwrap();
        assert_eq!(reply.req_id, 8);
        assert!(reply.result.is_err());
    }

    #[test]
    fn execution_range_malformed_row_fails_instead_of_empty_success() {
        let shared = SharedState::new();
        let mut requests = active();
        let mut invalid = row("old.01", true);
        invalid.remove(&6008);
        requests.report(&invalid, &shared, "DU123456");
        requests.report(
            &[(6556, "range1"), (32, "*")]
                .into_iter()
                .map(|(tag, value)| (tag, value.into()))
                .collect(),
            &shared,
            "DU123456",
        );
        assert!(
            shared
                .orders
                .drain_execution_range_replies()
                .pop()
                .unwrap()
                .result
                .is_err()
        );
    }

    #[test]
    fn execution_range_never_attaches_fee_from_another_correction() {
        for fee_id in ["old.01", "old.02", "old.03"] {
            let shared = SharedState::new();
            let mut requests = active();
            requests.commission(
                &[(17, fee_id), (6378, "0.25"), (6381, "USD")]
                    .into_iter()
                    .map(|(tag, value)| (tag, value.into()))
                    .collect(),
            );
            requests.report(&row("old.02", true), &shared, "DU123456");
            requests.report(
                &[(6556, "range1"), (32, "*")]
                    .into_iter()
                    .map(|(tag, value)| (tag, value.into()))
                    .collect(),
                &shared,
                "DU123456",
            );
            let rows = shared
                .orders
                .drain_execution_range_replies()
                .pop()
                .unwrap()
                .result
                .unwrap();
            assert_eq!(rows[0].commission_and_fees.is_some(), fee_id == "old.02");
        }
    }

    #[test]
    fn execution_range_archived_fees_do_not_change_live_economics_or_revision() {
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        let now = super::super::ccp::fix_utc_to_unix_secs("20261005-12:00:00").unwrap() * 1000;
        shared
            .reference
            .clock()
            .set(now - crate::control::logon::local_now_ms());
        ccp.execution_ranges = active();
        ccp.record_exec_con_id("old.01", 265598);
        deliver(&mut ccp, &mut context, &shared, &row("old.03", true));
        let fee = |id: &str, time: &str, realized: &str| {
            [
                (35, "U"),
                (6040, "60"),
                (17, id),
                (52, time),
                (6378, "0.25"),
                (6381, "USD"),
                (6099, realized),
            ]
            .into_iter()
            .map(|(tag, value)| (tag, value.into()))
            .collect()
        };
        // No 8080 flag exists on archived native fees. Even a higher
        // correction of a known family must not touch today's economics.
        deliver(
            &mut ccp,
            &mut context,
            &shared,
            &fee("old.03", "20261003-12:00:00", "30"),
        );
        assert!(shared.portfolio.realized_since_seed().is_empty());
        assert!(shared.orders.drain_commission_reports().is_empty());
        deliver(
            &mut ccp,
            &mut context,
            &shared,
            &fee("old.02", "20261005-11:59:00", "2"),
        );
        assert_eq!(
            shared.portfolio.realized_since_seed().get(&265598),
            Some(&2.0)
        );
        assert_eq!(shared.orders.drain_commission_reports().len(), 1);
        end(&mut ccp, &mut context, &shared);
        let rows = shared
            .orders
            .drain_execution_range_replies()
            .pop()
            .unwrap()
            .result
            .unwrap();
        assert_eq!(
            rows[0].commission_and_fees.as_ref().unwrap().exec_id,
            "old.03"
        );
        // Late dated replay after query completion is equally isolated.
        deliver(
            &mut ccp,
            &mut context,
            &shared,
            &fee("old.04", "20261003-12:00:00", "40"),
        );
        assert_eq!(
            shared.portfolio.realized_since_seed().get(&265598),
            Some(&2.0)
        );
        assert!(shared.orders.drain_commission_reports().is_empty());
    }
}

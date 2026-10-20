//! STANDARD history replies have untagged status rows and only a tagged end.
//! Keep their reduction separate from live orders, positions and executions.
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::api::types::{ComboLeg, Contract};
use crate::bridge::{ComboView, CompletedHistoryReply, Event, RichOrderInfo, SharedState};
use crate::config::chrono_free_timestamp;
use crate::engine::context::Context;
use crate::protocol::connection::Connection;
use crate::types::OrderStatus;
use crossbeam_channel::Sender;

use super::HeartbeatState;
use super::ccp::{CcpState, order_status_request, perm_id_from_clord_id};
use super::report::{ReportProjection, project_report, report_revision, report_time, stale_report};

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Read only self-contained, direct-routed BAG terms. The ordinary FIX map
/// loses repeated legs; the original message is the authority, not BAG conId.
/// SMART may have different leg routes and remains unsupported here.
pub(super) fn parse_direct_combo_report(
    msg: &[u8],
    fields: &HashMap<u32, String>,
) -> Result<Option<ComboView>, &'static str> {
    use super::ccp::all_values;

    if fields.get(&167).map(String::as_str) != Some("BAG") {
        return Ok(None);
    }
    let counts: Vec<_> = all_values(msg, 6079).collect();
    if counts.is_empty()
        && [6080, 6081, 6082, 616, 6879]
            .iter()
            .all(|tag| all_values(msg, *tag).next().is_none())
    {
        return Ok(None);
    }
    let [count] = counts.as_slice() else {
        return Err("Completed-order BAG has missing or repeated leg count");
    };
    let count = count
        .parse::<usize>()
        .ok()
        .filter(|count| *count > 0)
        .ok_or("Completed-order BAG has invalid leg count")?;
    let mut route = None;
    for tag in [6004, 6210, 100, 207] {
        for value in all_values(msg, tag) {
            if value.is_empty()
                || value.trim() != value
                || ["BEST", "SMART", "ZERO", "ANYEXCH"]
                    .iter()
                    .any(|reserved| value.eq_ignore_ascii_case(reserved))
                || route.is_some_and(|previous| previous != value)
            {
                return Err("Completed-order BAG has unsupported or conflicting direct route");
            }
            route = Some(value);
        }
    }
    let route = route.ok_or("Completed-order BAG has no explicit direct route")?;
    let leg_routes: Vec<_> = all_values(msg, 616).collect();
    if !leg_routes.is_empty()
        && (leg_routes.len() != count || leg_routes.iter().any(|value| *value != route))
    {
        return Err("Completed-order BAG leg routes do not match its direct route");
    }
    // Gateway jexec.eO.f(fb) reads 6087/6680/6679/6401/6216/1689/8211
    // into per-leg auxiliary terms, and e(fb) reads 6442 into combo-order
    // metadata. Those explicit mappings are not implemented here. Absence
    // uses ordinary API defaults; explicit terms must not be erased.
    if [6087, 6680, 6679, 6401, 6216, 1689, 8211, 6442]
        .iter()
        .any(|tag| all_values(msg, *tag).next().is_some())
    {
        return Err("Completed-order BAG has unsupported explicit leg terms");
    }
    let mut legs: Vec<ComboLeg> = Vec::new();
    let mut count_seen = false;
    for part in msg.split(|byte| *byte == crate::protocol::fix::SOH) {
        let Some((tag, value)) = std::str::from_utf8(part)
            .ok()
            .and_then(|part| part.split_once('='))
        else {
            continue;
        };
        match tag {
            "6079" => count_seen = true,
            "6080" => {
                if !count_seen
                    || legs.last().is_some_and(|leg| leg.ratio == 0 || leg.action.is_empty())
                {
                    return Err("Completed-order BAG has incomplete leg groups");
                }
                let con_id = value
                    .parse::<i64>()
                    .ok()
                    .filter(|id| *id > 0)
                    .ok_or("Completed-order BAG has invalid leg identity")?;
                legs.push(ComboLeg {
                    con_id,
                    exchange: route.into(),
                    ..ComboLeg::default()
                });
            }
            "6081" => {
                let leg = legs.last_mut().ok_or("Completed-order BAG has unordered leg groups")?;
                if leg.ratio != 0 {
                    return Err("Completed-order BAG has repeated leg ratio");
                }
                leg.ratio = value
                    .parse::<i32>()
                    .ok()
                    .filter(|ratio| *ratio > 0)
                    .ok_or("Completed-order BAG has invalid leg ratio")?;
            }
            "6082" => {
                let leg = legs.last_mut().ok_or("Completed-order BAG has unordered leg groups")?;
                if leg.ratio == 0 || !leg.action.is_empty() {
                    return Err("Completed-order BAG has unordered or repeated leg orientation");
                }
                leg.action = match value {
                    "1" => "BUY",
                    "0" => "SELL",
                    _ => return Err("Completed-order BAG has invalid leg orientation"),
                }
                .into();
            }
            _ => {}
        }
    }
    if legs.len() != count || legs.iter().any(|leg| leg.ratio == 0 || leg.action.is_empty()) {
        return Err("Completed-order BAG has incomplete leg groups");
    }
    // Gateway reads 6879 as an independent repeated-value array in leg order.
    // Do not drop a malformed element or infer a missing leg price as zero.
    let leg_prices = all_values(msg, 6879)
        .map(|value| {
            value
                .parse::<f64>()
                .ok()
                .filter(|price| price.is_finite())
                .ok_or("Completed-order BAG has invalid leg prices")
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !leg_prices.is_empty()
        && (leg_prices.len() != count
            || (leg_prices.contains(&f64::MAX)
                && leg_prices.iter().any(|price| *price != f64::MAX)))
    {
        return Err("Completed-order BAG has incomplete leg prices");
    }
    Ok(Some(ComboView {
        contract: Contract {
            con_id: fields.get(&6008).and_then(|id| id.parse().ok()).unwrap_or(0),
            sec_type: "BAG".into(),
            exchange: route.into(),
            currency: fields.get(&15).cloned().unwrap_or_default(),
            symbol: fields.get(&55).cloned().unwrap_or_default(),
            local_symbol: fields.get(&6035).cloned().unwrap_or_default(),
            combo_legs: legs,
            ..Contract::default()
        },
        leg_prices,
    }))
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum HistoryReport {
    Unrelated,
    Row,
    End,
}

struct Request {
    connection: String,
    api_only: bool,
    start: String,
    end: String,
    deadline: Instant,
}

struct Active {
    request: Request,
    wire_id: String,
    rows: Reducer,
    // A timeout ends the API request but not the untagged wire phase. Drain
    // until its real end or link loss before allowing another status query.
    timed_out: bool,
}

#[derive(Default)]
pub(super) struct HistoryRequests {
    waiting: VecDeque<Request>,
    active: Option<Active>,
    status_pending: HashMap<i64, usize>,
    status_deferred: VecDeque<String>,
    next_id: u64,
}

fn reply(shared: &SharedState, connection: String, result: Result<Vec<RichOrderInfo>, String>) {
    shared
        .orders
        .push_completed_history_reply(CompletedHistoryReply { connection, result });
    shared.notify();
}

impl HistoryRequests {
    /// Mark an active completed-order query incomplete when a row it would
    /// otherwise collect is instead quarantined as an option lifecycle
    /// report. Explicit foreign-account rows are unrelated; absent or empty
    /// account identity cannot support a successful omission.
    pub(super) fn reject_option_lifecycle(&mut self, fields: &HashMap<u32, String>, account: &str) {
        let Some(active) = self.active.as_mut().filter(|active| !active.timed_out) else {
            return;
        };
        if fields.get(&6556).is_some_and(|id| !id.starts_with("PT."))
            || fields.get(&20).map(String::as_str) != Some("3")
            || fields.get(&11).map(String::as_str) == Some("*")
            || fields
                .get(&1)
                .is_some_and(|reported| !reported.is_empty() && reported != account)
        {
            return;
        }
        active.rows.error.get_or_insert_with(|| {
            "Completed-order history contains unsupported option lifecycle report".into()
        });
    }

    pub(super) fn queue(&mut self, connection: String, api_only: bool, start: String, end: String) {
        self.waiting.push_back(Request {
            connection,
            api_only,
            start,
            end,
            deadline: Instant::now() + QUERY_TIMEOUT,
        });
    }

    pub(super) fn active(&self) -> bool {
        self.active.is_some()
    }

    pub(super) fn defer_status(&mut self, clord: String) {
        self.status_deferred.push_back(clord);
    }

    /// Complete a sparse working-order report without overlapping an existing
    /// request for the same native order or an untagged STANDARD history phase.
    pub(super) fn ensure_status(&mut self, clord: &str) {
        let base = perm_id_from_clord_id(clord);
        if !self.status_pending.contains_key(&base)
            && !self.status_deferred.iter().any(|pending| perm_id_from_clord_id(pending) == base)
        {
            self.defer_status(clord.to_owned());
        }
    }

    pub(super) fn sent_status(&mut self, clord: &str) {
        *self
            .status_pending
            .entry(perm_id_from_clord_id(clord))
            .or_default() += 1;
    }

    pub(super) fn status_reply(&mut self, fields: &HashMap<u32, String>) {
        if fields.get(&20).map(String::as_str) == Some("3")
            && let Some(clord) = fields.get(&11)
        {
            let base = perm_id_from_clord_id(clord);
            if let Some(pending) = self.status_pending.get_mut(&base) {
                *pending -= 1;
                if *pending == 0 {
                    self.status_pending.remove(&base);
                }
            }
        }
    }

    /// A quarantined lifecycle status is still the wire reply to an earlier
    /// per-order status request. Retire that bookkeeping without projecting
    /// the report into ordinary order state. An explicitly foreign account
    /// cannot acknowledge this account's pending request; absent identity is
    /// attributable because the native H reply is not request-tagged.
    pub(super) fn option_lifecycle_status_reply(
        &mut self,
        fields: &HashMap<u32, String>,
        account: &str,
    ) {
        if fields
            .get(&1)
            .is_some_and(|reported| !reported.is_empty() && reported != account)
        {
            return;
        }
        self.status_reply(fields);
    }

    #[cfg(test)]
    pub(super) fn status_reply_pending(&self) -> bool {
        !self.status_pending.is_empty()
    }

    pub(super) fn disconnected(&mut self, shared: &SharedState) {
        if let Some(active) = self.active.take()
            && !active.timed_out
        {
            reply(
                shared,
                active.request.connection,
                Err("Completed-order history connection lost".into()),
            );
        }
        for request in self.waiting.drain(..) {
            reply(
                shared,
                request.connection,
                Err("Completed-order history connection lost".into()),
            );
        }
        self.status_pending.clear();
        self.status_deferred.clear();
    }

    /// A side tap: status rows may also belong to a concurrently working order.
    /// Only the exact query end is consumed unconditionally by this collector.
    pub(super) fn classify(
        &mut self,
        fields: &HashMap<u32, String>,
        shared: &SharedState,
        _account: &str,
    ) -> HistoryReport {
        let Some(active) = self.active.as_mut() else {
            return HistoryReport::Unrelated;
        };
        if fields.get(&6556) == Some(&active.wire_id)
            && fields.get(&55).map(String::as_str) == Some("*")
        {
            let active = self.active.take().unwrap();
            if !active.timed_out {
                reply(
                    shared,
                    active.request.connection,
                    active.rows.finish(active.request.api_only),
                );
            }
            return HistoryReport::End;
        }
        // Non-PT request IDs are other end markers, never ordinary rows.
        if fields.get(&6556).is_some_and(|id| !id.starts_with("PT."))
            || fields.get(&20).map(String::as_str) != Some("3")
            || fields.get(&11).map(String::as_str) == Some("*")
        {
            return HistoryReport::Unrelated;
        }
        HistoryReport::Row
    }

    pub(super) fn collect(
        &mut self,
        fields: &HashMap<u32, String>,
        account: &str,
        known: Option<&RichOrderInfo>,
        known_combo: bool,
        reported_combo: &Result<Option<ComboView>, &'static str>,
    ) {
        if let Some(active) = self.active.as_mut()
            && !active.timed_out
        {
            active.rows.push_report(fields, account, reported_combo);
            if let Some(known) = known
                && let Some(row) = active
                    .rows
                    .rows
                    .get_mut(&(account.to_string(), known.order.perm_id))
            {
                row.known = Some(known.clone());
                row.known_combo = known_combo;
                row.version = known.report_revision.unwrap_or(row.version);
                if let Some(time) = &known.report_time {
                    row.time = time.clone();
                }
                row.status = match known.order_state.status.as_str() {
                    "Filled" => OrderStatus::Filled,
                    "Cancelled" | "Inactive" => OrderStatus::Cancelled,
                    _ => OrderStatus::Submitted,
                };
            }
        }
    }

    #[cfg(test)]
    fn report(
        &mut self,
        fields: &HashMap<u32, String>,
        shared: &SharedState,
        account: &str,
    ) -> HistoryReport {
        let report = self.classify(fields, shared, account);
        if report == HistoryReport::Row {
            self.collect(fields, account, None, false, &Ok(None));
        }
        report
    }

    fn expire(&mut self, now: Instant, shared: &SharedState) {
        if let Some(active) = self.active.as_mut()
            && !active.timed_out
            && now >= active.request.deadline
        {
            active.timed_out = true;
            active.rows = Reducer::default();
            reply(
                shared,
                active.request.connection.clone(),
                Err("Completed-order history timed out; awaiting its wire end".into()),
            );
        }
        while self.waiting.front().is_some_and(|r| now >= r.deadline) {
            let request = self.waiting.pop_front().unwrap();
            reply(
                shared,
                request.connection,
                Err("Completed-order history could not start before its deadline".into()),
            );
        }
    }
}

impl CcpState {
    pub(super) fn progress_completed_history(
        &mut self,
        conn: &mut Option<Connection>,
        context: &mut Context,
        shared: &SharedState,
        events: &Option<Sender<Event>>,
        hb: &mut HeartbeatState,
        account: &str,
    ) {
        let now = Instant::now();
        self.completed_history.expire(now, shared);
        if self.disconnected || self.completed_history.active() {
            return;
        }
        let Some(socket) = conn.as_mut() else { return };
        // A cancel/modify rejection or incomplete working-order report may
        // ask for a status during history. Release it first; never overlap
        // untagged H replies.
        while let Some(clord) = self.completed_history.status_deferred.pop_front() {
            if socket
                .send_fix(&order_status_request(
                    &clord,
                    account,
                    &chrono_free_timestamp(),
                ))
                .is_err()
            {
                self.handle_disconnect(context, shared, events);
                return;
            }
            self.completed_history.sent_status(&clord);
            hb.last_ccp_sent = now;
        }
        if shared.orders.open_orders_held()
            || shared.orders.execution_history_completion().is_none()
            || !self.completed_history.status_pending.is_empty()
        {
            return;
        }
        let Some(request) = self.completed_history.waiting.pop_front() else {
            return;
        };
        if !shared.orders.execution_history_matches(&request.connection) {
            reply(
                shared,
                request.connection,
                Err("Completed-order history connection changed before send".into()),
            );
            return;
        }
        self.completed_history.next_id += 1;
        let wire_id = format!("IBX.Completed.{}", self.completed_history.next_id);
        let ts = chrono_free_timestamp();
        let fields = [
            (35, "H"),
            (52, &ts),
            (11, "*"),
            (55, "*"),
            (54, "*"),
            (1, account),
            (6533, "1"),
            (6536, request.start.as_str()),
            (6537, request.end.as_str()),
            (6556, wire_id.as_str()),
        ];
        if socket.send_fix(&fields).is_err() {
            reply(
                shared,
                request.connection,
                Err("Failed to send completed-order history request".into()),
            );
            self.handle_disconnect(context, shared, events);
            return;
        }
        hb.last_ccp_sent = now;
        self.completed_history.active = Some(Active {
            request,
            wire_id,
            rows: Reducer::default(),
            timed_out: false,
        });
    }
}

struct Row {
    version: u32,
    time: String,
    fields: HashMap<u32, String>,
    status: OrderStatus,
    // A fresh accepted report of a known current order uses the same typed
    // reduction as the live receiver, including cancellation's retained terms.
    known: Option<RichOrderInfo>,
    // The live receiver has an exact per-order ComboView. A contract cached
    // by BAG conId alone is not authority: different combos share that conId.
    known_combo: bool,
    // Kept only in this query; never installed into the live order/contract
    // cache. Sparse cancellations preserve the previous report's terms.
    reported_combo: Result<Option<ComboView>, &'static str>,
}

#[derive(Default)]
struct Reducer {
    // Account and broker ClOrd base are authoritative; client order IDs can
    // collide across sessions and must not merge independent broker orders.
    rows: BTreeMap<(String, i64), Row>,
    error: Option<String>,
}

impl Reducer {
    #[cfg(test)]
    fn push(&mut self, fields: &HashMap<u32, String>, account: &str) {
        self.push_report(fields, account, &Ok(None));
    }

    fn push_report(
        &mut self,
        fields: &HashMap<u32, String>,
        account: &str,
        reported_combo: &Result<Option<ComboView>, &'static str>,
    ) {
        if self.error.is_some() {
            return;
        }
        if let Err(message) = self.add(fields, account, reported_combo) {
            self.error = Some(message.into());
        }
    }

    fn add(
        &mut self,
        fields: &HashMap<u32, String>,
        account: &str,
        reported_combo: &Result<Option<ComboView>, &'static str>,
    ) -> Result<(), &'static str> {
        let tag = |t| fields.get(&t).map(String::as_str);
        let owner = tag(1).ok_or("History row has no account")?;
        if owner != account {
            return Err("History row belongs to a different account");
        }
        match super::ccp::execution_component(fields) {
            crate::api::types::ExecutionComponent::ComboLeg { .. } => return Ok(()),
            crate::api::types::ExecutionComponent::UnsupportedCombo { .. } => {
                return Err("Completed-order history has unsupported combo component metadata");
            }
            _ => {}
        }
        let clord = tag(11).ok_or("History row has no broker order identity")?;
        let base = perm_id_from_clord_id(clord);
        if base == 0 {
            return Err("History row has invalid broker order identity");
        }
        let version =
            report_revision(fields).ok_or("History row has invalid broker order revision")?;
        let time = report_time(fields).ok_or("History row has no effective report time")?;
        let status = match tag(39) {
            Some("2") => OrderStatus::Filled,
            Some("4" | "C" | "8") => OrderStatus::Cancelled,
            Some("0" | "5" | "A") => OrderStatus::Submitted,
            Some("1") => OrderStatus::PartiallyFilled,
            Some("6" | "D") => OrderStatus::PendingCancel,
            _ => return Err("History row has unsupported status"),
        };
        let key = (owner.to_string(), base);
        if let Some(previous) = self.rows.get_mut(&key) {
            if stale_report(
                Some(version),
                Some(time),
                Some(previous.version),
                Some(previous.time.as_str()),
                matches!(tag(39), Some("4" | "C")),
            ) {
                return Ok(());
            }
            if matches!(tag(39), Some("4" | "C")) {
                // Native cancellation has its own path: it changes the state,
                // not the existing rich terms, at any accepted revision.
                previous.status = status;
                previous.known = None;
                previous.known_combo = false;
                previous.version = previous.version.max(version);
                previous.time = time.to_string();
                for t in [39, 150, 6699, 60, 52] {
                    previous.fields.remove(&t);
                    if let Some(value) = fields.get(&t) {
                        previous.fields.insert(t, value.clone());
                    }
                }
                // Cancellation preserves amended terms, not stale execution
                // totals/fees when the broker supplies newer values.
                for t in [14, 151, 6, 12] {
                    if let Some(value) = fields.get(&t) {
                        previous.fields.insert(t, value.clone());
                    }
                }
                return Ok(());
            }
        }
        self.rows.insert(
            key,
            Row {
                version,
                time: time.to_string(),
                fields: fields.clone(),
                status,
                known: None,
                known_combo: false,
                reported_combo: reported_combo.clone(),
            },
        );
        Ok(())
    }

    fn finish(self, api_only: bool) -> Result<Vec<RichOrderInfo>, String> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let api_id = |base: i64, fields: &HashMap<u32, String>| {
            fields
                .get(&6121)
                .and_then(|id| id.parse::<i64>().ok())
                .filter(|id| *id != 0 && *id != i64::from(i32::MAX))
                .unwrap_or(base)
        };
        let identities: HashMap<_, _> = self
            .rows
            .iter()
            .map(|((account, base), row)| ((account.as_str(), *base), api_id(*base, &row.fields)))
            .collect();
        let mut results = Vec::new();
        for ((account, base), row) in &self.rows {
            if !row.status.is_terminal() {
                continue;
            }
            let fields = &row.fields;
            let from_api = fields.get(&6088).is_some_and(|origin| origin == "Socket")
                || fields
                    .get(&6121)
                    .and_then(|id| id.parse::<i64>().ok())
                    .is_some_and(|id| id != 0 && id != i64::from(i32::MAX));
            if api_only && !from_api {
                continue;
            }
            let reported_combo = if fields.get(&167).map(String::as_str) == Some("BAG")
                && !row.known_combo
            {
                Some(
                    row.reported_combo
                        .as_ref()
                        .map_err(|message| message.to_string())?
                        .as_ref()
                        .ok_or("Completed-order BAG snapshot has no known per-order combo")?,
                )
            } else {
                None
            };
            // A generic cached BAG contract is not per-order authority.
            // A cold direct report must be projected from its own full terms.
            let known = row.known.as_ref().filter(|_| reported_combo.is_none());
            // STANDARD supplies complete typed order snapshots. Do not turn
            // an incomplete row or an unsupported native container into a
            // successful recovery record by using the projector's defaults.
            let number = |tag| {
                fields
                    .get(&tag)
                    .and_then(|v| v.parse::<f64>().ok())
                    .filter(|v| v.is_finite())
            };
            // A native OCA reduction can cancel an unfilled order with an
            // explicit zero current total (paper STANDARD reply, 2026-10-08).
            // Preserve that broker value, not an invented original quantity.
            // Missing totals, fills, and every non-cancel status still fail.
            let zero_quantity_cancel = fields.get(&39).map(String::as_str) == Some("4")
                && number(38) == Some(0.0)
                && number(14) == Some(0.0)
                && number(151) == Some(0.0);
            if known.is_none()
                && (!fields
                    .get(&6008)
                    .and_then(|v| v.parse::<i64>().ok())
                    .is_some_and(|v| v > 0)
                    || !(number(38).is_some_and(|v| v > 0.0) || zero_quantity_cancel)
                    || !matches!(fields.get(&54).map(String::as_str), Some("1" | "2" | "5"))
                    || !fields.get(&15).is_some_and(|v| !v.is_empty())
                    || !fields
                        .get(&167)
                        .is_some_and(|v| !v.is_empty() && (v != "BAG" || reported_combo.is_some()))
                    || fields.get(&8302).is_some_and(|v| v == "1")
                    || fields.get(&6406).is_some_and(|v| v == "1"))
            {
                return Err("Incomplete or unsupported completed-order snapshot".into());
            }
            if known.is_none() {
                match fields.get(&40).map(String::as_str) {
                    Some("1") => {}
                    Some("2") if number(44).is_some() => {}
                    Some("3") if number(99).is_some() => {}
                    Some("4") if number(44).is_some() && number(99).is_some() => {}
                    _ => return Err("Incomplete or unsupported completed-order terms".into()),
                }
            }
            if known.is_none()
                && !number(14)
                    .is_some_and(|v| v >= 0.0 && (row.status != OrderStatus::Filled || v > 0.0))
            {
                return Err(
                    "Completed-order snapshot has no valid cumulative fill quantity".into(),
                );
            }
            let parent = fields.get(&6107).map_or(0, |id| perm_id_from_clord_id(id));
            let parent_id = identities
                .get(&(account.as_str(), parent))
                .copied()
                .unwrap_or(parent);
            let mut projected = known.cloned().unwrap_or_else(|| {
                project_report(
                    fields,
                    ReportProjection {
                        order_id: api_id(*base, fields),
                        parent_id,
                        status: row.status,
                        account_id: account,
                        fallback_order: None,
                        fallback_con_id: 0,
                        cached_contract: None,
                        combo: reported_combo,
                        trail_limit: None,
                        combo_leg_prices: reported_combo
                            .map(|combo| combo.leg_prices.clone())
                            .unwrap_or_default(),
                    },
                )
            });
            let combo = projected.contract.sec_type == "BAG";
            if combo && !row.known_combo && reported_combo.is_none() {
                return Err("Completed-order BAG snapshot has no known per-order combo".into());
            }
            // Match openOrder's existing ComboView convention: per-leg prices
            // are the order limits, while FIX 44 is their derived net price.
            // Missing/partly populated leg prices must not become zero limits.
            let leg_prices = &projected.order.order_combo_legs;
            let per_leg_priced = combo && leg_prices.iter().any(|price| *price != f64::MAX);
            if combo
                && ((!leg_prices.is_empty()
                    && leg_prices.len() != projected.contract.combo_legs.len())
                    || leg_prices.iter().any(|price| !price.is_finite())
                    || (per_leg_priced && leg_prices.contains(&f64::MAX)))
            {
                return Err("Incomplete completed-order combo leg prices".into());
            }
            if per_leg_priced {
                projected.order.lmt_price = f64::MAX;
            }
            // A known order may have started with a partial acknowledgement;
            // retaining it on a sparse cancellation must not invent terms.
            let order = &projected.order;
            if projected.contract.con_id <= 0
                || projected.contract.currency.is_empty()
                || projected.contract.sec_type.is_empty()
                || !matches!(order.action.as_str(), "BUY" | "SELL" | "SSHORT")
                || !order.total_quantity.is_finite()
                || order.total_quantity < 0.0
                || (order.total_quantity == 0.0
                    && !(zero_quantity_cancel && order.filled_quantity == 0.0))
                || !order.filled_quantity.is_finite()
                || order.filled_quantity < 0.0
                || (row.status == OrderStatus::Filled && order.filled_quantity <= 0.0)
                || !projected.parent_id_known
                || fields.get(&8302).is_some_and(|value| value == "1")
                || fields.get(&6406).is_some_and(|value| value == "1")
            {
                return Err("Incomplete or unsupported completed-order snapshot".into());
            }
            match order.order_type.as_str() {
                "MKT" => {}
                "LMT"
                    if per_leg_priced
                        || (order.lmt_price.is_finite() && order.lmt_price != f64::MAX) => {}
                "STP" if order.aux_price.is_finite() && order.aux_price != f64::MAX => {}
                "STP LMT"
                    if order.lmt_price.is_finite()
                        && order.lmt_price != f64::MAX
                        && order.aux_price.is_finite()
                        && order.aux_price != f64::MAX => {}
                _ => return Err("Incomplete or unsupported completed-order terms".into()),
            }
            if projected.order.tif == "???" {
                return Err("Incomplete or unsupported completed-order time in force".into());
            }
            if projected.order.tif == "GTD" && projected.order.good_till_date.is_empty() {
                return Err("Completed GTD order has no expiry".into());
            }
            results.push(projected);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::fix;

    const CAPTURE: &str =
        include_str!("../../../tests/fixtures/completed_history/paper_amended_cancelled.jsonl");

    fn capture() -> Vec<HashMap<u32, String>> {
        CAPTURE
            .lines()
            .map(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                fix::fix_parse(
                    value["fix"]
                        .as_str()
                        .unwrap()
                        .replace('|', "\x01")
                        .as_bytes(),
                )
            })
            .collect()
    }

    fn active() -> HistoryRequests {
        let mut requests = HistoryRequests::default();
        requests.queue(
            "today4".into(),
            false,
            "20260102-00:00:00".into(),
            "20260102-23:59:59".into(),
        );
        requests.active = Some(Active {
            request: requests.waiting.pop_front().unwrap(),
            wire_id: "SANITIZED_HISTORY_QUERY_2".into(),
            rows: Reducer::default(),
            timed_out: false,
        });
        requests
    }

    fn working_row(version: u32, time: &str) -> HashMap<u32, String> {
        let mut row = capture()[1].clone();
        row.remove(&6699);
        for (tag, value) in [
            (11, format!("42.{version}")),
            (6121, "42".into()),
            (20, "3".into()),
            (150, "0".into()),
            (39, "0".into()),
            (60, time.into()),
            (38, "3".into()),
            (14, "0".into()),
            (151, "3".into()),
            (32, "0".into()),
            (44, "2".into()),
            (6, "0".into()),
            (12, "0.25".into()),
            (6107, "41.0".into()),
        ] {
            row.insert(tag, value);
        }
        row
    }

    fn zero_quantity_oca_cancel() -> HashMap<u32, String> {
        // Relevant fields from a fresh paper STANDARD reply on 2026-10-08:
        // the OCA peer filled and this previously working one-share stop was
        // cancelled with total/cumulative/leaves all zero. Identities and
        // prices are synthetic; zero is the current broker total, not one.
        let mut row = capture()[1].clone();
        for (tag, value) in [
            (20, "3"),
            (150, "4"),
            (39, "4"),
            (38, "0"),
            (14, "0"),
            (151, "0"),
            (40, "3"),
            (99, "100"),
            (54, "2"),
            (583, "fixture.oca"),
            (6209, "ReduceOnFillNonBlock"),
            (6107, "1234567890001.0"),
        ] {
            row.insert(tag, value.into());
        }
        row.remove(&44);
        row
    }

    fn captured_combo(filled: bool, side: &str) -> (HashMap<u32, String>, Vec<f64>) {
        let fixture = if filled {
            include_str!("../../../tests/fixtures/gw1040/scenarios/20260930/i105_combo_fill.jsonl")
        } else {
            include_str!(
                "../../../tests/fixtures/gw1040/scenarios/20260926b/i105_combo_leg_prices.jsonl"
            )
        };
        for line in fixture.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            if value["leg"] != "fix_in" || value["conn"] != "CCP" {
                continue;
            }
            let Some(values) = value["fields"].as_array() else {
                continue;
            };
            let mut fields: HashMap<u32, String> = values
                .iter()
                .map(|field| {
                    (
                        field[0].as_str().unwrap().parse().unwrap(),
                        field[1].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            if fields.get(&167).map(String::as_str) != Some("BAG")
                || fields.get(&39).map(String::as_str) != Some(if filled { "2" } else { "4" })
                || fields.get(&54).map(String::as_str) != Some(side)
            {
                continue;
            }
            let prices = values
                .iter()
                .filter(|field| field[0] == "6879")
                .map(|field| field[1].as_str().unwrap().parse().unwrap())
                .collect();
            // These are captured live reports, not a captured STANDARD reply.
            // Restate the same typed order for this history regression.
            fields.insert(20, "3".into());
            return (fields, prices);
        }
        panic!("missing captured terminal combo");
    }

    fn known_combo(fields: &HashMap<u32, String>, prices: Vec<f64>) -> RichOrderInfo {
        use crate::api::types::{ComboLeg, Contract};
        use crate::bridge::ComboView;
        // This exact view comes from the submitted contract in the captured
        // session. SMART is not inferred from a cold BAG history report.
        let combo = ComboView {
            contract: Contract {
                con_id: 28_812_380,
                symbol: "QQQ,SPY".into(),
                sec_type: "BAG".into(),
                exchange: "SMART".into(),
                currency: "USD".into(),
                combo_legs: [(756_733, "BUY"), (320_227_571, "SELL")]
                    .into_iter()
                    .map(|(con_id, action)| ComboLeg {
                        con_id,
                        ratio: 1,
                        action: action.into(),
                        exchange: "SMART".into(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            leg_prices: prices,
        };
        project_report(
            fields,
            ReportProjection {
                order_id: fields[&6121].parse().unwrap(),
                parent_id: 0,
                status: if fields[&39] == "2" {
                    OrderStatus::Filled
                } else {
                    OrderStatus::Cancelled
                },
                account_id: "DUXXXXXXX",
                fallback_order: None,
                fallback_con_id: 0,
                cached_contract: None,
                combo: Some(&combo),
                trail_limit: None,
                combo_leg_prices: combo.leg_prices.clone(),
            },
        )
    }

    fn finish_combo_query(
        fields: &HashMap<u32, String>,
        known: Option<&RichOrderInfo>,
        known_combo: bool,
    ) -> Result<Vec<RichOrderInfo>, String> {
        let mut history = active();
        let shared = SharedState::new();
        history.collect(fields, "DUXXXXXXX", known, known_combo, &Ok(None));
        history.report(&capture()[2], &shared, "DUXXXXXXX");
        assert!(shared.orders.drain_fills().is_empty());
        assert!(shared.orders.drain_order_updates().is_empty());
        assert!(shared.orders.drain_open_orders().is_empty());
        assert!(shared.portfolio.position_infos().is_empty());
        shared
            .orders
            .drain_completed_history_replies()
            .remove(0)
            .result
    }

    fn direct_report(filled: bool) -> Vec<(u32, String)> {
        let fixture = if filled {
            include_str!("../../../tests/fixtures/gw1040/scenarios/20260930/i105_combo_fill.jsonl")
        } else {
            include_str!("../../../tests/fixtures/gw1040/scenarios/20260926b/i105_combo_leg_prices.jsonl")
        };
        for line in fixture.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            if value["leg"] != "fix_in" || value["conn"] != "CCP" {
                continue;
            }
            let Some(values) = value["fields"].as_array() else { continue };
            let mut fields: Vec<_> = values
                .iter()
                .map(|field| (
                    field[0].as_str().unwrap().parse::<u32>().unwrap(),
                    field[1].as_str().unwrap().to_string(),
                ))
                .collect();
            let get = |tag| fields.iter().find(|(key, _)| *key == tag).map(|(_, value)| value.as_str());
            if get(167) != Some("BAG")
                || get(39) != Some(if filled { "2" } else { "4" })
                || get(54) != Some("1")
            {
                continue;
            }
            // These are captured LIVE SMART reports. Only the repeated leg
            // layout/prices are captured: history classification and this
            // direct route are synthetic, not cold STANDARD acceptance.
            for (tag, value) in &mut fields {
                match tag {
                    1 => *value = "DU123456".into(),
                    20 => *value = "3".into(),
                    100 | 207 | 6004 | 6210 => *value = "CBOE".into(),
                    _ => {}
                }
            }
            if !fields.iter().any(|(tag, _)| *tag == 6004) {
                fields.push((6004, "CBOE".into()));
            }
            return fields;
        }
        panic!("missing captured BAG report");
    }

    fn replace_tag(fields: &mut Vec<(u32, String)>, tag: u32, value: &str) {
        fields.retain(|(key, _)| *key != tag);
        fields.push((tag, value.into()));
    }

    fn cold_direct_history(
        rows: &[Vec<(u32, String)>],
    ) -> Result<Vec<(Contract, crate::api::Order, crate::api::types::OrderState)>, String> {
        use crate::api::{EClient, Wrapper};
        use std::sync::Arc;

        #[derive(Default)]
        struct Callback {
            rows: Vec<(Contract, crate::api::Order, crate::api::types::OrderState)>,
            ends: usize,
            errors: Vec<String>,
        }
        impl Wrapper for Callback {
            fn completed_order(
                &mut self,
                contract: &Contract,
                order: &crate::api::Order,
                state: &crate::api::types::OrderState,
            ) {
                self.rows.push((contract.clone(), order.clone(), state.clone()));
            }
            fn completed_orders_end(&mut self) { self.ends += 1; }
            fn error(&mut self, _: i64, _: i64, message: &str, _: &str) {
                self.errors.push(message.into());
            }
        }
        let mut ccp = CcpState::new();
        ccp.completed_history = active();
        let mut context = Context::new();
        let shared = Arc::new(SharedState::new());
        shared.orders.begin_execution_history("today4");
        shared.orders.complete_execution_history("today4");
        for row in rows {
            let fields: Vec<_> = row.iter().map(|(tag, value)| (*tag, value.as_str())).collect();
            ccp.process_ccp_message(
                &fix::fix_build(&fields, 1),
                &mut None,
                &mut context,
                &shared,
                &None,
                &mut HeartbeatState::new(),
                "DU123456",
            );
        }
        deliver(&mut ccp, &mut context, &shared, &capture()[2]);
        assert!(context.combos.orders.is_empty());
        assert!(shared.orders.drain_fills().is_empty());
        assert!(shared.orders.drain_order_updates().is_empty());
        assert!(shared.orders.drain_open_orders().is_empty());
        assert!(shared.portfolio.position_infos().is_empty());
        for row in rows {
            if let Some((_, id)) = row.iter().find(|(tag, _)| *tag == 6121) {
                let id = id.parse().unwrap();
                assert!(context.order(id).is_none());
                assert!(shared.orders.combo_view(id).is_none());
                assert!(shared.orders.get_order_info(id).is_none());
            }
        }
        let (sender, _receiver) = crossbeam_channel::unbounded();
        let client = EClient::from_parts(
            shared,
            sender,
            std::thread::spawn(|| {}),
            "DU123456".into(),
        );
        let mut callback = Callback::default();
        client.process_msgs(&mut callback);
        if callback.errors.is_empty() {
            assert_eq!(callback.ends, 1);
            Ok(callback.rows)
        } else {
            assert_eq!(callback.ends, 0);
            assert!(callback.rows.is_empty());
            Err(callback.errors.join("; "))
        }
    }

    #[test]
    fn completed_cold_direct_bag_raw_reports_reach_callbacks_with_signed_prices() {
        for price in ["0.05", "-0.05", "0"] {
            let mut fields = direct_report(true);
            replace_tag(&mut fields, 44, price);
            let rows = cold_direct_history(&[fields]).unwrap();
            let (contract, order, state) = &rows[0];
            assert_eq!(contract.exchange, "CBOE");
            assert_eq!(contract.combo_legs.len(), 2);
            assert_eq!(contract.combo_legs[0].con_id, 756_733);
            assert_eq!(contract.combo_legs[0].ratio, 1);
            assert_eq!(contract.combo_legs[0].action, "BUY");
            assert_eq!(contract.combo_legs[1].con_id, 320_227_571);
            assert_eq!(contract.combo_legs[1].action, "SELL");
            assert!(contract.combo_legs.iter().all(|leg| leg.exchange == "CBOE"));
            assert_eq!(order.lmt_price, price.parse::<f64>().unwrap());
            assert!(order.order_combo_legs.is_empty());
            assert_eq!(state.status, "Filled");
        }
        let mut fields = direct_report(false);
        // Prices are read as an ordered array even when outside the leg blocks.
        fields.retain(|(tag, _)| *tag != 6879);
        fields.extend([(6879, "0".into()), (6879, "1.25".into())]);
        fields.extend([(616, "CBOE".into()), (616, "CBOE".into())]);
        let rows = cold_direct_history(&[fields]).unwrap();
        assert_eq!(rows[0].1.order_combo_legs, vec![0.0, 1.25]);
        assert_eq!(rows[0].1.lmt_price, f64::MAX);
        assert_eq!(rows[0].2.status, "Cancelled");
    }

    #[test]
    fn completed_cold_direct_bag_keeps_amendment_on_sparse_cancel_and_ignores_stale_terms() {
        for per_leg in [false, true] {
        let mut first = direct_report(true);
        for (tag, value) in [
            (11, "42.0"), (6121, "42"), (39, "0"), (150, "0"),
            (14, "0"), (151, "1"), (60, "20260930-19:03:40"), (44, "0.05"),
        ] {
            replace_tag(&mut first, tag, value);
        }
        if per_leg {
            first.extend([(6879, "0.06".into()), (6879, "0.01".into())]);
        }
        let mut amended = first.clone();
        replace_tag(&mut amended, 11, "42.1");
        replace_tag(&mut amended, 60, "20260930-19:03:45");
        replace_tag(&mut amended, 44, "-0.10");
        amended.iter_mut().filter(|(tag, _)| *tag == 6080).nth(1).unwrap().1 =
            "920261651".into();
        if per_leg {
            amended.retain(|(tag, _)| *tag != 6879);
            amended.extend([(6879, "0.25".into()), (6879, "0.05".into())]);
        }
        let cancel: Vec<_> = [
            (35, "8"), (1, "DU123456"), (11, "42.2"), (20, "3"),
            (39, "4"), (150, "4"), (60, "20260930-19:03:50"), (14, "0"), (151, "0"),
        ].into_iter().map(|(tag, value)| (tag, value.into())).collect();
        let mut stale = first.clone();
        // Even an unusable older report must not replace admitted terms.
        stale.push((100, "SMART".into()));
        let rows = cold_direct_history(&[first, amended, cancel, stale]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.combo_legs[1].con_id, 920_261_651);
        if per_leg {
            assert_eq!(rows[0].1.order_combo_legs, vec![0.25, 0.05]);
            assert_eq!(rows[0].1.lmt_price, f64::MAX);
        } else {
            assert_eq!(rows[0].1.lmt_price, -0.10);
        }
        assert_eq!(rows[0].2.status, "Cancelled");
        }
    }

    #[test]
    fn completed_cold_direct_bags_do_not_share_legs_through_generic_bag_id() {
        let first = direct_report(true);
        let mut second = first.clone();
        replace_tag(&mut second, 11, "900000001.0");
        replace_tag(&mut second, 6121, "43");
        second.iter_mut().filter(|(tag, _)| *tag == 6080).nth(1).unwrap().1 =
            "920261651".into();
        let rows = cold_direct_history(&[first, second]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0.con_id, rows[1].0.con_id);
        let ids: std::collections::BTreeSet<_> =
            rows.iter().map(|row| row.0.combo_legs[1].con_id).collect();
        assert_eq!(ids, [320_227_571, 920_261_651].into());
    }

    #[test]
    fn direct_bag_decoder_preserves_native_single_leg_and_repeated_id_shapes() {
        let mut fields = direct_report(true);
        for (_, value) in fields.iter_mut().filter(|(tag, _)| *tag == 6080) {
            *value = "756733".into();
        }
        let encode = |fields: &[(u32, String)]| {
            let values: Vec<_> = fields.iter().map(|(tag, value)| (*tag, value.as_str())).collect();
            fix::fix_build(&values, 1)
        };
        let raw = encode(&fields);
        let parsed = parse_direct_combo_report(&raw, &fix::fix_parse(&raw)).unwrap().unwrap();
        assert_eq!(parsed.contract.combo_legs.len(), 2);
        assert_eq!(parsed.contract.combo_legs[0].con_id, parsed.contract.combo_legs[1].con_id);
        assert_ne!(parsed.contract.combo_legs[0].action, parsed.contract.combo_legs[1].action);
        // Keep a single exact leg without imposing Daytrader's package policy.
        fields.retain(|(tag, _)| ![6080, 6081, 6082].contains(tag));
        replace_tag(&mut fields, 6079, "1");
        fields.extend([(6080, "756733".into()), (6081, "2".into()), (6082, "1".into())]);
        let raw = encode(&fields);
        let parsed = parse_direct_combo_report(&raw, &fix::fix_parse(&raw)).unwrap().unwrap();
        assert_eq!(parsed.contract.combo_legs.len(), 1);
        assert_eq!(parsed.contract.combo_legs[0].ratio, 2);
    }

    #[test]
    fn completed_cold_direct_bag_rejects_incomplete_or_ambiguous_native_terms() {
        let base = direct_report(true);
        let mut invalid = Vec::new();
        for missing in [6079, 6080, 6081, 6082] {
            let mut fields = base.clone();
            fields.retain(|(tag, _)| *tag != missing);
            invalid.push(fields);
        }
        for (tag, value) in [
            (6079, "3"), (6079, "2"), (100, "BEST"), (100, "SMART"),
            (100, "ISE"), (616, "CBOE"), (6081, "1"), (6082, "0"),
            (6879, "NaN"), (6879, "invalid"), (6879, "0.25"), (6087, "O"),
        ] {
            let mut fields = base.clone();
            fields.push((tag, value.into()));
            invalid.push(fields);
        }
        let mut missing_route = base.clone();
        missing_route.retain(|(tag, _)| ![6004, 6210, 100, 207].contains(tag));
        invalid.push(missing_route);
        for values in [
            ["CBOE", "ISE"],
            ["", ""],
        ] {
            let mut fields = base.clone();
            fields.extend(values.map(|value| (616, value.into())));
            invalid.push(fields);
        }
        let mut mixed_prices = base;
        mixed_prices.extend([(6879, f64::MAX.to_string()), (6879, "0.25".into())]);
        invalid.push(mixed_prices);
        for (index, fields) in invalid.into_iter().enumerate() {
            assert!(cold_direct_history(&[fields]).is_err(), "accepted variant {index}");
        }
    }

    #[test]
    fn completed_known_combo_preserves_captured_fill_topology_and_signed_net_limits() {
        for side in ["1", "2"] {
            let (fields, prices) = captured_combo(true, side);
            assert!(prices.is_empty());
            let known = known_combo(&fields, vec![f64::MAX; 2]);
            let rows = finish_combo_query(&fields, Some(&known), true).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].contract, known.contract);
            assert_eq!(rows[0].order.lmt_price, known.order.lmt_price);
            assert_eq!(rows[0].order.action, known.order.action);
            assert_eq!(rows[0].order.filled_quantity, 1.0);
            assert_eq!(rows[0].order_state.status, "Filled");
        }
        for price in ["-1.25", "0", "1.25"] {
            let (mut fields, _) = captured_combo(true, "1");
            fields.insert(44, price.into());
            let known = known_combo(&fields, vec![f64::MAX; 2]);
            let rows = finish_combo_query(&fields, Some(&known), true).unwrap();
            assert_eq!(rows[0].order.lmt_price, price.parse::<f64>().unwrap());
        }
    }

    #[test]
    fn completed_known_combo_preserves_captured_per_leg_limits_without_inventing_net() {
        let (fields, prices) = captured_combo(false, "1");
        assert_eq!(prices, vec![721.35, 794.5]);
        assert_eq!(fields[&44], "-73.15");
        let known = known_combo(&fields, prices.clone());
        let rows = finish_combo_query(&fields, Some(&known), true).unwrap();
        assert_eq!(rows[0].order_state.status, "Cancelled");
        assert_eq!(rows[0].order.order_combo_legs, prices);
        assert_eq!(rows[0].order.lmt_price, f64::MAX);
        let known = known_combo(&fields, vec![0.0, 1.25]);
        let rows = finish_combo_query(&fields, Some(&known), true).unwrap();
        assert_eq!(rows[0].order.order_combo_legs, vec![0.0, 1.25]);
        assert_eq!(rows[0].order.lmt_price, f64::MAX);
        for prices in [
            vec![1.25],
            vec![1.25, f64::MAX],
            vec![f64::NAN, 1.25],
            vec![1.25, f64::INFINITY],
        ] {
            let known = known_combo(&fields, prices);
            assert!(finish_combo_query(&fields, Some(&known), true).is_err());
        }
    }

    #[test]
    fn completed_combo_requires_exact_per_order_view_not_shared_bag_contract_cache() {
        let (fields, _) = captured_combo(true, "1");
        let cached = known_combo(&fields, vec![f64::MAX; 2]);
        for known in [None, Some(&cached)] {
            assert_eq!(
                finish_combo_query(&fields, known, false).unwrap_err(),
                "Completed-order BAG snapshot has no known per-order combo"
            );
        }
        let mut other_order = cached.clone();
        other_order.order.perm_id += 1;
        assert!(finish_combo_query(&fields, Some(&other_order), true).is_err());
        for price in [f64::MAX, f64::NAN, f64::INFINITY] {
            let mut known = cached.clone();
            known.order.lmt_price = price;
            assert!(finish_combo_query(&fields, Some(&known), true).is_err());
        }
    }

    #[test]
    fn completed_combo_receiver_requires_exact_per_order_view_not_cached_bag_legs() {
        use crate::bridge::ComboView;
        use crate::engine::combo::{Combo, ComboOrder, Leg};
        use crate::types::{Order, PRICE_SCALE, Side};

        for exact_view in [false, true] {
            let (mut fields, _) = captured_combo(true, "1");
            fields.insert(1, "DU123456".into());
            let contract = known_combo(&fields, vec![f64::MAX; 2]).contract;
            let order_id = perm_id_from_clord_id(&fields[&11]);
            let (mut ccp, mut context, shared) =
                (CcpState::new(), Context::new(), SharedState::new());
            let instrument = context.register_instrument(contract.con_id);
            context.insert_order(Order::new(
                order_id,
                instrument,
                Side::Buy,
                1,
                23 * PRICE_SCALE,
                b'2',
                b'0',
                0,
            ));
            // Both branches have cached BAG legs. Only one has the exact
            // per-order authority established by local combo submission.
            shared
                .reference
                .cache_contract(contract.con_id, contract.clone());
            if exact_view {
                context.combos.orders.insert(
                    order_id,
                    ComboOrder {
                        combo: Combo {
                            bag_con_id: contract.con_id,
                            exchange: "SMART".into(),
                            routed: "BEST".into(),
                            currency: "USD".into(),
                            trading_class: "COMB".into(),
                            legs: [(756_733, "SPY", true), (320_227_571, "QQQ", false)]
                                .into_iter()
                                .map(|(con_id, symbol, buy)| Leg {
                                    con_id,
                                    ratio: 1,
                                    buy,
                                    exchange: "SMART".into(),
                                    symbol: symbol.into(),
                                    sec_type: "STK".into(),
                                    currency: "USD".into(),
                                    multiplier: 1.0,
                                })
                                .collect(),
                            multiplier: 1.0,
                        },
                        spec: Default::default(),
                        leg_prices: Vec::new(),
                        price: None,
                        cum_qty: 0,
                        leaves_qty: crate::types::QTY_SCALE,
                        avg_price: 0,
                        last_price: 0,
                    },
                );
                shared.orders.set_combo_view(
                    order_id,
                    ComboView {
                        contract: contract.clone(),
                        leg_prices: vec![f64::MAX; 2],
                    },
                );
            }
            ccp.completed_history = active();
            deliver(&mut ccp, &mut context, &shared, &fields);
            deliver(&mut ccp, &mut context, &shared, &capture()[2]);
            let result = shared
                .orders
                .drain_completed_history_replies()
                .remove(0)
                .result;
            if exact_view {
                let rows = result.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].contract, contract);
                assert_eq!(rows[0].order_state.status, "Filled");
                assert_eq!(rows[0].order.filled_quantity, 1.0);
            } else {
                assert_eq!(
                    result.unwrap_err(),
                    "Completed-order BAG has unsupported or conflicting direct route"
                );
            }
            assert!(shared.orders.drain_fills().is_empty());
            assert_eq!(context.position_fixed(instrument), 0);
            assert_eq!(shared.portfolio.position_fixed(instrument), 0);
        }
    }

    #[test]
    fn completed_known_combo_sparse_cancel_keeps_exact_amended_terms() {
        let (mut fields, prices) = captured_combo(false, "1");
        fields.insert(39, "0".into());
        let mut working = known_combo(&fields, prices.clone());
        working.order_state.status = "Submitted".into();
        let mut history = active();
        let shared = SharedState::new();
        history.collect(&fields, "DUXXXXXXX", Some(&working), true, &Ok(None));
        let cancel: HashMap<u32, String> = [
            (1, fields[&1].clone()),
            (11, fields[&11].clone()),
            (20, "3".into()),
            (39, "4".into()),
            (60, "20260926-20:26:00".into()),
            (14, "0".into()),
            (12, "0.25".into()),
        ]
        .into();
        let mut cancelled = working.clone();
        cancelled.order_state.status = "Cancelled".into();
        cancelled.order_state.commission_and_fees = 0.25;
        cancelled.report_time = Some(cancel[&60].clone());
        history.collect(&cancel, "DUXXXXXXX", Some(&cancelled), true, &Ok(None));
        history.report(&capture()[2], &shared, "DUXXXXXXX");
        let rows = shared
            .orders
            .drain_completed_history_replies()
            .remove(0)
            .result
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].contract, working.contract);
        assert_eq!(rows[0].order.order_combo_legs, prices);
        assert_eq!(rows[0].order.lmt_price, f64::MAX);
        assert_eq!(rows[0].order_state.status, "Cancelled");
        assert_eq!(rows[0].order_state.commission_and_fees, 0.25);
        assert!(shared.orders.drain_fills().is_empty());
    }

    #[test]
    fn completed_combo_leg_status_cannot_replace_its_parent_or_hide_unsupported_history() {
        for known_parent in [false, true] {
            let (fields, _) = captured_combo(true, "1");
            let known = known_combo(&fields, vec![f64::MAX; 2]);
            let mut history = active();
            let shared = SharedState::new();
            history.collect(
                &fields,
                "DUXXXXXXX",
                known_parent.then_some(&known),
                known_parent,
                &Ok(None),
            );
            let mut leg = fields.clone();
            for (tag, value) in [
                (167, "CS"),
                (6008, "756733"),
                (442, "2"),
                (6013, "0:2"),
                (6400, fields[&17].as_str()),
            ] {
                leg.insert(tag, value.into());
            }
            history.report(&leg, &shared, "DUXXXXXXX");
            history.report(&capture()[2], &shared, "DUXXXXXXX");
            let result = shared
                .orders
                .drain_completed_history_replies()
                .remove(0)
                .result;
            if known_parent {
                let rows = result.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].contract, known.contract);
            } else {
                assert!(result.is_err());
            }
            leg.remove(&6013);
            assert!(finish_combo_query(&leg, None, false).is_err());
        }
    }

    #[test]
    fn completed_oca_cancel_preserves_explicit_zero_current_quantity() {
        let mut state = active();
        let shared = SharedState::new();
        assert_eq!(
            state.report(&zero_quantity_oca_cancel(), &shared, "DU123456"),
            HistoryReport::Row
        );
        assert_eq!(
            state.report(&capture()[2], &shared, "DU123456"),
            HistoryReport::End
        );
        let result = shared
            .orders
            .drain_completed_history_replies()
            .remove(0)
            .result
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].order_state.status, "Cancelled");
        assert_eq!(result[0].order.total_quantity, 0.0);
        assert_eq!(result[0].order.filled_quantity, 0.0);
        assert_eq!(result[0].order.order_type, "STP");
        assert_eq!(result[0].order.aux_price, 100.0);
        assert_eq!(result[0].order.oca_group, "fixture.oca");
        assert_eq!(result[0].order.oca_type, 3);
        assert_eq!(result[0].order.parent_id, 1_234_567_890_001);
        assert!(result[0].parent_id_known);
        assert!(shared.orders.drain_fills().is_empty());
        assert!(shared.orders.drain_order_updates().is_empty());
        assert!(shared.orders.drain_open_orders().is_empty());
    }

    #[test]
    fn option_lifecycle_fails_only_an_eligible_own_or_unattributed_completed_query() {
        for (reported_account, status, expect_failure) in [
            (None, "3", true),
            (Some(""), "3", true),
            (Some("DU123456"), "3", true),
            (Some("OTHER"), "3", false),
            (Some("DU123456"), "0", false),
        ] {
            let shared = SharedState::new();
            let mut state = active();
            let mut lifecycle = working_row(1, "20260102-12:00:00");
            lifecycle.insert(20, status.into());
            lifecycle.insert(6809, "101".into());
            lifecycle.remove(&1);
            if let Some(reported_account) = reported_account {
                lifecycle.insert(1, reported_account.into());
            }
            state.reject_option_lifecycle(&lifecycle, "DU123456");
            assert_eq!(
                state.report(&capture()[2], &shared, "DU123456"),
                HistoryReport::End
            );
            let reply = shared
                .orders
                .drain_completed_history_replies()
                .pop()
                .unwrap();
            match (reply.result, expect_failure) {
                (Err(message), true) => assert_eq!(
                    message,
                    "Completed-order history contains unsupported option lifecycle report"
                ),
                (Ok(rows), false) => assert!(rows.is_empty()),
                (result, expected) => panic!("unexpected result {result:?}, failure={expected}"),
            }
        }
    }

    #[test]
    fn completed_zero_quantity_requires_explicit_unfilled_cancellation() {
        for (tag, value) in [
            (38, None),
            (38, Some("-1")),
            (38, Some("NaN")),
            (38, Some("inf")),
            (14, None),
            (14, Some("1")),
            (14, Some("-1")),
            (14, Some("NaN")),
            (151, None),
            (151, Some("1")),
            (151, Some("-1")),
            (151, Some("NaN")),
            (39, Some("2")),
            (39, Some("8")),
            (39, Some("C")),
            (6008, None),
            (15, None),
            (167, Some("BAG")),
            (54, Some("0")),
            (99, None),
            (8302, Some("1")),
        ] {
            let mut row = zero_quantity_oca_cancel();
            row.remove(&tag);
            if let Some(value) = value {
                row.insert(tag, value.into());
            }
            let mut reducer = Reducer::default();
            reducer.push(&row, "DU123456");
            assert!(reducer.finish(false).is_err(), "accepted {tag}={value:?}");
        }
    }

    #[test]
    fn completed_zero_quantity_cancel_keeps_known_terms_without_reopening_order() {
        let (mut ccp, mut context, shared) = live_context();
        deliver(
            &mut ccp,
            &mut context,
            &shared,
            &working_row(0, "20260102-12:30:00"),
        );
        ccp.completed_history = active();
        let mut cancel = zero_quantity_oca_cancel();
        cancel.insert(11, "42.0".into());
        cancel.insert(60, "20260102-12:40:00".into());
        deliver(&mut ccp, &mut context, &shared, &cancel);
        assert!(context.order(42).is_none());
        assert_eq!(context.finished_status(42), Some(OrderStatus::Cancelled));
        let result = finish_query(&mut ccp, &mut context, &shared);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].order_state.status, "Cancelled");
        assert_eq!(result[0].order.total_quantity, 3.0);
        assert_eq!(result[0].order.filled_quantity, 0.0);
        assert!(context.order(42).is_none());
        assert!(shared.orders.drain_fills().is_empty());
    }

    fn live_context() -> (CcpState, Context, SharedState) {
        use crate::types::{Order, PRICE_SCALE, Side};
        let mut context = Context::new();
        let instrument = context.register_instrument(265598);
        context.insert_order(Order::new(
            42,
            instrument,
            Side::Buy,
            3,
            2 * PRICE_SCALE,
            b'2',
            b'0',
            0,
        ));
        (CcpState::new(), context, SharedState::new())
    }

    fn deliver(
        ccp: &mut CcpState,
        context: &mut Context,
        shared: &SharedState,
        row: &HashMap<u32, String>,
    ) {
        let fields: Vec<_> = row
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

    fn finish_query(
        ccp: &mut CcpState,
        context: &mut Context,
        shared: &SharedState,
    ) -> Vec<RichOrderInfo> {
        deliver(ccp, context, shared, &capture()[2]);
        shared
            .orders
            .drain_completed_history_replies()
            .remove(0)
            .result
            .unwrap()
    }

    #[test]
    fn concurrent_history_preserves_ack_then_full_status_before_or_during_query() {
        for query_before_ack in [true, false] {
            let (mut ccp, mut context, shared) = live_context();
            let full = working_row(0, "20260102-12:30:00");
            let mut ack = full.clone();
            ack.insert(20, "0".into());
            ack.remove(&6107);
            if query_before_ack {
                ccp.completed_history = active();
            }
            deliver(&mut ccp, &mut context, &shared, &ack);
            assert!(!shared.orders.get_order_info(42).unwrap().parent_id_known);
            if !query_before_ack {
                ccp.completed_history = active();
            }
            deliver(&mut ccp, &mut context, &shared, &full);
            let current = shared.orders.get_order_info(42).unwrap();
            assert!(current.parent_id_known);
            assert_eq!(current.order.parent_id, 41);
            assert_eq!(
                context.last_clord.get(&42).map(String::as_str),
                Some("42.0")
            );
            assert_eq!(
                shared
                    .orders
                    .drain_order_updates()
                    .last()
                    .unwrap()
                    .parent_id,
                41
            );
            assert!(shared.orders.drain_fills().is_empty());
            assert!(finish_query(&mut ccp, &mut context, &shared).is_empty());
        }
    }

    #[test]
    fn concurrent_history_isolates_unknown_broker_even_with_colliding_client_id() {
        let (mut ccp, mut context, shared) = live_context();
        ccp.completed_history = active();
        let mut row = working_row(1, "20260102-12:30:00");
        row.insert(11, "999.1".into());
        row.insert(39, "4".into());
        row.insert(32, "3".into());
        deliver(&mut ccp, &mut context, &shared, &row);
        assert!(context.recovered_keys.is_empty());
        assert!(context.last_clord.is_empty());
        assert!(context.order(42).is_some());
        assert!(shared.orders.get_order_info(42).is_none());
        assert!(shared.orders.get_order_info(999).is_none());
        assert!(shared.orders.drain_fills().is_empty());
        let history = finish_query(&mut ccp, &mut context, &shared);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].order.perm_id, 999);
        assert_eq!(history[0].order.order_id, 42);
    }

    #[test]
    fn concurrent_history_scope_does_not_suppress_known_other_account_reports() {
        let (mut ccp, mut context, shared) = live_context();
        let mut row = working_row(0, "20260102-12:30:00");
        row.insert(1, "DU_OTHER".into());
        let mut ack = row.clone();
        ack.insert(20, "0".into());
        ack.remove(&6107);
        deliver(&mut ccp, &mut context, &shared, &ack);
        assert!(!shared.orders.get_order_info(42).unwrap().parent_id_known);
        ccp.completed_history = active();
        deliver(&mut ccp, &mut context, &shared, &row);
        let full = shared.orders.get_order_info(42).unwrap();
        assert!(full.parent_id_known);
        assert_eq!(full.order.parent_id, 41);
        assert_eq!(full.order.account, "DU_OTHER");
        row.insert(39, "4".into());
        deliver(&mut ccp, &mut context, &shared, &row);
        assert_eq!(context.finished_status(42), Some(OrderStatus::Cancelled));
        // The same account's unknown broker rows remain query-local, without
        // alias allocation or contaminating this other account's request.
        row.insert(11, "999.0".into());
        deliver(&mut ccp, &mut context, &shared, &row);
        assert!(context.recovered_keys.is_empty());
        assert!(shared.orders.get_order_info(999).is_none());
        assert!(finish_query(&mut ccp, &mut context, &shared).is_empty());
    }

    #[test]
    fn concurrent_history_older_snapshots_cannot_rollback_newer_modify_or_fill() {
        let (mut ccp, mut context, shared) = live_context();
        let current = working_row(2, "20260102-12:30:00");
        deliver(&mut ccp, &mut context, &shared, &current);
        let mut fill = current.clone();
        for (tag, value) in [
            (20, "0"),
            (150, "F"),
            (39, "1"),
            (60, "20260102-12:31:00"),
            (17, "NEWER.1"),
            (32, "1"),
            (31, "2"),
            (14, "1"),
            (151, "2"),
            (6, "2"),
        ] {
            fill.insert(tag, value.into());
        }
        deliver(&mut ccp, &mut context, &shared, &fill);
        let before = format!("{:?}", shared.orders.get_order_info(42).unwrap());
        ccp.completed_history = active();
        let mut old = working_row(3, "20260102-12:20:00");
        old.insert(44, "9".into());
        old.insert(32, "2".into());
        deliver(&mut ccp, &mut context, &shared, &old);
        old.insert(11, "42.1".into());
        old.insert(60, "20260102-12:40:00".into());
        deliver(&mut ccp, &mut context, &shared, &old);
        assert_eq!(
            format!("{:?}", shared.orders.get_order_info(42).unwrap()),
            before
        );
        assert_eq!(
            context.last_clord.get(&42).map(String::as_str),
            Some("42.2")
        );
        assert_eq!(
            context.order(42).unwrap().status,
            OrderStatus::PartiallyFilled
        );
        assert_eq!(context.position_fixed(0), crate::types::QTY_SCALE);
        assert_eq!(shared.orders.drain_fills().len(), 1);
        assert!(finish_query(&mut ccp, &mut context, &shared).is_empty());
    }

    #[test]
    fn concurrent_history_cancel_keeps_amended_terms_but_updates_totals_and_fees() {
        for version in [1, 2, 3] {
            let (mut ccp, mut context, shared) = live_context();
            let current = working_row(2, "20260102-12:30:00");
            deliver(&mut ccp, &mut context, &shared, &current);
            shared.orders.drain_order_updates();
            ccp.completed_history = active();
            let mut cancel = working_row(version, "20260102-12:40:00");
            for (tag, value) in [
                (39, "4"),
                (44, "1"),
                (14, "1"),
                (151, "2"),
                (6, "1.25"),
                (12, "0.75"),
                (32, "1"),
                (150, "F"),
            ] {
                cancel.insert(tag, value.into());
            }
            deliver(&mut ccp, &mut context, &shared, &cancel);
            assert!(context.order(42).is_none());
            assert_eq!(
                context.last_clord.get(&42).map(String::as_str),
                Some("42.2")
            );
            assert_eq!(context.position_fixed(0), 0);
            assert!(shared.orders.drain_fills().is_empty());
            let update = shared.orders.drain_order_updates().remove(0);
            assert_eq!(update.filled_qty_fixed, crate::types::QTY_SCALE);
            assert_eq!(update.remaining_qty_fixed, 2 * crate::types::QTY_SCALE);
            assert_eq!(update.avg_fill_price, 125 * crate::types::PRICE_SCALE / 100);
            let history = finish_query(&mut ccp, &mut context, &shared);
            assert_eq!(history.len(), 1);
            let live = shared.orders.get_order_info(42).unwrap();
            for info in [&live, &history[0]] {
                assert_eq!(info.order.lmt_price, 2.0);
                assert_eq!(info.order.total_quantity, 3.0);
                assert_eq!(info.order.filled_quantity, 1.0);
                assert_eq!(info.order_state.commission_and_fees, 0.75);
                assert_eq!(info.order_state.status, "Cancelled");
                assert_eq!(info.report_revision, Some(version.max(2)));
            }
        }
    }

    #[test]
    fn concurrent_history_stale_real_print_books_once_without_replacing_current_state() {
        let (mut ccp, mut context, shared) = live_context();
        let mut current = working_row(2, "20260102-12:30:00");
        current.insert(39, "1".into());
        current.insert(14, "2".into());
        deliver(&mut ccp, &mut context, &shared, &current);
        let before = format!("{:?}", shared.orders.get_order_info(42).unwrap());
        ccp.completed_history = active();
        let mut print = working_row(1, "20260102-12:20:00");
        for (tag, value) in [
            (20, "0"),
            (150, "F"),
            (39, "1"),
            (17, "DELAYED.1"),
            (32, "1"),
            (31, "1.50"),
            (14, "1"),
            (151, "0"),
            (6, "1.50"),
        ] {
            print.insert(tag, value.into());
        }
        deliver(&mut ccp, &mut context, &shared, &print);
        deliver(&mut ccp, &mut context, &shared, &print);
        assert_eq!(
            format!("{:?}", shared.orders.get_order_info(42).unwrap()),
            before
        );
        assert_eq!(
            context.last_clord.get(&42).map(String::as_str),
            Some("42.2")
        );
        assert_eq!(context.position_fixed(0), crate::types::QTY_SCALE);
        let fills = shared.orders.drain_fills_with_exec();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].0.price, 150 * crate::types::PRICE_SCALE / 100);
        assert!(fills[0].1.stale_order_state);
        assert!(finish_query(&mut ccp, &mut context, &shared).is_empty());
    }

    #[test]
    fn concurrent_history_stale_rows_keep_terminal_evidence_without_reopening_live_state() {
        for current_terminal in [true, false] {
            let (mut ccp, mut context, shared) = live_context();
            let current = working_row(2, "20260102-12:30:00");
            deliver(&mut ccp, &mut context, &shared, &current);
            if current_terminal {
                let mut cancel = current.clone();
                cancel.insert(39, "4".into());
                cancel.insert(60, "20260102-12:40:00".into());
                deliver(&mut ccp, &mut context, &shared, &cancel);
            }
            let before = format!("{:?}", shared.orders.get_order_info(42).unwrap());
            ccp.completed_history = active();
            let mut old = working_row(1, "20260102-12:20:00");
            old.insert(39, if current_terminal { "0" } else { "4" }.into());
            deliver(&mut ccp, &mut context, &shared, &old);
            assert_eq!(
                format!("{:?}", shared.orders.get_order_info(42).unwrap()),
                before
            );
            assert_eq!(context.order(42).is_none(), current_terminal);
            let history = finish_query(&mut ccp, &mut context, &shared);
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].order_state.status, "Cancelled");
            assert_eq!(
                history[0].order_state.completed_time,
                if current_terminal {
                    "20260102-12:40:00"
                } else {
                    "20260102-12:20:00"
                }
            );
        }
    }

    #[test]
    fn late_no_such_order_after_terminal_retirement_does_not_invent_rich_identity() {
        let (mut ccp, mut context, shared) = live_context();
        let mut cancelled = working_row(0, "20260102-12:30:00");
        cancelled.insert(39, "4".into());
        deliver(&mut ccp, &mut context, &shared, &cancelled);
        shared
            .orders
            .push_completed_history_reply(CompletedHistoryReply {
                connection: "today4".into(),
                result: Ok(Vec::new()),
            });
        let retired = shared.orders.completed_retirement_candidates();
        assert_eq!(retired.len(), 1);
        shared.orders.drain_order_updates();
        shared.orders.drain_completed_history_replies();
        shared.orders.retire_local_completed_orders(retired);
        assert!(shared.orders.get_order_info(42).is_none());
        let missing = [
            (35, "8"),
            (11, "42.0"),
            (20, "3"),
            (150, "8"),
            (39, "8"),
            (38, "0"),
            (14, "0"),
            (151, "0"),
            (58, "No such order"),
            (60, "20260102-12:31:00"),
        ]
        .into_iter()
        .map(|(tag, value)| (tag, value.to_string()))
        .collect();
        deliver(&mut ccp, &mut context, &shared, &missing);
        assert!(shared.orders.get_order_info(42).is_none());
        assert_eq!(context.finished_status(42), Some(OrderStatus::Cancelled));
        assert!(shared.orders.drain_open_orders().is_empty());
        assert!(shared.orders.drain_fills().is_empty());
    }

    #[test]
    fn no_such_order_preserves_open_partial_fill_status_totals() {
        let (mut ccp, mut context, shared) = live_context();
        let mut partial = working_row(0, "20260102-12:30:00");
        partial.insert(39, "1".into());
        partial.insert(14, "1".into());
        partial.insert(6, "2".into());
        deliver(&mut ccp, &mut context, &shared, &partial);
        shared.orders.drain_order_updates();
        let missing = [
            (35, "8"),
            (11, "42.0"),
            (20, "3"),
            (150, "8"),
            (39, "8"),
            (38, "0"),
            (14, "0"),
            (151, "0"),
            (6, "0"),
            (58, "No such order"),
            (60, "20260102-12:31:00"),
        ]
        .into_iter()
        .map(|(tag, value)| (tag, value.to_string()))
        .collect();
        deliver(&mut ccp, &mut context, &shared, &missing);
        let update = shared.orders.drain_order_updates().remove(0);
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.filled_qty_fixed, crate::types::QTY_SCALE);
        assert_eq!(update.avg_fill_price, 2 * crate::types::PRICE_SCALE);
        let info = shared.orders.get_order_info(42).unwrap();
        assert_eq!(info.order.filled_quantity, 1.0);
        assert_eq!(info.last_exec.avg_price, 2.0);
        assert_eq!(info.order_state.status, "Cancelled");
        assert!(shared.orders.drain_fills().is_empty());
    }

    #[test]
    fn fresh_history_uses_untagged_captured_rows_and_only_exact_end() {
        let shared = SharedState::new();
        let mut state = active();
        let rows = capture();
        assert_eq!(
            state.report(&rows[0], &shared, "DU123456"),
            HistoryReport::Row
        );
        assert_eq!(
            state.report(&rows[1], &shared, "DU123456"),
            HistoryReport::Row
        );
        let mut wrong = rows[2].clone();
        wrong.insert(6556, "today4".into());
        assert_eq!(
            state.report(&wrong, &shared, "DU123456"),
            HistoryReport::Unrelated
        );
        assert!(shared.orders.drain_completed_history_replies().is_empty());
        assert_eq!(
            state.report(&rows[2], &shared, "DU123456"),
            HistoryReport::End
        );
        let results = shared
            .orders
            .drain_completed_history_replies()
            .remove(0)
            .result
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[1].order.perm_id, 1_234_567_890_002);
        assert_eq!(results[1].order.lmt_price, 2.0);
        assert_eq!(results[1].order_state.completed_status, "Cancelled");
        assert!(shared.orders.drain_fills().is_empty());
        assert!(shared.orders.drain_order_updates().is_empty());
        assert!(shared.orders.drain_open_orders().is_empty());
        assert!(shared.orders.get_order_info(1_234_567_890_002).is_none());
    }

    #[test]
    fn live_fills_and_other_end_markers_are_not_consumed_by_history() {
        let mut state = active();
        let shared = SharedState::new();
        let mut report = capture()[0].clone();
        report.insert(20, "0".into());
        report.insert(32, "1".into());
        assert_eq!(
            state.report(&report, &shared, "DU123456"),
            HistoryReport::Unrelated
        );
        report.insert(20, "3".into());
        report.insert(6556, "OtherQuery".into());
        assert_eq!(
            state.report(&report, &shared, "DU123456"),
            HistoryReport::Unrelated
        );
        report.insert(6556, "PT.5".into());
        assert_eq!(
            state.report(&report, &shared, "DU123456"),
            HistoryReport::Row
        );
    }

    #[test]
    fn timed_out_history_keeps_draining_without_success_or_late_accounting() {
        let mut state = active();
        let shared = SharedState::new();
        state.expire(Instant::now() + QUERY_TIMEOUT, &shared);
        assert!(
            shared
                .orders
                .drain_completed_history_replies()
                .remove(0)
                .result
                .is_err()
        );
        assert!(state.active());
        for row in capture() {
            assert_ne!(
                state.report(&row, &shared, "DU123456"),
                HistoryReport::Unrelated
            );
        }
        assert!(!state.active());
        assert!(shared.orders.drain_completed_history_replies().is_empty());
    }

    #[test]
    fn loss_fails_active_and_waiting_queries_and_clears_wire_phase() {
        let mut state = active();
        let shared = SharedState::new();
        state.queue("today4".into(), false, "a".into(), "b".into());
        state.sent_status("123.1");
        state.defer_status("456.2".into());
        state.disconnected(&shared);
        let answers = shared.orders.drain_completed_history_replies();
        assert_eq!(answers.len(), 2);
        assert!(answers.iter().all(|r| r.result.is_err()));
        assert!(!state.active());
        assert!(state.status_pending.is_empty());
        assert!(state.status_deferred.is_empty());
    }

    #[test]
    fn reducer_uses_broker_identity_not_colliding_api_ids_and_filters_origins() {
        let mut rows = capture();
        rows[0].insert(6121, "7".into());
        rows[1].insert(6121, "7".into());
        let mut reducer = Reducer::default();
        for row in &rows[..2] {
            reducer.push(row, "DU123456");
        }
        assert_eq!(reducer.finish(true).unwrap().len(), 2);
        for (id, expected) in [("0", 0), ("2147483647", 0), ("-1", 1), ("7", 1)] {
            let mut row = rows[0].clone();
            row.remove(&6088);
            row.insert(6121, id.into());
            let mut reducer = Reducer::default();
            reducer.push(&row, "DU123456");
            assert_eq!(reducer.finish(true).unwrap().len(), expected, "{id}");
        }
    }

    #[test]
    fn reducer_rejects_older_time_before_revision_and_retains_terms_on_lower_cancel() {
        let mut current = capture()[1].clone();
        current.insert(39, "0".into());
        let mut earlier = current.clone();
        earlier.insert(11, "1234567890002.3".into());
        earlier.insert(60, "20260102-12:00:00".into());
        earlier.insert(44, "3".into());
        let mut cancel = current.clone();
        cancel.insert(11, "1234567890002.1".into());
        cancel.insert(39, "4".into());
        cancel.insert(60, "20260102-12:40:00".into());
        cancel.insert(44, "1".into());
        let mut reducer = Reducer::default();
        for row in [current, earlier, cancel] {
            reducer.push(&row, "DU123456");
        }
        let result = reducer.finish(false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].order.lmt_price, 2.0);
        assert_eq!(result[0].order_state.completed_time, "20260102-12:40:00");
        assert_eq!(result[0].order_state.status, "Cancelled");
    }

    #[test]
    fn reducer_rejects_missing_terms_instead_of_reporting_default_order() {
        let mut row = capture()[0].clone();
        row.remove(&44);
        let mut reducer = Reducer::default();
        reducer.push(&row, "DU123456");
        assert!(reducer.finish(false).is_err());
        let mut reducer = Reducer::default();
        reducer.push(&capture()[0], "DIFFERENT_ACCOUNT");
        assert!(reducer.finish(false).is_err());
    }

    #[test]
    fn utc_range_validates_calendar_time_and_order() {
        use crate::client_core::valid_completed_history_range as valid;
        assert!(valid("20261003-00:00:00", "20261005-23:59:59"));
        assert!(!valid("20260230-00:00:00", "20261005-23:59:59"));
        assert!(!valid("20261003-25:00:00", "20261005-23:59:59"));
        assert!(!valid("20261005-00:00:00", "20261003-23:59:59"));
        assert!(!valid("20261003", "20261005-23:59:59"));
    }

    #[test]
    fn sparse_cancels_at_any_revision_preserve_terms_but_replace_effective_time() {
        for revision in [1, 2, 3] {
            let mut row = capture()[1].clone();
            row.insert(39, "0".into());
            row.insert(6699, "20260102-12:39:26".into());
            let cancel = HashMap::from([
                (1, "DU123456".into()),
                (11, format!("1234567890002.{revision}")),
                (39, "4".into()),
                (60, "20260102-12:40:00".into()),
            ]);
            let mut reducer = Reducer::default();
            reducer.push(&row, "DU123456");
            reducer.push(&cancel, "DU123456");
            let result = reducer.finish(false).unwrap().remove(0);
            assert_eq!(result.order.lmt_price, 2.0);
            assert_eq!(result.order_state.completed_time, "20260102-12:40:00");
        }
    }

    #[test]
    fn filled_gtd_snapshot_preserves_cumulative_quantity_and_expiry_without_execution() {
        let mut row = capture()[1].clone();
        row.insert(39, "2".into());
        row.insert(14, "1".into());
        row.insert(59, "6".into());
        row.insert(126, "20261005-20:00:00".into());
        let mut reducer = Reducer::default();
        reducer.push(&row, "DU123456");
        let result = reducer.finish(false).unwrap().remove(0);
        assert_eq!(result.order.filled_quantity, 1.0);
        assert_eq!(result.order.good_till_date, "20261005 20:00:00 UTC");
        assert_eq!(result.order.tif, "GTD");
        for quantity in [None, Some("invalid"), Some("0"), Some("NaN")] {
            row.remove(&14);
            if let Some(quantity) = quantity {
                row.insert(14, quantity.into());
            }
            let mut reducer = Reducer::default();
            reducer.push(&row, "DU123456");
            assert!(reducer.finish(false).is_err());
        }
    }

    #[test]
    fn archive_retirement_does_not_remove_a_reused_open_order_id() {
        use crate::types::CompletedOrder;
        let shared = SharedState::new();
        let mut reducer = Reducer::default();
        reducer.push(&capture()[0], "DU123456");
        let mut rich = reducer.finish(false).unwrap().remove(0);
        shared.orders.push_order_info(1, rich.clone());
        rich.order_state.status = "Submitted".into();
        shared.orders.push_order_info(2, rich);
        for id in [1, 2] {
            shared.orders.push_completed_order(CompletedOrder {
                order_id: id,
                instrument: 0,
                status: OrderStatus::Cancelled,
                filled_qty_fixed: 0,
                timestamp_ns: 0,
            });
        }
        shared
            .orders
            .push_completed_history_reply(CompletedHistoryReply {
                connection: "test".into(),
                result: Ok(Vec::new()),
            });
        let retired = shared.orders.completed_retirement_candidates();
        // A later terminal update's rich data must also survive, not just
        // an open reused ID: its fill may arrive after this dispatch's drain.
        let mut later = shared.orders.get_order_info(1).unwrap();
        later.order.perm_id = 999;
        shared.orders.push_order_info(1, later);
        shared.orders.retire_local_completed_orders(retired);
        assert_eq!(shared.orders.get_order_info(1).unwrap().order.perm_id, 999);
        shared.orders.push_completed_order(CompletedOrder {
            order_id: 1,
            instrument: 0,
            status: OrderStatus::Cancelled,
            filled_qty_fixed: 0,
            timestamp_ns: 0,
        });
        let retired = shared.orders.completed_retirement_candidates();
        shared.orders.retire_local_completed_orders(retired);
        assert!(shared.orders.get_order_info(1).is_none());
        assert!(shared.orders.get_order_info(2).is_some());
        assert!(shared.orders.drain_completed_orders().is_empty());
    }

    fn socket() -> (Option<Connection>, std::net::TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        (Some(Connection::new_raw(client).unwrap()), server)
    }

    fn sent(server: &mut std::net::TcpStream) -> String {
        use std::io::Read;
        let mut all = Vec::new();
        let mut bytes = [0; 8192];
        while let Ok(n) = server.read(&mut bytes) {
            if n == 0 {
                break;
            }
            all.extend_from_slice(&bytes[..n]);
        }
        String::from_utf8(all).unwrap().replace('\x01', "|")
    }

    #[test]
    fn wire_query_waits_for_bootstrap_and_per_order_status_then_serializes_deferred_h() {
        let mut ccp = CcpState::new();
        let mut context = Context::new();
        let shared = SharedState::new();
        let mut hb = HeartbeatState::new();
        let (mut conn, mut peer) = socket();
        ccp.completed_history.queue(
            "today4".into(),
            false,
            "20260102-00:00:00".into(),
            "20260102-23:59:59".into(),
        );
        shared.orders.begin_execution_history("today4");
        shared.orders.set_open_orders_held(true);
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        assert!(sent(&mut peer).is_empty());
        shared.orders.complete_execution_history("today4");
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        assert!(sent(&mut peer).is_empty());
        shared.orders.set_open_orders_held(false);
        ccp.completed_history.sent_status("1234567890001.1");
        ccp.completed_history.sent_status("1234567890001.2");
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        assert!(sent(&mut peer).is_empty());
        ccp.completed_history.status_reply(&capture()[0]);
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        assert!(
            sent(&mut peer).is_empty(),
            "second per-order H is still outstanding"
        );
        ccp.completed_history.status_reply(&capture()[0]);
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        let request = sent(&mut peer);
        assert!(request.contains("35=H|"));
        assert!(request.contains(
            "6533=1|6536=20260102-00:00:00|6537=20260102-23:59:59|6556=IBX.Completed.1|"
        ));
        assert!(!request.contains("6471="));
        ccp.completed_history.ensure_status("987.2");
        ccp.completed_history.ensure_status("987.2");
        ccp.completed_history.ensure_status("987.3");
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        assert!(sent(&mut peer).is_empty());
        let mut end = capture()[2].clone();
        end.insert(6556, "IBX.Completed.1".into());
        assert_eq!(
            ccp.completed_history.report(&end, &shared, "DU123456"),
            HistoryReport::End
        );
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        let request = sent(&mut peer);
        assert_eq!(request.matches("35=H|").count(), 1);
        assert!(request.contains("11=987.2|55=*|54=*|6471=1|1=DU123456|"));
        assert!(ccp.completed_history.status_pending.contains_key(&987));
        ccp.completed_history.ensure_status("987.3");
        ccp.progress_completed_history(
            &mut conn,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        assert!(sent(&mut peer).is_empty(), "one native order already has a status request");
    }

    #[test]
    fn history_report_does_not_touch_live_context_but_real_fill_still_counts() {
        use crate::types::{Order, PRICE_SCALE, QTY_SCALE, Side};
        let mut ccp = CcpState::new();
        ccp.completed_history = active();
        let shared = SharedState::new();
        let mut context = Context::new();
        let instrument = context.register_instrument(265598);
        context.insert_order(Order::new(
            42,
            instrument,
            Side::Buy,
            1,
            PRICE_SCALE,
            b'2',
            b'0',
            0,
        ));
        let mut hb = HeartbeatState::new();
        for row in capture() {
            let mut row = row;
            if row.get(&11).map(String::as_str) != Some("*") {
                // Status snapshots can carry cumulative execution fields;
                // they are never live fills, even when lastShares is nonzero.
                row.insert(32, "1".into());
                row.insert(14, "1".into());
            }
            let fields: Vec<_> = row
                .iter()
                .map(|(tag, value)| (*tag, value.as_str()))
                .collect();
            ccp.process_ccp_message(
                &fix::fix_build(&fields, 1),
                &mut None,
                &mut context,
                &shared,
                &None,
                &mut hb,
                "DU123456",
            );
        }
        assert!(shared.orders.drain_fills().is_empty());
        assert!(context.order(42).is_some());
        assert!(context.order(1_234_567_890_001).is_none());
        ccp.completed_history = active();
        ccp.process_ccp_message(
            &fix::fix_build(
                &[
                    (35, "8"),
                    (11, "42.0"),
                    (20, "0"),
                    (150, "F"),
                    (39, "2"),
                    (17, "LIVE.1"),
                    (32, "1"),
                    (31, "1.0"),
                    (14, "1"),
                    (151, "0"),
                    (6, "1"),
                    (54, "1"),
                    (6008, "265598"),
                    (1, "DU123456"),
                ],
                2,
            ),
            &mut None,
            &mut context,
            &shared,
            &None,
            &mut hb,
            "DU123456",
        );
        let fills = shared.orders.drain_fills();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty_fixed, QTY_SCALE);
        assert_eq!(fills[0].order_id, 42);
    }
}

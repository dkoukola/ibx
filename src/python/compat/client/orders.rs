//! Order placement, cancellation, open orders, executions, completed orders.

use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::api::types::{
    Contract as ApiContract, Order as ApiOrder, ExecutionFilter,
};
use crate::client_core::{ClientCore, ModifyPlan};
use crate::bridge::SharedState;
use crate::types::*;
use super::{send_cmd, EClient};
use super::super::contract::{Contract, Order, Execution};

#[pymethods]
impl EClient {
    /// Place an order.
    fn place_order(&self, py: Python<'_>, order_id: i64, contract: &Contract, order: &Order) -> PyResult<()> {
        // Convert and validate order params first (fail fast, no connection needed)
        let mut api_order = order.to_api();
        api_order.conditions = order.convert_conditions(py);
        api_order.order_combo_legs = order.convert_order_combo_legs(py);
        // The contract with its combo legs (ibx#470).
        let mut full_contract = contract.to_api();
        full_contract.combo_legs = contract.convert_combo_legs(py);
        // The reference's other names for an order type (ibx#469).
        if let Some(name) = ClientCore::canonical_order_type(&api_order.order_type) {
            api_order.order_type = name.to_string();
        }
        // GTX and NMIN go out as GTC, as the reference (ibx#307).
        if let Some(name) = ClientCore::canonical_tif(&api_order.tif) {
            api_order.tif = name.to_string();
        }
        ClientCore::validate_order(&api_order)
            .map_err(|e| PyRuntimeError::new_err(e))?;
        ClientCore::validate_order_contract(&contract.sec_type)
            .map_err(|e| PyRuntimeError::new_err(e))?;
        // After the checks above, which refuse an invalid order even with no
        // connection (ibx#115).
        if let Some(r) = self.not_connected(order_id) { return r; }

        let tx = self.tx()?;

        let oid = if order_id > 0 {
            order_id
        } else {
            self.next_order_id.fetch_add(1, Ordering::Relaxed)
        };

        // Warnings the reference sends while it reads the order (ibx#416).
        let shared = self.shared_state()?;
        for (code, message) in ClientCore::implied_zone_warnings(&api_order) {
            shared.orders.push_order_error(oid, code, message);
        }
        // Refused before sending, like the reference: error() only.
        let session_account = self.account_id.lock().unwrap().clone().unwrap_or_default();
        let contract_zone = shared.reference.time_zone_id(contract.con_id);
        if let Some((code, message)) = ClientCore::refusal_before_sending(&api_order)
            .or_else(|| ClientCore::algo_definition_refusal(&api_order, &contract.exchange, &shared.reference))
            .or_else(|| ClientCore::account_config_refusal(
                &api_order, shared.reference.account_features().as_deref(), &session_account))
            .or_else(|| ClientCore::good_till_date_refusal(&api_order, contract_zone.as_deref()))
            .or_else(|| ClientCore::condition_time_zone_refusal(&api_order, contract_zone.as_deref()))
            .or_else(|| ClientCore::price_refusal(&api_order))
            .or_else(|| self.core.refusal_for_order_id(oid, &api_order))
        {
            shared.orders.push_order_error(oid, code, message);
            return Ok(());
        }
        // A combo (BAG) order, read and checked as the reference reads it
        // (ibx#470).
        let combo = match ClientCore::combo_order(&full_contract, &api_order, &shared.reference, &session_account) {
            Ok(combo) => combo,
            Err((code, message)) => {
                shared.orders.push_order_error(oid, code, message);
                return Ok(());
            }
        };
        // The condition times as the reference sends them (ibx#416); the
        // order is tracked as the caller placed it.
        let sent = ClientCore::with_condition_times(&api_order);

        // A smart combo goes out on its currency's smart combo conId.
        let con_id = combo.as_ref().map(|c| c.smart_con_id).filter(|&c| c > 0).unwrap_or(contract.con_id);
        let instrument = self.find_or_register_con_id(py, con_id, contract)?;
        // A send only for a new currency, then with the interpreter lock
        // released (ibx#271).
        if !self.core.currency_noted(con_id, &contract.currency) {
            py.detach(|| self.core.note_currency(&tx, con_id, &contract.currency));
        }

        // If orderId is already tracked, this is a modification: replace it
        // with the full wanted state (ibx#247). A what-if never modifies:
        // it previews a new order (ibx#462).
        let working = if api_order.what_if { None } else { self.core.tracked_order(oid) };
        if working.is_some() {
            let refusal = self.core.tracked_contract(oid)
                .and_then(|placed| ClientCore::combo_modify_refusal(&full_contract, &placed));
            if let Some((code, message)) = refusal {
                shared.orders.push_order_error(oid, code, message);
                return Ok(());
            }
        }
        let cmd = if let Some(working) = working {
            match ClientCore::build_modify_request(&sent, oid, &working)
                .map_err(|e| PyRuntimeError::new_err(e))?
            {
                ModifyPlan::Send(cmd) => cmd,
                ModifyPlan::Refused { code, message } => {
                    // Refused before sending, like the reference: the caller
                    // gets error() and the tracked order keeps its old state.
                    self.shared_state()?.orders.push_order_error(oid, code, message);
                    return Ok(());
                }
            }
        } else if let Some(combo) = combo {
            ClientCore::build_combo_order_request(&sent, oid, instrument, combo)
                .map_err(|e| PyRuntimeError::new_err(e))?
        } else {
            ClientCore::build_order_request(&sent, oid, instrument)
                .map_err(|e| PyRuntimeError::new_err(e))?
        };
        send_cmd(py, &tx, cmd)?;

        // Track order in shared core
        let api_contract = ApiContract {
            con_id: contract.con_id,
            symbol: contract.symbol.clone(),
            sec_type: contract.sec_type.clone(),
            exchange: contract.exchange.clone(),
            currency: contract.currency.clone(),
            combo_legs: full_contract.combo_legs.clone(),
            ..Default::default()
        };
        let mut tracked_order = api_order.clone();
        tracked_order.order_id = oid;
        self.core.cache_contract(contract.con_id, api_contract.clone());
        if tracked_order.what_if {
            self.core.track_what_if(oid, api_contract, tracked_order);
        } else {
            self.core.track_order(oid, api_contract, tracked_order, instrument);
        }

        Ok(())
    }

    /// Cancel an order.
    #[pyo3(signature = (order_id, manual_order_cancel_time=""))]
    fn cancel_order(&self, py: Python<'_>, order_id: i64, manual_order_cancel_time: &str) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        let tx = self.tx()?;
        send_cmd(py, &tx, ControlCommand::Order(OrderRequest::Cancel { order_id }))?;
        let _ = manual_order_cancel_time;
        Ok(())
    }

    /// Cancel all orders globally.
    fn req_global_cancel(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        let tx = self.tx()?;
        let shared = self.shared_state()?;
        let count = shared.market.instrument_count();
        for instrument in 0..count {
            let _ = send_cmd(py, &tx, ControlCommand::Order(OrderRequest::CancelAll { instrument }));
        }
        Ok(())
    }

    /// Request next valid order ID.
    #[pyo3(signature = (num_ids=1))]
    fn req_ids(&self, py: Python<'_>, num_ids: i32) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        let next_id = self.next_order_id.load(Ordering::Relaxed);
        self.wrapper.call_method1(py, "next_valid_id", (next_id,))?;
        let _ = num_ids;
        Ok(())
    }

    /// Get the next order ID (local counter, auto-increments).
    fn next_order_id(&self) -> i64 {
        self.next_order_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Request all open orders for this client.
    ///
    /// Queued for the dispatch loop after initial/reconnect replay and
    /// queued status updates. Interrupted replies can repeat order IDs.
    fn req_open_orders(&self) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        let shared = self.shared_state()?;
        self.core
            .queue_open_orders(crate::client_core::OpenOrdersRequest::Open);
        shared.notify();
        Ok(())
    }

    /// Request all open orders across all clients, queued like req_open_orders.
    fn req_all_open_orders(&self) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        let shared = self.shared_state()?;
        self.core
            .queue_open_orders(crate::client_core::OpenOrdersRequest::All);
        shared.notify();
        Ok(())
    }

    /// Automatically bind future orders to this client.
    #[pyo3(signature = (b_auto_bind))]
    fn req_auto_open_orders(&self, b_auto_bind: bool) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        let _ = b_auto_bind;
        Ok(())
    }

    /// Queue execution reports until the current server history has ended.
    /// Replies are delivered by the event dispatch loop.
    #[pyo3(signature = (req_id, exec_filter=None))]
    fn req_executions(&self, py: Python<'_>, req_id: i64, exec_filter: Option<Py<PyAny>>) -> PyResult<()> {
        if let Some(r) = self.not_connected(-1) { return r; }
        if !crate::client_core::ClientCore::ids_fit("req_executions", &[req_id]) { return Ok(()); }
        let filter = if let Some(ref fobj) = exec_filter {
            let get = |attr: &str| -> String {
                fobj.getattr(py, pyo3::types::PyString::new(py, attr))
                    .and_then(|v| v.extract::<String>(py))
                    .unwrap_or_default()
            };
            ExecutionFilter {
                symbol: get("symbol"),
                sec_type: get("secType"),
                exchange: get("exchange"),
                side: get("side"),
                acct_code: get("acctCode"),
                time: get("time"),
                client_id: fobj.getattr(py, pyo3::types::PyString::new(py, "clientId"))
                    .and_then(|v| v.extract::<i64>(py))
                    .unwrap_or(0),
            }
        } else {
            ExecutionFilter::default()
        };

        self.core.queue_execution_request(req_id, &filter);
        self.shared_state()?.notify();
        Ok(())
    }

    /// Fresh broker execution history over an explicit inclusive UTC interval.
    #[pyo3(signature = (req_id, start, end, exec_filter=None))]
    fn req_executions_range(&self, py: Python<'_>, req_id: i64, start: &str, end: &str, exec_filter: Option<Py<PyAny>>) -> PyResult<()> {
        if let Some(result) = self.not_connected(req_id) { return result; }
        if !ClientCore::ids_fit("req_executions_range", &[req_id]) { return Ok(()); }
        if !crate::client_core::valid_completed_history_range(start, end) {
            self.wrapper.call_method1(py, "error", (req_id, 321, "Invalid execution UTC history interval", ""))?;
            return Ok(());
        }
        let shared = self.shared_state()?;
        let Some(connection) = shared.orders.execution_history_request() else {
            self.wrapper.call_method1(py, "error", (req_id, 504, "Not connected", ""))?;
            return Ok(());
        };
        let filter = if let Some(object) = exec_filter {
            let get = |name: &str| object.getattr(py, pyo3::types::PyString::new(py, name))
                .and_then(|value| value.extract::<String>(py)).unwrap_or_default();
            ExecutionFilter { symbol: get("symbol"), sec_type: get("secType"), exchange: get("exchange"),
                side: get("side"), acct_code: get("acctCode"), time: get("time"),
                client_id: object.getattr(py, pyo3::types::PyString::new(py, "clientId"))
                    .and_then(|value| value.extract::<i64>(py)).unwrap_or(0), }
        } else { ExecutionFilter::default() };
        send_cmd(py, &self.tx()?, ControlCommand::RequestExecutionsRange {
            connection, req_id, start: start.into(), end: end.into(), filter,
        })
    }

    /// Request today's completed orders from a fresh broker query.
    #[pyo3(signature = (api_only=false))]
    fn req_completed_orders(&self, py: Python<'_>, api_only: bool) -> PyResult<()> {
        let end = crate::config::chrono_free_timestamp();
        let start = format!("{}-00:00:00", &end[..8]);
        self.req_completed_orders_range(py, &start, &end, api_only)
    }

    /// Fresh history over an inclusive UTC YYYYMMDD-HH:MM:SS interval.
    #[pyo3(signature = (start, end, api_only=false))]
    fn req_completed_orders_range(&self, py: Python<'_>, start: &str, end: &str, api_only: bool) -> PyResult<()> {
        if let Some(result) = self.not_connected(-1) { return result; }
        if !crate::client_core::valid_completed_history_range(start, end) {
            self.wrapper.call_method1(py, "error", (-1, 321, "Invalid completed-order UTC history interval", ""))?;
            return Ok(());
        }
        let shared = self.shared_state()?;
        let Some(connection) = shared.orders.execution_history_request() else {
            self.wrapper.call_method1(py, "error", (-1, 504, "Not connected", ""))?;
            return Ok(());
        };
        send_cmd(py, &self.tx()?, ControlCommand::RequestCompletedOrders {
            connection, api_only, start: start.to_string(), end: end.to_string(),
        })
    }
}

impl EClient {
    fn requeue_execution_if_current(
        &self,
        shared: &std::sync::Arc<SharedState>,
        req_id: i64,
        filter: &ExecutionFilter,
    ) {
        let current = self.shared.lock().unwrap();
        if self.connected.load(Ordering::Acquire)
            && current
                .as_ref()
                .is_some_and(|active| std::sync::Arc::ptr_eq(active, shared))
        {
            self.core.queue_execution_request(req_id, filter);
        }
    }

    pub(crate) fn answer_executions(
        &self,
        py: Python<'_>,
        shared: &std::sync::Arc<SharedState>,
        history_id: &str,
        req_id: i64,
        filter: &ExecutionFilter,
    ) -> PyResult<()> {
        // No lock is held during the callbacks (ibx#265). As the reference:
        // every execution, then the commission reports, then the end.
        let execs = self.core.matching_executions(filter);
        if !shared.orders.execution_history_matches(history_id) {
            self.requeue_execution_if_current(shared, req_id, filter);
            return Ok(());
        }
        self.send_execution_rows(py, req_id, &execs)?;
        // The report exists once the server's commission frame came (ibx#471).
        for cr in execs.iter().filter_map(|se| se.commission_and_fees.as_ref()) {
            self.send_commission_report(py, cr)?;
        }
        if shared.orders.execution_history_matches(history_id) {
            self.wrapper.call_method1(py, "exec_details_end", (req_id,))?;
        } else {
            self.requeue_execution_if_current(shared, req_id, filter);
        }
        Ok(())
    }

    pub(crate) fn send_execution_rows(&self, py: Python<'_>, req_id: i64, rows: &[crate::client_core::StoredExecution]) -> PyResult<()> {
        for se in rows {
            let c_py = Py::new(py, Contract {
                con_id: se.contract.con_id,
                symbol: se.contract.symbol.clone(),
                sec_type: se.contract.sec_type.clone(),
                exchange: se.contract.exchange.clone(),
                currency: se.contract.currency.clone(),
                ..Default::default()
            })?.into_any();

            let exec_obj = Execution {
                exec_id: se.execution.exec_id.clone(),
                time: se.execution.time.clone(),
                acct_number: se.execution.acct_number.clone(),
                exchange: se.execution.exchange.clone(),
                side: se.execution.side.clone(),
                shares: se.execution.shares,
                price: se.execution.price,
                perm_id: se.execution.perm_id,
                client_id: se.execution.client_id,
                order_id: se.execution.order_id,
                liquidation: se.execution.liquidation,
                cum_qty: se.execution.cum_qty,
                avg_price: se.execution.avg_price,
                order_ref: se.execution.order_ref.clone(),
                ev_rule: se.execution.ev_rule.clone(),
                ev_multiplier: se.execution.ev_multiplier,
                model_code: se.execution.model_code.clone(),
                last_liquidity: se.execution.last_liquidity,
                pending_price_revision: se.execution.pending_price_revision,
            };
            let exec_py = Py::new(py, exec_obj)?.into_any();

            self.wrapper.call_method(
                py, "exec_details",
                (req_id, &c_py, &exec_py),
                None,
            )?;
        }
        Ok(())
    }

    /// The open orders, each as open_order then order_status, then the end
    /// of the list.
    pub(crate) fn answer_open_orders(
        &self,
        py: Python<'_>,
        shared: &std::sync::Arc<SharedState>,
        request: crate::client_core::OpenOrdersRequest,
        history: Option<&str>,
    ) -> PyResult<()> {
        let Some(orders) = self.core.prepare_open_orders(shared) else {
            self.requeue_open_orders_if_current(shared, request);
            return Ok(());
        };
        if !self.open_order_snapshot_current(shared, history) {
            self.requeue_open_orders_if_current(shared, request);
            return Ok(());
        }
        for (order_id, tracked) in &orders {
            // A combo with its legs (ibx#470).
            let c_py = Py::new(py, Contract::from_api(py, &tracked.contract)?)?.into_any();
            let mut o = Order::default();
            o.order_id = tracked.order.order_id;
            o.action = tracked.order.action.clone();
            o.total_quantity = tracked.order.total_quantity;
            o.order_type = tracked.order.order_type.clone();
            o.lmt_price = tracked.order.lmt_price;
            o.aux_price = tracked.order.aux_price;
            o.tif = tracked.order.tif.clone();
            o.account = tracked.order.account.clone();
            o.perm_id = tracked.order.perm_id;
            o.parent_id = tracked.order.parent_id;
            o.oca_group = tracked.order.oca_group.clone();
            o.order_ref = tracked.order.order_ref.clone();
            o.oca_type = tracked.order.oca_type;
            o.use_price_mgmt_algo = (tracked.order.use_price_mgmt_algo != i32::MAX).then_some(tracked.order.use_price_mgmt_algo != 0);
            o.trail_stop_price = tracked.order.trail_stop_price;
            o.algo_strategy = tracked.order.algo_strategy.clone();
            // A combo's per-leg prices and routing (ibx#470).
            for price in &tracked.order.order_combo_legs {
                o.order_combo_legs.push(Py::new(py, super::super::contract::OrderComboLeg { price: *price })?.into_any());
            }
            o.smart_combo_routing_params = tracked.order.smart_combo_routing_params.iter()
                .map(|tv| super::super::contract::TagValue { tag: tv.tag.clone(), value: tv.value.clone() }).collect();
            let o_py = Py::new(py, o)?.into_any();
            let mut state = super::super::contract::OrderState::default();
            state.status = tracked.status.clone();
            let state_py = Py::new(py, state)?.into_any();
            self.wrapper.call_method(
                py, "open_order",
                (*order_id, &c_py, &o_py, &state_py),
                None,
            )?;
            self.wrapper.call_method(
                py, "order_status",
                (*order_id, tracked.status.as_str(), tracked.filled, tracked.remaining,
                 0.0f64, tracked.order.perm_id, tracked.order.parent_id, 0.0f64, 0i64, "", 0.0f64),
                None,
            )?;
        }
        if self.open_order_snapshot_current(shared, history) {
            self.wrapper.call_method0(py, "open_order_end")?;
        } else {
            self.requeue_open_orders_if_current(shared, request);
        }
        Ok(())
    }

    fn open_order_snapshot_current(
        &self,
        shared: &std::sync::Arc<SharedState>,
        history: Option<&str>,
    ) -> bool {
        let current = self.shared.lock().unwrap();
        self.connected.load(Ordering::Acquire)
            && current
                .as_ref()
                .is_some_and(|active| std::sync::Arc::ptr_eq(active, shared))
            && shared.orders.execution_history_request_matches(history)
            && !shared.orders.open_orders_held()
    }

    fn requeue_open_orders_if_current(
        &self,
        shared: &std::sync::Arc<SharedState>,
        request: crate::client_core::OpenOrdersRequest,
    ) {
        let current = self.shared.lock().unwrap();
        if self.connected.load(Ordering::Acquire)
            && current
                .as_ref()
                .is_some_and(|active| std::sync::Arc::ptr_eq(active, shared))
        {
            self.core.queue_open_orders(request);
        }
    }
}

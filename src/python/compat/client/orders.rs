//! Order placement, cancellation, open orders, executions, completed orders.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::atomic::Ordering;

use super::super::contract::{Contract, Execution, Order};
use super::{EClient, send_cmd};
use crate::api::types::{Contract as ApiContract, Order as ApiOrder, ExecutionFilter};
use crate::bridge::SharedState;
use crate::client_core::{ClientCore, ModifyPlan};
use crate::types::*;

impl EClient {
    /// An order placed with transmit off (ibx#509): nothing is sent but the
    /// lookup of a contract given without a conId, and nothing is answered.
    /// A working order placed again with transmit off stays as it is
    /// (paper 09/10/2026).
    fn hold_order(&self, py: Python<'_>, oid: i64, contract: &ApiContract, order: &ApiOrder) -> PyResult<()> {
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
        if self.core.requests_ignored() { return Ok(()); }
        let tx = self.tx()?;
        let shared = self.shared_state()?;
        if self.core.working_order(&shared, oid).is_some() {
            return Ok(());
        }
        // The order is read and checked now, as the reference does; one
        // that is refused is not held (ibx#547).
        let session_account = self.account_id.lock().unwrap().clone().unwrap_or_default();
        let (notices, refusal, _) = self.core.order_read_checks(
            oid, contract.con_id, &contract.sec_type, &contract.exchange, &contract.last_trade_date_or_contract_month, order,
            &shared, &session_account, false);
        let combo = ClientCore::combo_order(contract, order, &shared.reference, &session_account);
        for (code, message) in notices {
            shared.orders.push_order_error(oid, code, message);
        }
        if let Some((code, message)) = refusal.or(combo.err()) {
            shared.orders.push_order_error(oid, code, message);
            return Ok(());
        }
        let instrument = if contract.sec_type.eq_ignore_ascii_case("BAG") { 0 } else {
            py.detach(|| self.core.order_instrument(&tx, oid, false, contract.con_id, &contract.symbol,
                &contract.exchange, &contract.sec_type, &contract.currency, crate::types::SecDefFilters {
                    // The primary exchange goes into the order's lookup (ibx#710).
                    primary_exchange: contract.primary_exchange.clone(), local_symbol: contract.local_symbol.clone(),
                    last_trade_date_or_contract_month: contract.last_trade_date_or_contract_month.clone(),
                    strike: contract.strike, right: contract.right.clone(), multiplier: contract.multiplier.clone(),
                    trading_class: contract.trading_class.clone(), ..Default::default()
                })).map_err(PyRuntimeError::new_err)?
        };
        // Not known as a parent until the engine has its lookups' answers
        // (ibx#718): noted before the engine can answer.
        self.core.note_held_placed(&shared, oid);
        send_cmd(py, &tx, ControlCommand::HoldOrder {
            order_id: oid, instrument,
            qty: (order.total_quantity * crate::types::QTY_SCALE as f64).round() as crate::types::Qty,
            parent_id: order.parent_id, oca_group: order.oca_group.clone(),
        })?;
        let mut held = order.clone();
        held.order_id = oid;
        self.core.hold_order(oid, contract.clone(), held, instrument);
        Ok(())
    }

    /// Send one order: a new order, or the change of a working one.
    fn send_order(&self, py: Python<'_>, order_id: i64, contract: &ApiContract, order: &ApiOrder, was_held: bool, built: &mut Vec<ControlCommand>) -> PyResult<()> {
        let tx = self.tx()?;
        let (api_order, full_contract) = (order, contract);

        // The id as given: the reference refuses 0 with 10149 below.
        let oid = order_id;

        // The warnings and the refusal of the order as it is read; the
        // warnings of an order that was held were given when it was placed
        // with transmit off (ibx#547).
        let shared = self.shared_state()?;
        let session_account = self.account_id.lock().unwrap().clone().unwrap_or_default();
        // A contract without a conId is looked up first; what the reference
        // checks once it is known comes after (ibx#665).
        let looked_up = contract.con_id == 0 && !contract.sec_type.eq_ignore_ascii_case("BAG");
        let (notices, refusal, deferred) = self.core.order_read_checks(
            oid, contract.con_id, &contract.sec_type, &contract.exchange, &contract.last_trade_date_or_contract_month, api_order,
            &shared, &session_account, looked_up);
        if !was_held {
            for (code, message) in notices {
                shared.orders.push_order_error(oid, code, message);
            }
        }
        if let Some((code, message)) = refusal {
            shared.orders.push_order_error(oid, code, message);
            return Ok(());
        }
        // A combo (BAG) order, read and checked as the reference reads it
        // (ibx#470).
        let combo = match ClientCore::combo_order(full_contract, api_order, &shared.reference, &session_account) {
            Ok(combo) => combo,
            Err((code, message)) => {
                shared.orders.push_order_error(oid, code, message);
                return Ok(());
            }
        };
        // The condition times as the reference sends them (ibx#416); the
        // order is tracked as the caller placed it.
        let sent = self.core.hedge_sent(ClientCore::with_condition_times(api_order));

        // A smart combo goes out on its currency's smart combo conId.
        let con_id = combo.as_ref().map(|c| c.smart_con_id).filter(|&c| c > 0).unwrap_or(contract.con_id);
        // A contract without a conId is looked up by the engine before the
        // order goes out (ibx#486); one with a conId takes the slot of its
        // exchange, as in the Rust client (ibx#688: an AAPL order on ISLAND
        // after one on SMART was checked with the SMART rules).
        let instrument = if !contract.sec_type.eq_ignore_ascii_case("BAG") {
            py.detach(|| self.core.order_instrument(&tx, oid, api_order.what_if, con_id, &contract.symbol,
                &contract.exchange, &contract.sec_type, &contract.currency, crate::types::SecDefFilters {
                    // The primary exchange goes into the order's lookup (ibx#710).
                    primary_exchange: contract.primary_exchange.clone(), local_symbol: contract.local_symbol.clone(),
                    last_trade_date_or_contract_month: contract.last_trade_date_or_contract_month.clone(),
                    strike: contract.strike, right: contract.right.clone(), multiplier: contract.multiplier.clone(),
                    trading_class: contract.trading_class.clone(), ..Default::default()
                })).map_err(PyRuntimeError::new_err)?
        } else {
            let known = self.core.con_id_to_instrument.lock().unwrap().get(&con_id).copied();
            match known {
                Some(id) => id,
                None => py.detach(|| self.core.find_or_register_instrument(
                    &tx, con_id, &contract.symbol, &contract.exchange, &contract.sec_type,
                )).map_err(PyRuntimeError::new_err)?,
            }
        };
        // A send only for a new currency, then with the interpreter lock
        // released (ibx#271).
        if !self.core.currency_noted(con_id, &contract.currency) {
            py.detach(|| self.core.note_currency(&tx, con_id, &contract.currency));
        }

        // If orderId is already tracked, this is a modification: replace it
        // with the full wanted state (ibx#247). A what-if never modifies:
        // it previews a new order (ibx#462).
        let working = if api_order.what_if { None } else { self.core.working_order(&shared, oid) };
        if working.is_some() {
            let refusal = self.core.tracked_contract(oid)
                .and_then(|placed| ClientCore::combo_modify_refusal(full_contract, &placed)
                    .or_else(|| ClientCore::algo_destination_modify_refusal(full_contract, &placed,
                        |e| shared.reference.is_algo_provider(e))))
                // A beta hedge's max size below what it filled (ibx#602).
                .or_else(|| self.core.hedge_filled_refusal(oid, &api_order));
            if let Some((code, message)) = refusal {
                shared.orders.push_order_error(oid, code, message);
                return Ok(());
            }
        }
        let cmd = if let Some(working) = working {
            match ClientCore::build_modify_request(&sent, oid, &working, shared.reference.form_10_40())
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
        let mut cmd = cmd;
        if !self.core.carry_deferred_checks(oid, &mut cmd, deferred, &shared) {
            return Ok(());
        }
        // A rule checked once the contract is known that keeps the order
        // for its cancel (ibx#688).
        self.core.carry_kept_refusal(&mut cmd, api_order);
        let child_contract = ApiContract {
            con_id: contract.con_id, symbol: contract.symbol.clone(), sec_type: contract.sec_type.clone(),
            exchange: contract.exchange.clone(), currency: contract.currency.clone(), ..Default::default()
        };
        // A scale order's profit taker (ibx#751).
        if !api_order.what_if {
            self.core.scale_profit_child(&mut cmd, &child_contract, api_order, oid, instrument);
        }
        built.push(cmd);

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
}

#[pymethods]
impl EClient {
    /// Place an order.
    fn place_order(&self, py: Python<'_>, order_id: i64, contract: &Contract, order: &Order) -> PyResult<()> {
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
        if self.core.requests_ignored() { return Ok(()); }
        // Convert and validate order params first (fail fast, no connection needed)
        let mut api_order = order.to_api();
        // A condition that is not understood ends the request here (ibx#541).
        api_order.conditions = order.convert_conditions(py)?;
        api_order.order_combo_legs = order.convert_order_combo_legs(py);
        // The contract with its combo legs (ibx#470).
        let mut full_contract = contract.to_api();
        full_contract.combo_legs = contract.convert_combo_legs(py);
        // The reference's other names for an order type (ibx#469).
        if let Some(name) = ClientCore::canonical_order_type(&api_order.order_type) {
            api_order.order_type = name.to_string();
        }
        // GTX and NMIN go out as GTC in the 10.40 form (ibx#307); 10.51
        // refuses them (ibx#604).
        let form_10_40 = self.shared.lock().unwrap().as_ref().is_some_and(|s| s.reference.form_10_40());
        if let Some(name) = ClientCore::canonical_tif(&api_order.tif, form_10_40) {
            api_order.tif = name.to_string();
        }
        ClientCore::validate_order(&api_order)
            .map_err(|e| PyRuntimeError::new_err(e))?;
        // After the checks above, which refuse an invalid order even with no
        // connection (ibx#115).
        if let Some(r) = self.not_connected(order_id) { return r; }
        if !ClientCore::ids_fit("place_order", &[order_id, contract.con_id]) { return Ok(()); }
        // The refusals of the contract's security type (ibx#666).
        if let Some((code, message)) = ClientCore::order_contract_refusal(&full_contract) {
            self.shared_state()?.orders.push_order_error(order_id, code, message);
            return Ok(());
        }

        // Transmit off: the order is held (ibx#509). A what-if with
        // transmit off is refused with 321 by the checks of an order.
        if !api_order.transmit && !api_order.what_if {
            return self.hold_order(py, order_id, &full_contract, &api_order);
        }
        let tx = self.tx()?;
        let mut built = Vec::new();
        if api_order.what_if {
            self.send_order(py, order_id, &full_contract, &api_order, false, &mut built)?;
        } else {
            // Transmit on: the order goes out with the held orders of its
            // tree, together (ibx#547). An order that fails ends the list;
            // the ones before it go.
            let shared = self.shared_state()?;
            let account = self.account_id.lock().unwrap().clone().unwrap_or_default();
            for (id, contract, order, was_held) in self.core.orders_to_transmit_checked(order_id, &full_contract, &api_order, &shared, &account) {
                if let Err(e) = self.send_order(py, id, &contract, &order, was_held, &mut built) {
                    if built.len() > 1 {
                        send_cmd(py, &tx, ClientCore::order_group(built))?;
                    } else if let Some(cmd) = built.pop() {
                        send_cmd(py, &tx, cmd)?;
                    }
                    return Err(e);
                }
            }
        }
        let result = if built.len() > 1 {
            send_cmd(py, &tx, ClientCore::order_group(built))
        } else {
            built.pop().map_or(Ok(()), |cmd| send_cmd(py, &tx, cmd))
        };
        if result.is_err() && api_order.what_if {
            self.core.untrack_latest_what_if(order_id);
        }
        result
    }

    /// Cancel an order.
    #[pyo3(signature = (order_id, manual_order_cancel_time=""))]
    fn cancel_order(&self, py: Python<'_>, order_id: i64, manual_order_cancel_time: &str) -> PyResult<()> {
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
        if let Some(r) = self.not_connected(-1) { return r; }
        if !ClientCore::ids_fit("cancel_order", &[order_id]) { return Ok(()); }
        let tx = self.tx()?;
        self.core.drop_held_order(order_id);
        send_cmd(py, &tx, ControlCommand::Order(OrderRequest::Cancel { order_id }))?;
        let _ = manual_order_cancel_time;
        Ok(())
    }

    /// Cancel all orders globally: every order of the account the session
    /// knows, those of other clients and of earlier sessions too, as the
    /// reference cancels them.
    fn req_global_cancel(&self, py: Python<'_>) -> PyResult<()> {
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
        if let Some(r) = self.not_connected(-1) { return r; }
        let tx = self.tx()?;
        self.core.drop_held_orders();
        send_cmd(py, &tx, ControlCommand::Order(OrderRequest::GlobalCancel))
    }

    /// Request next valid order ID: the highest order id this client used
    /// + 1, as the reference computes it per client id (1 when none), a
    /// 32-bit id. The ids of the client's earlier sessions count as far as
    /// the server's replays of the logon show them; right after the
    /// connect the answer waits for the order replay of the logon. Nothing
    /// is reserved.
    #[pyo3(signature = (num_ids=1))]
    fn req_ids(&self, py: Python<'_>, num_ids: i32) -> PyResult<()> {
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
        if let Some(r) = self.not_connected(-1) { return r; }
        if self.later(move |this, py| this.req_ids(py, num_ids)) { return Ok(()); }
        let shared = self.shared_state()?;
        let next_id = py.detach(|| {
            ClientCore::wait_order_replay(&shared);
            self.core.next_valid_id(&shared)
        });
        self.wrapper.call_method1(py, "next_valid_id", (next_id,))?;
        let _ = num_ids;
        Ok(())
    }

    /// The next order id for a new order: the next valid id (see
    /// ``req_ids``), or above the ids this method gave before. Each call
    /// reserves the id it gives.
    fn next_order_id(&self, py: Python<'_>) -> PyResult<i64> {
        let shared = self.shared_state()?;
        Ok(py.detach(|| {
            ClientCore::wait_order_replay(&shared);
            self.core.take_order_id(&shared)
        }))
    }

    /// Request all open orders for this client.
    ///
    /// Queued for the dispatch loop after initial/reconnect replay and
    /// queued status updates. Interrupted replies can repeat order IDs.
    fn req_open_orders(&self) -> PyResult<()> {
        let _request = crate::engine::park::one_request();
        if let Some(r) = self.not_connected(-1) { return r; }
        let shared = self.shared_state()?;
        self.core
            .queue_open_orders(crate::client_core::OpenOrdersRequest::Open);
        shared.notify();
        Ok(())
    }

    /// Request all open orders across all clients, queued like req_open_orders.
    fn req_all_open_orders(&self) -> PyResult<()> {
        let _request = crate::engine::park::one_request();
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
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
        if let Some(r) = self.not_connected(-1) { return r; }
        let _ = b_auto_bind;
        Ok(())
    }

    /// Queue execution reports until the current server history has ended.
    /// Replies are delivered by the event dispatch loop.
    #[pyo3(signature = (req_id, exec_filter=None))]
    fn req_executions(&self, py: Python<'_>, req_id: i64, exec_filter: Option<Py<PyAny>>) -> PyResult<()> {
        // One API request for the pacing, whatever it sends (ibx#565).
        let _request = crate::engine::park::one_request();
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

    /// Reference-compatible current-day completed orders. Explicit ranges below
    /// always query fresh broker history instead of reusing the session day cache.
    #[pyo3(signature = (api_only=false))]
    fn req_completed_orders(&self, py: Python<'_>, api_only: bool) -> PyResult<()> {
        let _request = crate::engine::park::one_request();
        if let Some(r) = self.not_connected(-1) { return r; }
        send_cmd(py, &self.tx()?, ControlCommand::CompletedOrders { api_only })
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
            let c_py = Py::new(py, Contract::from_api(py, &se.contract)?)?.into_any();

            let exec_obj = Execution::from_api(&se.execution);
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
        let Some(orders) = self.core.prepare_open_orders_listing(shared, request) else {
            self.requeue_open_orders_if_current(shared, request);
            return Ok(());
        };
        if !self.open_order_snapshot_current(shared, history) {
            self.requeue_open_orders_if_current(shared, request);
            return Ok(());
        }
        for (order_id, tracked, client_id) in &orders {
            // A combo with its legs (ibx#470).
            let c_py = Py::new(py, Contract::from_api(py, &tracked.contract)?)?.into_any();
            // A combo's per-leg prices and routing with it (ibx#470).
            let o_py = Py::new(py, Order::from_api(py, &tracked.order)?)?.into_any();
            let mut state = super::super::contract::OrderState::default();
            state.status = tracked.status.clone();
            let state_py = Py::new(py, state)?.into_any();
            // An order kept pending after a refusal: its status only
            // (ibx#682).
            if tracked.status != "ApiPending" {
                self.wrapper.call_method(
                    py, "open_order",
                    (*order_id, &c_py, &o_py, &state_py),
                    None,
                )?;
            }
            let why_held = self.core.why_held(&tracked.status, &tracked.order.order_type, tracked.order.parent_id);
            self.wrapper.call_method(
                py, "order_status",
                (*order_id, tracked.status.as_str(), crate::python::compat::values::decimal_to_py(py, tracked.filled)?,
                 crate::python::compat::values::decimal_to_py(py, tracked.remaining)?,
                 0.0f64, tracked.order.perm_id, tracked.order.parent_id, tracked.last_fill_price, *client_id, why_held.as_str(), 0.0f64),
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

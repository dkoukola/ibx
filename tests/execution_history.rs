//! Execution snapshots use the explicit U72 completion, never an idle gap.
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use ibx::api::types::{Contract, Execution, ExecutionFilter};
use ibx::bridge::SharedState;
use ibx::gateway::Gateway;
use ibx::protocol::{connection::Connection, fix::fix_build};
use ibx::{EClient, Wrapper};

fn connection() -> (Connection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    (Connection::new_raw(client).unwrap(), server)
}

fn gateway() -> Gateway {
    Gateway {
        account_id: "DUXXXXXXX".into(),
        session_token: Default::default(),
        server_session_id: String::new(),
        settings_object_key: String::new(),
        heartbeat_interval: 30,
        hw_info: String::new(),
        encoded: String::new(),
        raw_soft_dollar_tiers: String::new(),
        raw_family_codes: String::new(),
        raw_news_providers: String::new(),
        raw_news_sources: String::new(),
        raw_news_capabilities: String::new(),
        deny_news: false,
        white_branding_id: String::new(),
        fa_session: false,
        super_user: false,
        omnibus: false,
        raw_smart_combo_con_ids: String::new(),
        account_config: None,
        algo_definitions: Vec::new(),
        scale_us_lots: false,
        tick_by_tick_limit: 100,
        depth_limit: 3,
        user_book: false,
        tick_by_tick_off: false,
        price_mgmt: false,
        price_mgmt_exclusions: None,
        max_real_time_requests: 100,
        misc_urls: Default::default(),
        ccp_sign_key: Vec::new(),
        ccp_sign_iv: Vec::new(),
        hmds_host: String::new(),
        hmds_farm: String::new(),
        session_epoch: String::new(),
        farm_name: "fixture".into(),
        farm_host: String::new(),
        md_routing: None,
        hmds_routing: None,
        ns_secure_refused: false,
        logon: Default::default(),
        version_cutoff: None,
        version_cutoff_date: None,
        max_backfill_years: 1,
    }
}

fn marker(request: &str) -> Vec<u8> {
    fix_build(
        &[(35, "8"), (6556, request), (32, "*"), (150, "0"), (39, "0")],
        1,
    )
}

fn fill() -> Vec<u8> {
    fix_build(
        &[
            (35, "8"),
            (11, "1339547414.0"),
            (6121, "7"),
            (17, "history-execution.01"),
            (150, "2"),
            (39, "2"),
            (20, "0"),
            (6008, "265598"),
            (55, "AAPL"),
            (54, "1"),
            (32, "1"),
            (31, "100"),
            (14, "1"),
            (151, "0"),
            (6, "100"),
            (60, "20260930-19:04:31"),
        ],
        1,
    )
}

#[derive(Default)]
struct Observed {
    rows: Vec<(i64, String)>,
    ends: Vec<i64>,
    invalidate: Option<Arc<SharedState>>,
}

impl Wrapper for Observed {
    fn exec_details(&mut self, req_id: i64, _: &Contract, execution: &Execution) {
        self.rows.push((req_id, execution.exec_id.clone()));
        if let Some(shared) = self.invalidate.take() {
            shared.orders.invalidate_execution_history();
        }
    }

    fn exec_details_end(&mut self, req_id: i64) {
        self.ends.push(req_id);
    }
}

#[test]
fn gateway_initial_history_waits_for_buffered_rows_and_matching_end() {
    for has_fill in [false, true] {
        let shared = Arc::new(SharedState::new());
        let (farm, _farm_peer) = connection();
        let (mut ccp, _ccp_peer) = connection();
        let mut initial = if has_fill { fill() } else { Vec::new() };
        initial.extend(marker("wrong"));
        ccp.seed_buffer(&initial);
        let (mut engine, tx) = gateway().into_hot_loop(shared.clone(), None, farm, ccp, None, None);
        let client = EClient::from_parts(
            shared.clone(),
            tx,
            std::thread::spawn(|| {}),
            "DUXXXXXXX".into(),
        );
        let mut observed = Observed::default();
        client.req_executions(1, &ExecutionFilter::default(), &mut observed);
        client.req_executions(
            2,
            &ExecutionFilter {
                symbol: "MSFT".into(),
                ..Default::default()
            },
            &mut observed,
        );
        client.process_msgs(&mut observed);
        assert!(observed.ends.is_empty());
        engine.poll_auth_for_test();
        assert!(shared.orders.execution_history_completion().is_none());
        assert!(observed.rows.is_empty());
        assert!(
            observed.ends.is_empty(),
            "wrong marker cannot finish history"
        );
        engine.inject_ccp_message(&marker("today4"));
        assert!(observed.ends.is_empty(), "callbacks are asynchronous");
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, [1, 2]);
        assert_eq!(observed.rows.len(), usize::from(has_fill));
        let position = engine.context_mut().position_fixed(0);
        engine.inject_ccp_message(&marker("today4"));
        client.process_msgs(&mut observed);
        assert_eq!(
            observed.ends,
            [1, 2],
            "duplicate end does not repeat requests"
        );
        assert_eq!(
            engine.context_mut().position_fixed(0),
            position,
            "callbacks do not rebook fills"
        );
    }
}

#[test]
fn history_loss_during_callbacks_waits_for_replacement_and_never_invents_end() {
    let shared = Arc::new(SharedState::new());
    let mut engine = ibx::engine::hot_loop::HotLoop::new(shared.clone(), None, None);
    let (tx, _rx) = crossbeam_channel::unbounded();
    let client = EClient::from_parts(
        shared.clone(),
        tx,
        std::thread::spawn(|| {}),
        "DUXXXXXXX".into(),
    );
    shared.orders.begin_execution_history("today4");
    engine.inject_ccp_message(&fill());
    engine.inject_ccp_message(&marker("today4"));
    let mut observed = Observed {
        invalidate: Some(shared.clone()),
        ..Default::default()
    };
    client.req_executions(1, &ExecutionFilter::default(), &mut observed);
    client.process_msgs(&mut observed);
    assert_eq!(observed.rows.len(), 1);
    assert!(
        observed.ends.is_empty(),
        "loss during callback must suppress end"
    );
    shared.orders.begin_execution_history("todayfillup5");
    engine.inject_ccp_message(&marker("today4"));
    client.process_msgs(&mut observed);
    assert!(
        observed.ends.is_empty(),
        "old end cannot complete replacement"
    );
    engine.inject_ccp_message(&marker("todayfillup5"));
    client.process_msgs(&mut observed);
    assert_eq!(observed.ends, [1]);
    assert_eq!(
        observed.rows[0], observed.rows[1],
        "interrupted replies retain stable IDs"
    );
    client.req_executions(2, &ExecutionFilter::default(), &mut observed);
    shared.set_connection_lost();
    client.process_msgs(&mut observed);
    assert_eq!(
        observed.ends,
        [1],
        "terminal loss invalidates completion too"
    );
    client.disconnect();
    assert!(shared.orders.execution_history_completion().is_none());
}

#[derive(Default)]
struct OpenObserved {
    orders: Vec<i64>,
    ends: usize,
    reconnect: Option<Arc<SharedState>>,
}

impl Wrapper for OpenObserved {
    fn open_order(
        &mut self,
        id: i64,
        _: &Contract,
        _: &ibx::api::types::Order,
        _: &ibx::api::types::OrderState,
    ) {
        self.orders.push(id);
        if let Some(shared) = self.reconnect.take() {
            shared.orders.set_open_orders_held(true);
            shared.orders.invalidate_execution_history();
            shared.orders.begin_execution_history("today5");
            // The new open-order end can precede its independent U72 end.
            shared.orders.set_open_orders_held(false);
        }
    }

    fn open_order_end(&mut self) {
        self.ends += 1;
    }
}

fn open_row() -> Vec<u8> {
    fix_build(
        &[
            (35, "8"),
            (11, "1339547414.0"),
            (6121, "7"),
            (150, "0"),
            (39, "0"),
            (6008, "265598"),
            (55, "AAPL"),
            (54, "1"),
            (38, "1"),
            (44, "100"),
            (40, "2"),
        ],
        1,
    )
}

fn open_end() -> Vec<u8> {
    fix_build(&[(35, "8"), (11, "*"), (55, "*"), (150, "0"), (39, "0")], 1)
}

#[test]
fn gateway_open_orders_wait_for_their_own_initial_replay_not_execution_end() {
    for has_order in [false, true] {
        let shared = Arc::new(SharedState::new());
        let (farm, _farm_peer) = connection();
        let (mut ccp, _ccp_peer) = connection();
        if has_order {
            ccp.seed_buffer(&open_row());
        }
        let (mut engine, tx) = gateway().into_hot_loop(shared.clone(), None, farm, ccp, None, None);
        let client = EClient::from_parts(
            shared.clone(),
            tx,
            std::thread::spawn(|| {}),
            "DUXXXXXXX".into(),
        );
        let mut observed = OpenObserved::default();
        client.req_open_orders(&mut observed);
        client.req_all_open_orders(&mut observed);
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 0);
        engine.poll_auth_for_test();
        // An unrelated execution-history end must not release open orders.
        engine.inject_ccp_message(&marker("wrong"));
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 0);
        assert!(observed.orders.is_empty());
        engine.inject_ccp_message(&open_end());
        assert!(shared.orders.execution_history_completion().is_none());
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 2);
        assert_eq!(observed.orders, if has_order { vec![7, 7] } else { vec![] });
        engine.inject_ccp_message(&open_end());
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 2);
    }
}

#[test]
fn open_order_callback_reconnect_cannot_end_an_old_snapshot_after_a_new_end() {
    let shared = Arc::new(SharedState::new());
    let mut engine = ibx::engine::hot_loop::HotLoop::new(shared.clone(), None, None);
    let (tx, _rx) = crossbeam_channel::unbounded();
    let client = EClient::from_parts(
        shared.clone(),
        tx,
        std::thread::spawn(|| {}),
        "DUXXXXXXX".into(),
    );
    shared.orders.begin_execution_history("today4");
    engine.inject_ccp_message(&open_row());
    let mut observed = OpenObserved {
        reconnect: Some(shared.clone()),
        ..Default::default()
    };
    client.req_open_orders(&mut observed);
    client.process_msgs(&mut observed);
    assert_eq!(observed.orders, [7]);
    assert_eq!(
        observed.ends, 0,
        "held false→true→false must not end old snapshot"
    );
    client.process_msgs(&mut observed);
    assert_eq!(observed.orders, [7, 7]);
    assert_eq!(observed.ends, 1);
    assert!(
        shared.orders.execution_history_completion().is_none(),
        "U72 completion is independent"
    );
    client.req_open_orders(&mut observed);
    shared.set_connection_lost();
    client.process_msgs(&mut observed);
    assert_eq!(observed.ends, 1, "terminal loss holds readers too");
}

#[derive(Default)]
struct PositionsObserved {
    rows: Vec<(i64, f64)>,
    ends: usize,
}

impl Wrapper for PositionsObserved {
    fn position(&mut self, _: &str, contract: &Contract, quantity: f64, _: f64) {
        self.rows.push((contract.con_id, quantity));
    }

    fn position_end(&mut self) {
        self.ends += 1;
    }
}

#[test]
fn gateway_position_snapshot_waits_for_buffered_account_image_even_when_empty() {
    for has_position in [false, true] {
        let shared = Arc::new(SharedState::new());
        let (farm, _farm_peer) = connection();
        let (mut ccp, _ccp_peer) = connection();
        let mut fields = vec![(35, "U"), (6040, "75"), (1, "DUXXXXXXX"), (6544, "1")];
        if has_position {
            fields.extend([(146, "1"), (6008, "265598"), (6064, "2.5"), (6101, "100")]);
        }
        ccp.seed_buffer(&fix_build(&fields, 1));
        let (mut engine, tx) = gateway().into_hot_loop(shared.clone(), None, farm, ccp, None, None);
        let client = EClient::from_parts(
            shared.clone(),
            tx,
            std::thread::spawn(|| {}),
            "DUXXXXXXX".into(),
        );
        let mut observed = PositionsObserved::default();
        client.req_positions(&mut observed);
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 0, "buffered bytes have not been decoded");
        engine.poll_auth_for_test();
        assert!(shared.portfolio.account_download_complete());
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 1);
        assert_eq!(
            observed.rows,
            if has_position {
                vec![(265598, 2.5)]
            } else {
                vec![]
            }
        );
        client.process_msgs(&mut observed);
        assert_eq!(observed.ends, 1);
        client.disconnect();
        assert!(!shared.portfolio.account_download_complete());
    }
}

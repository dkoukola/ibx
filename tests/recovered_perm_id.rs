//! Broker permanent IDs must match recorded API replies, not local order IDs
//! or hashes of the unrelated FIX OrderID (tag 37).

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ibx::{
    EClient, Wrapper,
    api::types::{Contract, Execution, ExecutionFilter, Order, OrderState},
    bridge::SharedState,
    engine::hot_loop::HotLoop,
};

const LIMIT: &str = include_str!("fixtures/gw1040/scenarios/20260926/lmt_cancel.jsonl");
const BRACKET: &str = include_str!("fixtures/gw1040/scenarios/20260926/bracket.jsonl");
const FILL: &str = include_str!("fixtures/gw1040/scenarios/20260930/i105_combo_fill.jsonl");

fn record(fixture: &str, sequence: u64, leg: &str) -> Vec<u8> {
    let record = fixture
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|record| record["seq"].as_u64() == Some(sequence))
        .expect("recorded reply");
    assert_eq!(record["leg"], leg);
    STANDARD
        .decode(record["raw_b64"].as_str().unwrap())
        .unwrap()
}

fn varint(bytes: &mut &[u8]) -> u64 {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let byte = bytes[0];
        *bytes = &bytes[1..];
        value |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return value;
        }
    }
    panic!("invalid fixture varint");
}

// OrderStatus protobuf: field 1 is API order ID, field 6 is broker permId.
// Decode the captured reference callback instead of deriving expectations
// using the implementation under test.
fn reference_status(fixture: &str, sequence: u64) -> (i64, i64) {
    let bytes = record(fixture, sequence, "api_in");
    assert_eq!(&bytes[..4], &203_u32.to_be_bytes());
    let mut bytes = &bytes[4..];
    let (mut order_id, mut perm_id) = (None, None);
    while !bytes.is_empty() {
        let tag = varint(&mut bytes);
        match tag & 7 {
            0 => {
                let value = i64::try_from(varint(&mut bytes)).unwrap();
                match tag >> 3 {
                    1 => order_id = Some(value),
                    6 => perm_id = Some(value),
                    _ => {}
                }
            }
            1 => bytes = &bytes[8..],
            2 => {
                let size = usize::try_from(varint(&mut bytes)).unwrap();
                bytes = &bytes[size..];
            }
            5 => bytes = &bytes[4..],
            other => panic!("unexpected fixture wire type {other}"),
        }
    }
    (order_id.unwrap(), perm_id.unwrap())
}

#[derive(Default)]
struct Observed {
    statuses: Vec<(i64, i64)>,
    open: Vec<Order>,
    completed: Vec<Order>,
    executions: Vec<Execution>,
}

impl Wrapper for Observed {
    fn open_order(&mut self, _: i64, _: &Contract, order: &Order, _: &OrderState) {
        self.open.push(order.clone());
    }

    fn order_status(
        &mut self,
        order_id: i64,
        _: &str,
        _: f64,
        _: f64,
        _: f64,
        perm_id: i64,
        _: i64,
        _: f64,
        _: i64,
        _: &str,
        _: f64,
    ) {
        self.statuses.push((order_id, perm_id));
    }

    fn completed_order(&mut self, _: &Contract, order: &Order, _: &OrderState) {
        self.completed.push(order.clone());
    }

    fn exec_details(&mut self, _: i64, _: &Contract, execution: &Execution) {
        self.executions.push(execution.clone());
    }
}

fn fixture() -> (HotLoop, EClient, Arc<SharedState>) {
    let shared = Arc::new(SharedState::new());
    let engine = HotLoop::new(shared.clone(), None, None);
    let (tx, _rx) = crossbeam_channel::unbounded();
    let client = EClient::from_parts(
        shared.clone(),
        tx,
        std::thread::spawn(|| {}),
        "DUXXXXXXX".into(),
    );
    (engine, client, shared)
}

#[test]
fn recovered_perm_id_matches_recorded_status_and_cancel_version() {
    let (mut engine, client, shared) = fixture();
    let mut observed = Observed::default();
    for (fix_sequence, api_sequence) in [(2366, 2368), (2375, 2376)] {
        let expected = reference_status(LIMIT, api_sequence);
        assert_eq!(expected, (1, 1_339_547_414));
        engine.inject_ccp_message(&record(LIMIT, fix_sequence, "fix_in"));
        client.process_msgs(&mut observed);
        assert_eq!(
            shared
                .orders
                .get_order_info(expected.0)
                .unwrap()
                .order
                .perm_id,
            expected.1
        );
        if fix_sequence == 2366 {
            client.req_all_open_orders(&mut observed);
            assert_eq!(observed.open.len(), 1);
            assert_eq!(observed.open[0].perm_id, expected.1);
        } else {
            assert_eq!(observed.statuses.last(), Some(&expected));
        }
    }
    client.req_completed_orders(&mut observed);
    assert_eq!(observed.completed.len(), 1);
    assert_eq!(observed.completed[0].perm_id, 1_339_547_414);
}

#[test]
fn recovered_bracket_perm_ids_are_broker_ids_not_local_aliases() {
    let (mut engine, client, shared) = fixture();
    let mut observed = Observed::default();
    for (fix_sequence, api_sequence) in [(2642, 2644), (2632, 2634), (2637, 2639)] {
        let expected = reference_status(BRACKET, api_sequence);
        assert_ne!(expected.0, expected.1);
        engine.inject_ccp_message(&record(BRACKET, fix_sequence, "fix_in"));
        client.process_msgs(&mut observed);
        assert_eq!(
            shared
                .orders
                .get_order_info(expected.0)
                .unwrap()
                .order
                .perm_id,
            expected.1
        );
        client.req_all_open_orders(&mut observed);
        assert!(
            observed
                .open
                .iter()
                .any(|order| (order.order_id, order.perm_id) == expected)
        );
    }
}

#[test]
fn untracked_execution_keeps_the_recorded_broker_perm_id() {
    let (mut engine, client, shared) = fixture();
    shared.orders.begin_execution_history("fixture");
    shared.orders.complete_execution_history("fixture");
    let expected = reference_status(FILL, 13819);
    assert_eq!(expected, (42, 1_947_378_957));
    engine.inject_ccp_message(&record(FILL, 13816, "fix_in"));
    let mut observed = Observed::default();
    client.process_msgs(&mut observed);
    assert!(observed.executions.is_empty(), "no invented live callback");
    client.req_executions(1, &ExecutionFilter::default(), &mut observed);
    client.process_msgs(&mut observed);
    assert_eq!(observed.executions.len(), 1);
    let execution = &observed.executions[0];
    assert_eq!((execution.order_id, execution.perm_id), expected);
}

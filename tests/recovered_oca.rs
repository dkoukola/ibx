//! Recovered protection fields must survive the broker report → API boundary.
//! These are recorded server replies, not locally submitted order objects.

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ibx::{
    EClient, Wrapper,
    api::types::{Contract, Order, OrderState},
    bridge::SharedState,
    engine::hot_loop::HotLoop,
};

const OCA: &str = include_str!("fixtures/gw1040/scenarios/20260926/oca_group.jsonl");
const BRACKET: &str = include_str!("fixtures/gw1040/scenarios/20260926/bracket.jsonl");

#[derive(Default)]
struct Orders {
    orders: Vec<Order>,
    ends: usize,
}

impl Wrapper for Orders {
    fn open_order(&mut self, _: i64, _: &Contract, order: &Order, _: &OrderState) {
        self.orders.push(order.clone());
    }

    fn open_order_end(&mut self) {
        self.ends += 1;
    }
}

fn recover(fixture: &str, sequences: &[u64]) -> Vec<Order> {
    let shared = Arc::new(SharedState::new());
    let mut engine = HotLoop::new(shared.clone(), None, None);
    for sequence in sequences {
        let record = fixture
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|record| record["seq"].as_u64() == Some(*sequence))
            .expect("recorded server report");
        assert_eq!(record["leg"], "fix_in");
        assert_eq!(record["msg_type"], "8");
        let bytes = STANDARD
            .decode(record["raw_b64"].as_str().unwrap())
            .unwrap();
        engine.inject_ccp_message(&bytes);
    }
    let (tx, _rx) = crossbeam_channel::unbounded();
    let client = EClient::from_parts(shared, tx, std::thread::spawn(|| {}), "DUXXXXXXX".into());
    let mut wrapper = Orders::default();
    client.req_all_open_orders(&mut wrapper);
    client.process_msgs(&mut wrapper);
    assert_eq!(wrapper.ends, 1);
    wrapper.orders.sort_by_key(|order| order.order_id);
    wrapper.orders
}

#[test]
fn recovered_named_oca_group_is_reported_without_local_order_tracking() {
    // ACKs alone omit parent authority. Supply the captured full snapshots
    // too; both arrival orders occur on the native wire.
    let orders = recover(OCA, &[2717, 2711, 2740, 2730]);
    assert_eq!(orders.len(), 2);
    assert_eq!((orders[0].order_id, orders[1].order_id), (6, 7));
    for order in orders {
        assert_eq!(order.oca_group, "oca198_1790451961");
        assert_eq!(order.oca_type, 1);
        assert_eq!(order.parent_id, 0);
        assert_eq!(order.account, "DUXXXXXXX");
        assert_eq!(order.order_ref, "fourleg");
    }
}

#[test]
fn recovered_bracket_group_keeps_server_name_separate_from_api_parent_id() {
    // Replay the parent before its children, as a cold-start order snapshot.
    let orders = recover(BRACKET, &[2642, 2627, 2632, 2637]);
    assert_eq!(orders.len(), 3);
    assert_eq!(orders[0].order_id, 3);
    assert!(orders[0].oca_group.is_empty());
    assert_eq!((orders[1].order_id, orders[2].order_id), (4, 5));
    for child in &orders[1..] {
        assert_eq!(child.oca_group, "1339547416");
        assert_eq!(child.oca_type, 3);
        assert_eq!(child.parent_id, 3);
    }
}

#[test]
fn recovered_order_without_oca_does_not_invent_a_group() {
    let orders = recover(BRACKET, &[2642, 2627]);
    assert_eq!(orders.len(), 1);
    assert!(orders[0].oca_group.is_empty());
    assert_eq!(orders[0].parent_id, 0);
}

use super::*;
use crate::md_events::{MdEvent, MdStep, field};

#[test]
fn quote_refresh_preserves_consumers_and_emits_only_new_fields() {
    let (client, rx, shared) = test_client();
    let (sent, received) = std::sync::mpsc::channel();
    let engine = std::thread::spawn(move || {
        while let Ok(command) = rx.recv_timeout(std::time::Duration::from_millis(300)) {
            match command {
                ControlCommand::Subscribe {
                    reply_tx: Some(tx), ..
                } => {
                    let _ = tx.send(Ok(5));
                }
                ControlCommand::RefreshMarketData {
                    req_id,
                    instrument,
                    con_id,
                } => {
                    sent.send((req_id, instrument, con_id)).unwrap();
                }
                _ => {}
            }
        }
    });
    client
        .req_mkt_data(1, &spy_stk(), "", false, false)
        .unwrap();
    client
        .req_mkt_data(2, &spy_stk(), "", false, false)
        .unwrap();
    let quote = crate::types::Quote {
        bid: 100 * PRICE_SCALE,
        ask: 101 * PRICE_SCALE,
        last: 100 * PRICE_SCALE,
        ..Default::default()
    };
    message(&shared, &quote, &[MdStep::Quote, MdStep::Last], None);
    client.process_msgs(&mut SnapRec::default());
    // This old queued price must not acquire a new receipt after renewal.
    message(
        &shared,
        &crate::types::Quote {
            bid: 99 * PRICE_SCALE,
            ..quote
        },
        &[MdStep::Quote],
        None,
    );
    client.refresh_mkt_data(1).unwrap();
    assert_eq!(
        received
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap(),
        (1, 5, 756733)
    );
    // The actual engine performs this only after retiring old native routes.
    shared.market.push_quote(5, &Default::default());
    shared.market.md_events.begin_refresh(5);
    // A new observer during renewal cannot replay the previous BBO either.
    client
        .req_mkt_data(3, &spy_stk(), "", false, false)
        .unwrap();
    let mut callbacks = SnapRec::default();
    client.process_msgs(&mut callbacks);
    assert!(
        !callbacks
            .events
            .iter()
            .any(|event| event.starts_with("price:")),
        "{:?}",
        callbacks.events
    );
    let fresh_last = crate::types::Quote {
        last: quote.last,
        ..Default::default()
    };
    message(&shared, &fresh_last, &[MdStep::Last], None);
    let mut callbacks = SnapRec::default();
    client.process_msgs(&mut callbacks);
    for id in [1, 2, 3] {
        assert!(
            callbacks.events.contains(&format!("price:{id}:4:100")),
            "{:?}",
            callbacks.events
        );
        assert!(
            !callbacks
                .events
                .iter()
                .any(|event| event.starts_with(&format!("price:{id}:1:"))
                    || event.starts_with(&format!("price:{id}:2:")))
        );
    }
    // Exactly the old numerical BBO is new evidence when it arrives from wire.
    message(&shared, &quote, &[MdStep::Quote], None);
    let mut callbacks = SnapRec::default();
    client.process_msgs(&mut callbacks);
    for id in [1, 2, 3] {
        assert!(callbacks.events.contains(&format!("price:{id}:1:100")));
        assert!(callbacks.events.contains(&format!("price:{id}:2:101")));
    }
    assert_eq!(
        client.core.instrument_to_req.lock().unwrap()[&5],
        vec![1, 2, 3]
    );
    engine.join().unwrap();
}

#[test]
fn quote_refresh_boundary_survives_full_ring_and_catchup() {
    let (client, rx, shared) = test_client();
    let engine = top_engine(rx);
    client
        .req_mkt_data(1, &spy_stk(), "", false, false)
        .unwrap();
    let quote = crate::types::Quote {
        bid: 100 * PRICE_SCALE,
        ask: 101 * PRICE_SCALE,
        ..Default::default()
    };
    message(&shared, &quote, &[MdStep::Quote], None);
    client.process_msgs(&mut SnapRec::default());
    let old = MdEvent::new(5, MdStep::Quote).with(field::BID, 99 * PRICE_SCALE);
    while shared.market.md_events.push_message(&[old]) {}
    assert!(!shared.market.md_events.offer(&old));
    assert!(shared.market.md_events.needs_catch_up(5));
    shared.market.push_quote(5, &Default::default());
    shared.market.md_events.begin_refresh(5);
    let mut callbacks = SnapRec::default();
    client.process_msgs(&mut callbacks);
    assert!(
        !callbacks
            .events
            .iter()
            .any(|event| event.starts_with("price:"))
    );
    shared
        .market
        .md_events
        .write_catch_ups(|id| MdEvent::catch_up(id, &Default::default(), Default::default()));
    assert!(!shared.market.md_events.needs_catch_up(5));
    client.process_msgs(&mut callbacks);
    assert!(
        !callbacks
            .events
            .iter()
            .any(|event| event.starts_with("price:"))
    );
    message(&shared, &quote, &[MdStep::Quote], None);
    client.process_msgs(&mut callbacks);
    assert!(callbacks.events.contains(&"price:1:1:100".into()));
    engine.join().unwrap();
}

#[test]
fn quote_refresh_rejects_unknown_snapshot_and_mdoff() {
    let (client, rx, _) = test_client();
    let engine = top_engine(rx);
    assert!(client.refresh_mkt_data(99).is_err());
    client.req_mkt_data(1, &spy_stk(), "", true, false).unwrap();
    assert!(client.refresh_mkt_data(1).is_err());
    client
        .req_mkt_data(2, &spy_stk(), "mdoff,236", false, false)
        .unwrap();
    assert!(client.refresh_mkt_data(2).is_err());
    engine.join().unwrap();
}

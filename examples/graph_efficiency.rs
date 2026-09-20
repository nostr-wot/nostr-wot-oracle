//! Synthetic timings; run with `cargo run --release --example graph_efficiency`.
//! Every trial uses a fresh database and removes it on exit.
#![allow(dead_code, unused_imports)]
#[path = "../src/db/mod.rs"]
mod db;
#[path = "../src/graph/mod.rs"]
mod graph;
use db::{Database, FollowUpdateBatch};
use graph::WotGraph;
use std::time::Instant;
fn main() {
    let directory = tempfile::tempdir().unwrap();
    let keys: Vec<String> = (0..5000).map(|i| format!("{i:064x}")).collect();
    let lists: Vec<Vec<String>> = (0..keys.len())
        .map(|i| {
            (1..=40)
                .map(|j| keys[(i + j * 97) % keys.len()].clone())
                .collect()
        })
        .collect();
    for trial in 0..3 {
        let g = WotGraph::new();
        let t = Instant::now();
        for (i, k) in keys.iter().enumerate() {
            g.update_follows(k, &lists[i], Some("a".into()), Some(1));
        }
        println!(
            "trial={trial} nodes=5000 edges=200000 cold_graph_ms={:.3}",
            t.elapsed().as_secs_f64() * 1000.0
        );
        let t = Instant::now();
        for (i, k) in keys.iter().enumerate() {
            g.update_follows(k, &lists[i], Some("b".into()), Some(2));
        }
        println!(
            "trial={trial} identical_newer_graph_ms={:.3}",
            t.elapsed().as_secs_f64() * 1000.0
        );
        let t = Instant::now();
        for (i, k) in keys.iter().enumerate() {
            let mut v = lists[i].clone();
            v[0] = k.clone();
            g.update_follows(k, &v, Some("c".into()), Some(3));
        }
        println!(
            "trial={trial} one_edge_replacement_graph_ms={:.3}",
            t.elapsed().as_secs_f64() * 1000.0
        );
        let path = directory.path().join(format!("trial-{trial}.db"));
        let d = Database::open(&path).unwrap();
        let t = Instant::now();
        for chunk in (0..keys.len()).collect::<Vec<_>>().chunks(100) {
            let updates: Vec<_> = chunk
                .iter()
                .map(|&i| FollowUpdateBatch {
                    pubkey: &keys[i],
                    follows: &lists[i],
                    event_id: Some("a"),
                    created_at: Some(1),
                })
                .collect();
            d.update_follows_batch(&updates).unwrap();
        }
        println!(
            "trial={trial} cold_database_ms={:.3}",
            t.elapsed().as_secs_f64() * 1000.0
        );
        let restored = WotGraph::new();
        let t = Instant::now();
        d.load_graph(&restored).unwrap();
        println!(
            "trial={trial} database_restore_ms={:.3}",
            t.elapsed().as_secs_f64() * 1000.0
        );
        assert_eq!(restored.stats().edge_count, 200000);
        drop(d);
        println!(
            "trial={trial} database_bytes={}",
            std::fs::metadata(&path).unwrap().len()
        );
    }
    let hubkeys: Vec<String> = (0..50000).map(|i| format!("{i:064x}")).collect();
    for reverse in [false, true] {
        let g = WotGraph::new();
        for k in &hubkeys {
            g.get_or_create_node(k);
        }
        let t = Instant::now();
        for i in 0..hubkeys.len() {
            let i = if reverse { hubkeys.len() - 1 - i } else { i };
            g.update_follows(&hubkeys[i], &["hub".to_string()], Some("a".into()), Some(1));
        }
        println!(
            "hub_authors=50000 reverse={reverse} graph_ms={:.3}",
            t.elapsed().as_secs_f64() * 1000.0
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::compute::{current_unix_secs, ComputeEngine};
    use crate::mock::{ItemDictUpdate, MockItemDictClient, MockKafkaOrderStream};
    use crate::registry::OrderRegistry;
    use crate::server::{ApiError, GetTopKRequest, PriorityServer, GRPC_NOT_FOUND, GRPC_UNAVAILABLE};
    use crate::topology::{PairStatus, TopologyState};
    use crate::types::{
        make_pair_key, DoubleBuffer, OrderMeta, PairInfo, SCORE_SCALE, TBL_HEURISTIC, TOP_K,
    };
    use std::sync::Arc;
    use std::time::Instant;

    fn make_pairs(num_pps: u32, num_bintags: u32) -> Vec<PairInfo> {
        let mut pairs = Vec::new();
        for pps_id in 0..num_pps {
            for bintag_id in 0..num_bintags {
                pairs.push(PairInfo {
                    pps_id,
                    bintag_id,
                    key: make_pair_key(pps_id, bintag_id),
                });
            }
        }
        pairs
    }

    fn make_active_topology(pairs: &[PairInfo]) -> Arc<TopologyState> {
        let topo = Arc::new(TopologyState::new());
        for p in pairs {
            topo.activate(p.pps_id, p.bintag_id);
        }
        topo
    }

    fn ingest_events(engine: &mut ComputeEngine, events: &[crate::mock::KafkaOrderEvent]) {
        let now = current_unix_secs();
        for ev in events {
            // Write base score to every PPS row.
            let base_i16 = (ev.base_score * SCORE_SCALE)
                .clamp(i16::MIN as f32, i16::MAX as f32) as i16;
            for pps_id in 0..engine.matrix.num_pps {
                let row = engine.matrix.row_mut(pps_id);
                if (ev.order_id as usize) < row.len() {
                    row[ev.order_id as usize] = base_i16;
                }
            }

            engine.registry.upsert(OrderMeta {
                order_id: ev.order_id,
                required_qty: ev.required_qty,
                pbt_deadline: ev.pbt_deadline,
                active: true,
                inserted_at_secs: now,
            });

            engine
                .indexes
                .add_order(ev.order_id, &ev.tpids, &ev.eligible_bintag_ids);
        }
    }

    #[test]
    fn test_compute_cycle_basic() {
        let pairs = make_pairs(1, 5);
        let topology = make_active_topology(&pairs);
        let db = Arc::new(DoubleBuffer::new(pairs.len()));
        let mut engine = ComputeEngine::new(pairs, db.clone(), topology.clone());
        engine.item_dict_client = MockItemDictClient::new(1, 1_000);
        engine.item_dict_client.update_fraction = 0.5;

        let stream = MockKafkaOrderStream::new(2, 100_000, 1_000, 5);
        let events = stream.generate_batch(10_000);
        ingest_events(&mut engine, &events);

        for cycle in 1..=2_u32 {
            let dict = engine.item_dict_client.fetch();
            let stats = engine.run_cycle_with_dict(&dict);
            assert!(
                stats.total_ms < 60_000,
                "cycle {} took too long: {} ms",
                cycle,
                stats.total_ms
            );

            let snap = db.load();
            let list = snap.get_list(0, TBL_HEURISTIC, TOP_K);
            assert!(
                !list.is_empty(),
                "cycle {}: heuristic list for pair 0 is empty",
                cycle
            );

            for &id in &list {
                assert!(id >= 0, "cycle {}: invalid order ID {} in list", cycle, id);
            }

            // Check scores are non-increasing using PPSMatrix row for pps_id 0.
            if list.len() > 1 {
                let row = engine.matrix.row(0); // pps_id = 0
                let scores: Vec<i32> = list.iter().map(|&id| row[id as usize] as i32).collect();
                for i in 0..scores.len() - 1 {
                    assert!(
                        scores[i] >= scores[i + 1] - 1,
                        "cycle {}: scores not descending at pos {}: {} < {}",
                        cycle, i, scores[i], scores[i + 1]
                    );
                }
            }
        }
    }

    #[test]
    fn test_topology_warmup_to_active() {
        let pairs = make_pairs(2, 3);
        let topology = make_active_topology(&pairs);
        let db = Arc::new(DoubleBuffer::new(pairs.len()));
        let mut engine = ComputeEngine::new(pairs, db.clone(), topology.clone());
        engine.item_dict_client = MockItemDictClient::new(3, 500);

        let new_pps = 50_u32;
        let new_bt = 77_u32;
        engine.handle_add_pair(new_pps, new_bt);

        let key = make_pair_key(new_pps, new_bt);
        assert_eq!(topology.get(key), Some(PairStatus::WarmingUp));

        let dict = ItemDictUpdate { updates: vec![] };
        engine.run_cycle_with_dict(&dict);

        assert_eq!(topology.get(key), Some(PairStatus::Active));
    }

    #[test]
    fn test_circuit_breaker_warmingup() {
        let pairs = make_pairs(1, 1);
        let topology = Arc::new(TopologyState::new());
        topology.add_warming_up(0, 0);

        let db = Arc::new(DoubleBuffer::new(pairs.len()));
        let server = PriorityServer::new(topology, db, &pairs);

        let req = GetTopKRequest { pps_id: 0, bintag_id: 0, k: 10 };
        let result = server.get_top_k(&req);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ApiError::Unavailable(_)));
        assert_eq!(err.grpc_code(), GRPC_UNAVAILABLE);
    }

    #[test]
    fn test_circuit_breaker_removed() {
        let pairs = make_pairs(1, 1);
        let topology = Arc::new(TopologyState::new());
        topology.remove(0, 0);

        let db = Arc::new(DoubleBuffer::new(pairs.len()));
        let server = PriorityServer::new(topology, db, &pairs);

        let req = GetTopKRequest { pps_id: 0, bintag_id: 0, k: 10 };
        let result = server.get_top_k(&req);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ApiError::NotFound(_)));
        assert_eq!(err.grpc_code(), GRPC_NOT_FOUND);
    }

    #[test]
    fn test_kafka_order_ingestion() {
        let mut registry = OrderRegistry::new();
        let mut indexes = crate::registry::InvertedIndexes::new();
        let now = current_unix_secs();

        let stream = MockKafkaOrderStream::new(5, 100_000, 2_000, 20);
        let events = stream.generate_batch(1_000);

        for ev in &events {
            registry.upsert(OrderMeta {
                order_id: ev.order_id,
                required_qty: ev.required_qty,
                pbt_deadline: ev.pbt_deadline,
                active: true,
                inserted_at_secs: now,
            });
            indexes.add_order(ev.order_id, &ev.tpids, &ev.eligible_bintag_ids);
        }

        let active_count = registry.meta.iter().filter(|m| m.active).count();
        assert!(active_count > 0);

        assert!(!indexes.bintag_to_orders.is_empty());
        assert!(!indexes.tpid_to_orders.is_empty());

        let sample_bintag = *indexes.bintag_to_orders.keys().next().unwrap();
        for &oid in &indexes.bintag_to_orders[&sample_bintag] {
            assert!(
                registry.meta[oid as usize].active,
                "order {} in bintag_to_orders is not active in registry",
                oid
            );
        }
    }

    #[test]
    fn test_order_eviction() {
        let pairs = make_pairs(2, 2);
        let topology = make_active_topology(&pairs);
        let db = Arc::new(DoubleBuffer::new(pairs.len()));
        let mut engine = ComputeEngine::new(pairs, db.clone(), topology.clone());

        // Ingest one order.
        let now = current_unix_secs();
        engine.registry.upsert(OrderMeta {
            order_id: 5,
            required_qty: 1.0,
            pbt_deadline: -1,
            active: true,
            inserted_at_secs: now,
        });
        engine.indexes.add_order(5, &[42u64], &[1u32]);
        for pps_id in 0..engine.matrix.num_pps {
            engine.matrix.row_mut(pps_id)[5] = 1000i16;
        }

        // Evict via removal event.
        engine.handle_order_removed(5);

        assert!(!engine.registry.meta[5].active, "order should be inactive after eviction");
        assert_eq!(engine.matrix.row(0)[5], 0i16, "PPSMatrix slot should be zeroed");
        assert!(
            !engine.indexes.tpid_to_orders.get(&42u64).map(|v| v.contains(&5)).unwrap_or(false),
            "order should be removed from tpid_to_orders"
        );
    }

    #[test]
    fn test_item_dict_latency() {
        let client = MockItemDictClient::new(77, 5_000);
        for call in 1..=3_u32 {
            let t0 = Instant::now();
            let _update = client.fetch();
            let elapsed = t0.elapsed();
            let ms = elapsed.as_millis();
            assert!(ms >= 90, "call {} was too fast: {} ms", call, ms);
            assert!(ms <= 250, "call {} was too slow: {} ms", call, ms);
        }
    }
}

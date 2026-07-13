    use super::*;

    #[test]
    fn connector_response_applied_constructor_sets_applied_status() {
        let response = ConnectorResponse::applied(
            "req-1",
            ConnectorResult::Mutation(MutationResult { affected_rows: 9 }),
        );

        assert_eq!(response.request_id, "req-1");
        assert_eq!(response.status, ResponseStatus::Applied);
        assert!(matches!(
            response.result,
            ConnectorResult::Mutation(MutationResult { affected_rows: 9 })
        ));
    }

    #[test]
    fn connector_response_rejected_constructor_sets_error_result() {
        let response = ConnectorResponse::rejected("req-2", "not allowed");

        assert_eq!(response.request_id, "req-2");
        assert_eq!(response.status, ResponseStatus::Rejected);
        assert_eq!(
            response.result,
            ConnectorResult::Error("not allowed".to_string())
        );
    }

    #[test]
    fn query_result_and_cache_observation_serialize_roundtrip() {
        let result = QueryResult {
            columns: vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::UInt(64),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }],
            rows: vec![vec![b"1".to_vec()]],
            timings: QueryTimings {
                server_parse_ms: 1,
                server_execute_ms: 2,
                server_total_ms: 3,
                network_round_trip_ms: Some(4),
                cache: Some(QueryCacheObservation::Hit {
                    lookup_ms: 5,
                    materialize_ms: 6,
                    snapshot_revision: Some(7),
                }),
            },
        };

        let encoded = bincode::serialize(&result).expect("query result should serialize");
        let decoded: QueryResult = bincode::deserialize(&encoded).expect("query result should deserialize");

        assert_eq!(decoded, result);
    }

    #[test]
    fn query_cache_variants_serialize_roundtrip() {
        let miss = QueryCacheObservation::Miss {
            lookup_ms: 10,
            reason: Some("no snapshot".to_string()),
        };
        let bypassed = QueryCacheObservation::Bypassed {
            reason: QueryCacheBypassReason::UnsupportedShape,
        };

        let miss_encoded = bincode::serialize(&miss).expect("cache miss should serialize");
        let miss_decoded: QueryCacheObservation =
            bincode::deserialize(&miss_encoded).expect("cache miss should deserialize");
        assert_eq!(miss_decoded, miss);

        let bypassed_encoded = bincode::serialize(&bypassed).expect("cache bypass should serialize");
        let bypassed_decoded: QueryCacheObservation =
            bincode::deserialize(&bypassed_encoded).expect("cache bypass should deserialize");
        assert_eq!(bypassed_decoded, bypassed);
    }
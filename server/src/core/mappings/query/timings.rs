use std::time::Instant;

use connector::{ConnectorResponse, ConnectorResult, QueryTimings};

pub(super) fn with_query_timings(
    mut response: ConnectorResponse,
    timings: QueryTimings,
) -> ConnectorResponse {
    if let ConnectorResult::Query(result) = &mut response.result {
        result.timings = timings;
    }

    response
}

pub(super) fn empty_query_timings() -> QueryTimings {
    QueryTimings {
        server_parse_ms: 0,
        server_execute_ms: 0,
        server_total_ms: 0,
        network_round_trip_ms: None,
        cache: None,
    }
}

pub(super) fn make_query_timings(request_start: Instant, parse_ms: u64) -> QueryTimings {
    let total_ms = request_start.elapsed().as_millis() as u64;
    QueryTimings {
        server_parse_ms: parse_ms,
        server_execute_ms: total_ms.saturating_sub(parse_ms),
        server_total_ms: total_ms,
        network_round_trip_ms: None,
        cache: None,
    }
}

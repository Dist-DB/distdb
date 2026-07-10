use clientlib::{ClientOptions, DistDbClient, ExecuteResponse, QueryResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let options = ClientOptions::from_cli_args(&args)?;

    let query_sql = args
        .iter()
        .find_map(|arg| arg.strip_prefix("query="))
        .map(ToOwned::to_owned);

    let execute_sql = args
        .iter()
        .find_map(|arg| arg.strip_prefix("execute="))
        .map(ToOwned::to_owned);

    let client = DistDbClient::new(options)?;
    let connection = client.connect().await?;

    log::info!(
        "connected peer={} session={} database={} user={}",
        connection.active_peer_id,
        connection.session_id.as_deref().unwrap_or("<none>"),
        connection.database.as_deref().unwrap_or("<none>"),
        connection.user.as_deref().unwrap_or("<none>"),
    );

    if let Some(sql) = query_sql {
        let response = client.query(sql).await?;
        print_query_response(&response);
    }

    if let Some(sql) = execute_sql {
        let response = client.execute(sql).await?;
        print_execute_response(&response);
    }

    if query_sql_is_missing_and_execute_is_missing(&args) {
        let response = client.query("show databases").await?;
        print_query_response(&response);
    }

    client.disconnect().await?;
    Ok(())

}

fn query_sql_is_missing_and_execute_is_missing(args: &[String]) -> bool {

    !args.iter().any(|arg| arg.starts_with("query=") || arg.starts_with("execute="))
    
}

fn print_query_response(response: &QueryResponse) {
    
    log::info!(
        "query request={} status={} rows={}",
        response.request_id, response.status, response.row_count
    );

    if !response.columns.is_empty() {
        let columns = response
            .columns
            .iter()
            .map(|column| {
                format!(
                    "#{} {}:{} nullable={} indexed={}",
                    column.ordinal,
                    column.name,
                    column.sql_type,
                    column.nullable,
                    column.indexed
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        log::info!("columns: {}", columns);
    } else {
        log::info!("columns: <none>");
    }

    if response.rows.is_empty() {
        log::info!("query returned no rows");
    } else {
        for row in &response.rows {
            let rendered = row
                .values
                .iter()
                .map(|value| value.render_display())
                .collect::<Vec<_>>()
                .join(" | ");

            log::info!("row: {}", rendered);
        }
    }

    log::info!(
        "timing total={}ms parse={}ms execute={}ms network={}ms",
        response.timings.server_total_ms,
        response.timings.server_parse_ms,
        response.timings.server_execute_ms,
        response.timings.network_round_trip_ms.unwrap_or(0),
    );

}

fn print_execute_response(response: &ExecuteResponse) {

    match response {
        
        ExecuteResponse::Mutation {
            request_id,
            status,
            affected_rows,
        } => {
            log::info!(
                "execute mutation request={} status={} affected_rows={}",
                request_id, status, affected_rows
            );
        },
        
        ExecuteResponse::Schema {
            request_id,
            status,
            table_id,
            schema_revision,
        } => {
            log::info!(
                "execute schema request={} status={} table={} revision={}",
                request_id, status, table_id, schema_revision
            );
        },

        ExecuteResponse::Query(query) => {
            print_query_response(query);
        }

    }

}

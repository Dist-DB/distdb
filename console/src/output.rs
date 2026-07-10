use connector::{ConnectorResponse, ConnectorResult, QueryResult};

pub(crate) fn summarize_response(response: &ConnectorResponse) -> String {

    match &response.result {

        ConnectorResult::Query(result) => format!("query rows={}", result.rows.len()),

        ConnectorResult::Mutation(result) => {
            format!("mutation affected_rows={}", result.affected_rows)
        },

        ConnectorResult::Schema(result) => {
            format!("schema table={} revision={}", result.table_id, result.schema_revision)
        },

        ConnectorResult::Error(message) => format!("error {}", message),

    }

}

pub(crate) fn print_response(response: &ConnectorResponse) {

    match &response.result {

        ConnectorResult::Query(result) => {
            print_query_table(result);

            println!("{} row(s)", result.rows.len());
            println!(
                "timing: server_total={}ms parse={}ms execute={}ms network_rtt={}ms",
                result.timings.server_total_ms,
                result.timings.server_parse_ms,
                result.timings.server_execute_ms,
                result.timings.network_round_trip_ms.unwrap_or(0)
            );

            if let Some(cache) = &result.timings.cache {
                println!("cache: {:?}", cache);
            }
        },

        ConnectorResult::Mutation(result) => {
            println!("ok: {} row(s) affected", result.affected_rows);
        },

        ConnectorResult::Schema(result) => {
            println!(
                "schema updated: table={} revision={}",
                result.table_id, result.schema_revision
            );
        },

        ConnectorResult::Error(message) => {
            println!("error: {}", message);
        },

    }

}

fn print_query_table(result: &QueryResult) {

    if result.columns.is_empty() {
        return;
    }

    let headers = result
        .columns
        .iter()
        .map(|field| format!("{}:{}", field.field_name, field.field_type.sql_variant_display_name()))
        .collect::<Vec<_>>();

    let rows = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|col| String::from_utf8_lossy(col).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let mut widths = headers.iter().map(|h| h.chars().count()).collect::<Vec<_>>();
    
    for row in &rows {
        for (i, col) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(col.chars().count());
            }
        }
    }

    println!("{}", format_table_separator(&widths));
    println!("{}", format_table_row(&headers, &widths));
    println!("{}", format_table_separator(&widths));

    for row in &rows {
        println!("{}", format_table_row(row, &widths));
    }

    println!("{}", format_table_separator(&widths));

}

fn format_table_separator(widths: &[usize]) -> String {

    let mut sep = String::new();
    sep.push('+');
    
    for width in widths {
        sep.push_str(&"-".repeat(*width + 2));
        sep.push('+');
    }

    sep

}

fn format_table_row(cells: &[String], widths: &[usize]) -> String {

    let mut line = String::new();
    line.push('|');

    for (i, width) in widths.iter().enumerate() {
        let cell = cells.get(i).map(|s| s.as_str()).unwrap_or("");
        let padding = width.saturating_sub(cell.chars().count());
        line.push(' ');
        line.push_str(cell);
        line.push_str(&" ".repeat(padding + 1));
        line.push('|');
    }

    line

}

use std::io::BufRead;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportStatementKind {
    Begin,
    Commit,
    Query,
}

impl ImportStatementKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Query => "query",
        }
    }
}

pub(crate) fn classify_import_statement(statement: &str) -> ImportStatementKind {
    let trimmed = statement.trim_start();

    if starts_with_ascii_case_insensitive(trimmed, "begin") {
        return ImportStatementKind::Begin;
    }

    if starts_with_ascii_case_insensitive(trimmed, "commit") {
        return ImportStatementKind::Commit;
    }

    ImportStatementKind::Query
}

pub(crate) fn record_import_statement_timing(
    transaction_state: &mut super::ImportTransactionState,
    kind: ImportStatementKind,
    statement_bytes: usize,
    elapsed_ms: u128,
) {
    transaction_state.statement_calls += 1;
    transaction_state.execute_statement_ms += elapsed_ms;

    if elapsed_ms > transaction_state.max_statement_ms {
        transaction_state.max_statement_ms = elapsed_ms;
        transaction_state.max_statement_kind = Some(kind);
        transaction_state.max_statement_bytes = statement_bytes;

        log::debug!(
            "import new max statement: kind={} bytes={} elapsed_ms={}",
            kind.as_str(),
            statement_bytes,
            elapsed_ms,
        );
    }

    match kind {
        ImportStatementKind::Begin => transaction_state.begin_statement_ms += elapsed_ms,
        ImportStatementKind::Commit => transaction_state.commit_statement_ms += elapsed_ms,
        ImportStatementKind::Query => transaction_state.query_statement_ms += elapsed_ms,
    }
}

pub(crate) fn execute_import_from_reader<R, F>(
    mut reader: R,
    database_id: &str,
    transaction_state: &mut super::ImportTransactionState,
    mut execute_statement: F,
) -> Result<(), String>
where
    R: BufRead,
    F: FnMut(&str, &str, &mut super::ImportTransactionState) -> Result<(), String>,
{
    let mut parser = SqlStatementParser::default();
    let mut pending_bytes = Vec::<u8>::new();

    loop {
        let chunk_len = {
            let buffer = reader.fill_buf().map_err(|err| err.to_string())?;

            if buffer.is_empty() {
                break;
            }

            pending_bytes.extend_from_slice(buffer);
            buffer.len()
        };

        if chunk_len == 0 {
            break;
        }

        reader.consume(chunk_len);

        loop {
            if pending_bytes.is_empty() {
                break;
            }

            let chunk_len = match std::str::from_utf8(&pending_bytes) {
                Ok(valid) => {
                    parser.push_chunk(valid, &mut |statement| {
                        if statement_starts_with_use(statement) {
                            return Ok(());
                        }

                        let normalized_statement = normalize_import_statement(statement);

                        if statement_is_import_dump_directive(&normalized_statement) {
                            return Ok(());
                        }

                        if normalized_statement.len() >= super::IMPORT_LARGE_STATEMENT_BYTES {
                            log::debug!(
                                "import executing large statement: bytes={} head='{}'",
                                normalized_statement.len(),
                                statement_head_token(&normalized_statement)
                            );
                        }

                        stream_import_insert_values_statements(
                            &normalized_statement,
                            import_insert_chunk_target_bytes(),
                            import_insert_chunk_max_tuples(),
                            |import_statement| {
                                if let Err(err) =
                                    execute_statement(database_id, import_statement, transaction_state)
                                {
                                    if should_skip_import_error(import_statement, &err) {
                                        return Ok(());
                                    }

                                    return Err(err);
                                }

                                Ok(())
                            },
                        )?;

                        Ok(())
                    })?;

                    pending_bytes.clear();

                    break;
                }

                Err(err) if err.error_len().is_none() => err.valid_up_to(),

                Err(err) => return Err(err.to_string()),
            };

            if chunk_len == 0 {
                break;
            }

            let valid_chunk =
                std::str::from_utf8(&pending_bytes[..chunk_len]).map_err(|err| err.to_string())?;

            parser.push_chunk(valid_chunk, &mut |statement| {
                if statement_starts_with_use(statement) {
                    return Ok(());
                }

                let normalized_statement = normalize_import_statement(statement);
                if statement_is_import_dump_directive(&normalized_statement) {
                    return Ok(());
                }

                if normalized_statement.len() >= super::IMPORT_LARGE_STATEMENT_BYTES {
                    log::debug!(
                        "import executing large statement: bytes={} head='{}'",
                        normalized_statement.len(),
                        statement_head_token(&normalized_statement)
                    );
                }

                stream_import_insert_values_statements(
                    &normalized_statement,
                    import_insert_chunk_target_bytes(),
                    import_insert_chunk_max_tuples(),
                    |import_statement| {
                        if let Err(err) =
                            execute_statement(database_id, import_statement, transaction_state)
                        {
                            if should_skip_import_error(import_statement, &err) {
                                return Ok(());
                            }

                            return Err(err);
                        }

                        Ok(())
                    },
                )?;

                Ok(())
            })?;

            pending_bytes.drain(..chunk_len);
        }
    }

    parser.flush(&mut |statement| {
        if statement_starts_with_use(statement) {
            return Ok(());
        }

        let normalized_statement = normalize_import_statement(statement);
        if statement_is_import_dump_directive(&normalized_statement) {
            return Ok(());
        }

        if normalized_statement.len() >= super::IMPORT_LARGE_STATEMENT_BYTES {
            log::debug!(
                "import executing large statement: bytes={} head='{}'",
                normalized_statement.len(),
                statement_head_token(&normalized_statement)
            );
        }

        stream_import_insert_values_statements(
            &normalized_statement,
            import_insert_chunk_target_bytes(),
            import_insert_chunk_max_tuples(),
            |import_statement| {
                if let Err(err) = execute_statement(database_id, import_statement, transaction_state) {
                    if should_skip_import_error(import_statement, &err) {
                        return Ok(());
                    }

                    return Err(err);
                }

                Ok(())
            },
        )?;

        Ok(())
    })?;

    Ok(())
}

pub(crate) fn import_duplicate_key_error_is_skippable(error: &str) -> bool {
    let normalized_error = error.to_ascii_lowercase();
    normalized_error.contains("duplicate primary key") || normalized_error.contains("duplicate key")
}

pub(crate) fn statement_is_import_batchable_dml(statement: &str) -> bool {
    let normalized = statement.trim_start();

    starts_with_ascii_case_insensitive(normalized, "insert ")
        || starts_with_ascii_case_insensitive(normalized, "update ")
        || starts_with_ascii_case_insensitive(normalized, "delete ")
        || starts_with_ascii_case_insensitive(normalized, "replace ")
}

pub(crate) fn import_transport_error_is_retryable(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();

    normalized.contains("no queued response")
        || normalized.contains("resource temporarily unavailable")
        || normalized.contains("failed to read response length")
        || normalized.contains("no active peer connection")
        || normalized.contains("connection reset")
        || normalized.contains("broken pipe")
}

pub(crate) fn import_transaction_batch_size() -> usize {
    std::env::var("IMPORT_TX_BATCH_SIZE")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(super::IMPORT_TRANSACTION_BATCH_SIZE)
}

pub(crate) fn import_transaction_batch_max_age_ms() -> u128 {
    std::env::var("IMPORT_TX_BATCH_MAX_AGE_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u128>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(super::IMPORT_TRANSACTION_BATCH_MAX_AGE_MS)
}

fn statement_starts_with_use(statement: &str) -> bool {
    starts_with_ascii_case_insensitive(statement.trim_start(), "use ")
}

fn should_skip_import_error(statement: &str, error: &str) -> bool {
    let normalized_statement = statement.trim_start();
    let normalized_error = error.to_ascii_lowercase();

    if import_duplicate_key_error_is_skippable(error) {
        return true;
    }

    if starts_with_ascii_case_insensitive(normalized_statement, "drop table")
        && normalized_error.contains("not found")
    {
        return true;
    }

    false
}

fn statement_is_import_dump_directive(statement: &str) -> bool {
    let normalized = statement.trim_start();

    starts_with_ascii_case_insensitive(normalized, "lock tables ")
        || starts_with_ascii_case_insensitive(normalized, "unlock tables")
        || starts_with_ascii_case_insensitive(normalized, "drop table ")
        || starts_with_keyword_ascii_case_insensitive(normalized, "delimiter")
        || starts_with_ascii_case_insensitive(normalized, "set ")
        || normalized.starts_with("/*!")
}

fn normalize_import_statement(statement: &str) -> String {
    let mut normalized = statement.to_string();

    // MySQL dumps commonly include index USING clauses that are currently unsupported.
    // Removing them keeps structural intent while allowing first-pass CREATE parsing.
    normalized = remove_case_insensitive_all(&normalized, " USING BTREE");
    normalized = remove_case_insensitive_all(&normalized, " USING HASH");

    // MySQL column definitions often use UNSIGNED numeric modifiers, which the parser
    // currently rejects in CREATE TABLE statements.
    if starts_with_ascii_case_insensitive(normalized.trim_start(), "create table ") {
        normalized = remove_case_insensitive_word_outside_quotes(&normalized, "unsigned");
    }

    normalized
}

fn statement_head_token(statement: &str) -> String {
    statement
        .split_whitespace()
        .next()
        .unwrap_or("<empty>")
        .to_ascii_uppercase()
}

fn stream_import_insert_values_statements<F>(
    statement: &str,
    max_bytes: usize,
    max_tuples_per_chunk: usize,
    mut on_statement: F,
) -> Result<(), String>
where
    F: FnMut(&str) -> Result<(), String>,
{
    let normalized = statement.trim_start();
    if !starts_with_ascii_case_insensitive(normalized, "insert ") {
        return on_statement(statement);
    }

    let Some(values_index) = find_ascii_case_insensitive(statement, " values ") else {
        return on_statement(statement);
    };

    let prefix_end = values_index + " values ".len();
    let prefix = &statement[..prefix_end];
    let values_tail = &statement[prefix_end..];

    let tuples = extract_insert_value_tuples(values_tail);
    if tuples.len() <= 1 {
        return on_statement(statement);
    }

    let mut current = prefix.to_string();
    let mut tuples_in_chunk = 0usize;

    for tuple in tuples {
        let tuple = tuple.trim();
        let additional = if current.len() > prefix.len() {
            tuple.len() + 1
        } else {
            tuple.len()
        };

        if current.len() > prefix.len()
            && (current.len() + additional > max_bytes || tuples_in_chunk >= max_tuples_per_chunk)
        {
            on_statement(&current)?;
            current = prefix.to_string();
            tuples_in_chunk = 0;
        }

        if current.len() > prefix.len() {
            current.push(',');
        }
        current.push_str(tuple);
        tuples_in_chunk += 1;
    }

    if current.len() > prefix.len() {
        on_statement(&current)?;
    }

    Ok(())
}

fn import_insert_chunk_target_bytes() -> usize {
    std::env::var("IMPORT_INSERT_CHUNK_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 8_192)
        .unwrap_or(super::IMPORT_INSERT_CHUNK_TARGET_BYTES)
}

fn import_insert_chunk_max_tuples() -> usize {
    std::env::var("IMPORT_INSERT_CHUNK_MAX_TUPLES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(super::IMPORT_INSERT_CHUNK_MAX_TUPLES)
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }

    let haystack_bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();

    if needle_bytes.len() > haystack_bytes.len() {
        return None;
    }

    (0..=haystack_bytes.len() - needle_bytes.len())
        .find(|index| haystack_bytes[*index..*index + needle_bytes.len()].eq_ignore_ascii_case(needle_bytes))
}

fn starts_with_ascii_case_insensitive(input: &str, prefix: &str) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn starts_with_keyword_ascii_case_insensitive(input: &str, keyword: &str) -> bool {
    if !starts_with_ascii_case_insensitive(input, keyword) {
        return false;
    }

    let Some(next) = input.as_bytes().get(keyword.len()) else {
        return true;
    };

    !(*next).is_ascii_alphanumeric() && *next != b'_'
}

fn extract_insert_value_tuples(values_tail: &str) -> Vec<&str> {
    let mut tuples = Vec::<&str>::new();

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick_quote = false;
    let mut escape_next = false;
    let mut paren_depth = 0usize;
    let mut tuple_start: Option<usize> = None;

    let bytes = values_tail.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let ch = bytes[index] as char;

        if escape_next {
            escape_next = false;
            index += 1;
            continue;
        }

        if (in_single_quote || in_double_quote) && ch == '\\' {
            escape_next = true;
            index += 1;
            continue;
        }

        if ch == '\'' && !in_double_quote && !in_backtick_quote {
            in_single_quote = !in_single_quote;
            index += 1;
            continue;
        }

        if ch == '"' && !in_single_quote && !in_backtick_quote {
            in_double_quote = !in_double_quote;
            index += 1;
            continue;
        }

        if ch == '`' && !in_single_quote && !in_double_quote {
            in_backtick_quote = !in_backtick_quote;
            index += 1;
            continue;
        }

        if in_single_quote || in_double_quote || in_backtick_quote {
            index += 1;
            continue;
        }

        if ch == '(' {
            if paren_depth == 0 {
                tuple_start = Some(index);
            }
            paren_depth += 1;
        } else if ch == ')' && paren_depth > 0 {
            paren_depth -= 1;
            if paren_depth == 0
                && let Some(start) = tuple_start.take()
            {
                tuples.push(&values_tail[start..=index]);
            }
        }

        index += 1;
    }

    tuples
}

fn remove_case_insensitive_all(input: &str, needle: &str) -> String {
    let mut output = String::with_capacity(input.len());

    let mut index = 0;
    while let Some(relative) = find_ascii_case_insensitive(&input[index..], needle) {
        let found = index + relative;
        output.push_str(&input[index..found]);
        index = found + needle.len();
    }

    output.push_str(&input[index..]);
    output
}

fn remove_case_insensitive_word_outside_quotes(input: &str, word: &str) -> String {
    if word.is_empty() {
        return input.to_string();
    }

    let bytes = input.as_bytes();
    let word_bytes = word.as_bytes();
    let mut output = String::with_capacity(input.len());

    let mut index = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick_quote = false;
    let mut escape_next = false;

    while index < bytes.len() {
        let current = bytes[index] as char;

        if (in_single_quote || in_double_quote) && escape_next {
            escape_next = false;
            output.push(current);
            index += 1;
            continue;
        }

        if (in_single_quote || in_double_quote) && current == '\\' {
            escape_next = true;
            output.push(current);
            index += 1;
            continue;
        }

        if !in_double_quote && !in_backtick_quote && current == '\'' {
            in_single_quote = !in_single_quote;
            output.push(current);
            index += 1;
            continue;
        }

        if !in_single_quote && !in_backtick_quote && current == '"' {
            in_double_quote = !in_double_quote;
            output.push(current);
            index += 1;
            continue;
        }

        if !in_single_quote && !in_double_quote && current == '`' {
            in_backtick_quote = !in_backtick_quote;
            output.push(current);
            index += 1;
            continue;
        }

        if in_single_quote || in_double_quote || in_backtick_quote {
            output.push(current);
            index += 1;
            continue;
        }

        let end = index + word_bytes.len();
        let before_ok = index == 0 || !is_identifier_byte(bytes[index - 1]);
        let after_ok = end >= bytes.len() || !is_identifier_byte(bytes[end]);
        if end <= bytes.len()
            && bytes[index..end].eq_ignore_ascii_case(word_bytes)
            && before_ok
            && after_ok
        {
            index = end;
            continue;
        }

        output.push(current);
        index += 1;
    }

    output
}

fn is_identifier_byte(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

#[derive(Default)]
struct SqlStatementParser {
    buffer: String,
    in_single_quote: bool,
    in_double_quote: bool,
    in_backtick_quote: bool,
    in_block_comment: bool,
    in_line_comment: bool,
    pending_dash: bool,
    pending_slash: bool,
    pending_block_comment_star: bool,
}

impl SqlStatementParser {
    fn push_chunk<F>(&mut self, chunk: &str, on_statement: &mut F) -> Result<(), String>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        for ch in chunk.chars() {
            if self.in_line_comment {
                if ch == '\n' {
                    self.in_line_comment = false;
                    if !self.buffer.is_empty()
                        && !self.in_single_quote
                        && !self.in_double_quote
                        && !self.in_backtick_quote
                    {
                        self.buffer.push('\n');
                    }
                }
                continue;
            }

            if self.in_block_comment {
                if self.pending_block_comment_star && ch == '/' {
                    self.in_block_comment = false;
                    self.pending_block_comment_star = false;
                } else {
                    self.pending_block_comment_star = ch == '*';
                }
                continue;
            }

            if self.pending_dash {
                self.pending_dash = false;

                if !self.in_single_quote
                    && !self.in_double_quote
                    && !self.in_backtick_quote
                    && ch == '-'
                {
                    self.in_line_comment = true;
                    continue;
                }

                self.buffer.push('-');
            }

            if self.pending_slash {
                self.pending_slash = false;

                if !self.in_single_quote
                    && !self.in_double_quote
                    && !self.in_backtick_quote
                    && ch == '*'
                {
                    self.in_block_comment = true;
                    self.pending_block_comment_star = false;
                    continue;
                }

                self.buffer.push('/');
            }

            if !self.in_single_quote && !self.in_double_quote && !self.in_backtick_quote {
                if ch == '-' {
                    self.pending_dash = true;
                    continue;
                }

                if ch == '#' {
                    self.in_line_comment = true;
                    continue;
                }

                if ch == '/' {
                    self.pending_slash = true;
                    continue;
                }
            }

            if ch == '\'' && !self.in_double_quote && !self.in_backtick_quote {
                let escaped = self.buffer.ends_with('\\');
                if !escaped {
                    self.in_single_quote = !self.in_single_quote;
                }
                self.buffer.push(ch);
                continue;
            }

            if ch == '"' && !self.in_single_quote && !self.in_backtick_quote {
                let escaped = self.buffer.ends_with('\\');
                if !escaped {
                    self.in_double_quote = !self.in_double_quote;
                }
                self.buffer.push(ch);
                continue;
            }

            if ch == '`' && !self.in_single_quote && !self.in_double_quote {
                self.in_backtick_quote = !self.in_backtick_quote;
                self.buffer.push(ch);
                continue;
            }

            if ch == ';' && !self.in_single_quote && !self.in_double_quote && !self.in_backtick_quote {
                let statement = self.buffer.trim();
                if !statement.is_empty() {
                    on_statement(statement)?;
                }
                self.buffer.clear();
                continue;
            }

            self.buffer.push(ch);
        }

        Ok(())
    }

    fn flush<F>(&mut self, on_statement: &mut F) -> Result<(), String>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        if self.pending_dash {
            self.buffer.push('-');
            self.pending_dash = false;
        }

        if self.pending_slash {
            self.buffer.push('/');
            self.pending_slash = false;
        }

        let statement = self.buffer.trim();
        if !statement.is_empty() {
            on_statement(statement)?;
        }

        self.buffer.clear();
        self.in_line_comment = false;
        self.pending_block_comment_star = false;
        Ok(())
    }
}

#[cfg(test)]
#[path = "import_test.rs"]
mod tests;

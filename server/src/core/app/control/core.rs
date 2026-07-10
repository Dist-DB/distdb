use std::collections::HashSet;
use std::time::Instant;

use connector::{ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, MutationResult};
use serverlib::{
    AclMutationKind, AccountAclEntry, ConcurrentWalManager, DatabaseCatalog, SqlOperation,
    SqlRequest, TransactionId, UserCredential, UserId,
    parse_mysql8_sql_requests,
};

use crate::core::app::helpers::{
    CommandKind, SessionTxMarkerType, command_info, is_staged_dml_query,
    is_staged_dml_requests, is_transactional_read_query, is_transactional_read_requests,
};
use crate::core::app::state::SessionSnapshot;
use crate::core::app::ServerApp;
use crate::core::mappings::query::{
    abort_external_write_group, commit_external_write_group, handle_query_command,
    handle_query_command_with_parsed,
    handle_query_command_in_write_group,
};
use crate::core::transaction_coordinator::QueryRoutingDecision;

impl ServerApp {

    fn quoted_sql_identifier(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn quoted_sql_string(value: &str) -> String {
        format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
    }

    fn sql_literal_for_field_bytes(value: &[u8]) -> String {

        if let Ok(text) = std::str::from_utf8(value) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                let is_numeric = trimmed
                    .chars()
                    .enumerate()
                    .all(|(idx, ch)| ch.is_ascii_digit() || ch == '.' || (idx == 0 && ch == '-'));

                if is_numeric {
                    return trimmed.to_string();
                }
            }

            return Self::quoted_sql_string(text);
        }

        let hex = value
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!("x'{hex}'")
        
    }

    fn sql_column_definition_from_field_spec(field: &connector::FieldSpec) -> String {
        let mut definition = format!(
            "{} {}",
            Self::quoted_sql_identifier(&field.name),
            field.kind.to_sql_string()
        );

        if !field.nullable {
            definition.push_str(" not null");
        }

        if let Some(default_value) = field.default_value.as_ref() {
            definition.push_str(" default ");
            definition.push_str(&Self::sql_literal_for_field_bytes(default_value));
        }

        if matches!(field.indexed, common::schema::FieldIndex::PrimaryKey) {
            definition.push_str(" primary key");
        }

        definition
    }

    fn query_from_schema_command(
        database_id: &str,
        command: &connector::SchemaCommand,
    ) -> connector::DataQuery {

        let sql = match command {
            connector::SchemaCommand::CreateTable { table_id, fields } => {
                let mut table_constraints = Vec::new();

                for field in fields {
                    if matches!(field.indexed, common::schema::FieldIndex::Indexed) {
                        table_constraints.push(format!(
                            "index ({})",
                            Self::quoted_sql_identifier(&field.name)
                        ));
                    }
                }

                let mut definitions = fields
                    .iter()
                    .map(Self::sql_column_definition_from_field_spec)
                    .collect::<Vec<_>>();
                definitions.extend(table_constraints);

                format!(
                    "create table {} ({})",
                    Self::quoted_sql_identifier(table_id),
                    definitions.join(", ")
                )
            }

            connector::SchemaCommand::AlterTable { change } => {
                let mut operations = Vec::new();

                for field_name in &change.remove {
                    operations.push(format!(
                        "drop column {}",
                        Self::quoted_sql_identifier(field_name)
                    ));
                }

                for field in &change.update {
                    operations.push(format!(
                        "modify column {}",
                        Self::sql_column_definition_from_field_spec(field)
                    ));
                }

                for field in &change.add {
                    operations.push(format!(
                        "add column {}",
                        Self::sql_column_definition_from_field_spec(field)
                    ));

                    if matches!(field.indexed, common::schema::FieldIndex::Indexed) {
                        operations.push(format!(
                            "add index ({})",
                            Self::quoted_sql_identifier(&field.name)
                        ));
                    }
                }

                format!(
                    "alter table {} {}",
                    Self::quoted_sql_identifier(&change.table_id),
                    operations.join(", ")
                )
            }

            connector::SchemaCommand::DropTable { table_id } => {
                format!("drop table {}", Self::quoted_sql_identifier(table_id))
            }
        };

        connector::DataQuery {
            database_id: database_id.to_string(),
            sql,
        }

    }

    fn query_from_mutation_command(
        database_id: &str,
        mutation: &connector::DataMutation,
    ) -> connector::DataQuery {

        let sql = match mutation {

            connector::DataMutation::Insert { table_id, values } => {
                let columns = values
                    .iter()
                    .map(|value| Self::quoted_sql_identifier(&value.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                let rendered_values = values
                    .iter()
                    .map(|value| Self::sql_literal_for_field_bytes(&value.value))
                    .collect::<Vec<_>>()
                    .join(", ");

                format!(
                    "insert into {} ({}) values ({})",
                    Self::quoted_sql_identifier(table_id),
                    columns,
                    rendered_values
                )
            },

            connector::DataMutation::Update {
                table_id,
                values,
                predicate_sql,
            } => {
                let assignments = values
                    .iter()
                    .map(|value| {
                        format!(
                            "{} = {}",
                            Self::quoted_sql_identifier(&value.name),
                            Self::sql_literal_for_field_bytes(&value.value)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let mut sql = format!(
                    "update {} set {}",
                    Self::quoted_sql_identifier(table_id),
                    assignments
                );

                if let Some(predicate_sql) = predicate_sql.as_deref() {
                    let predicate_sql = predicate_sql.trim();
                    if !predicate_sql.is_empty() {
                        sql.push_str(" where ");
                        sql.push_str(predicate_sql);
                    }
                }

                sql
            },

            connector::DataMutation::Delete {
                table_id,
                predicate_sql,
            } => {
                let mut sql = format!("delete from {}", Self::quoted_sql_identifier(table_id));

                if let Some(predicate_sql) = predicate_sql.as_deref() {
                    let predicate_sql = predicate_sql.trim();
                    if !predicate_sql.is_empty() {
                        sql.push_str(" where ");
                        sql.push_str(predicate_sql);
                    }
                }

                sql
            },

        };

        connector::DataQuery {
            database_id: database_id.to_string(),
            sql,
        }

    }

    fn parse_create_user_spec(sql: &str) -> Option<(String, String, bool)> {

        let mut rest = sql.trim().trim_end_matches(';').trim();

        if !rest.to_ascii_lowercase().starts_with("create user") {
            return None;
        }

        rest = rest["create user".len()..].trim_start();

        let mut if_not_exists = false;
        if rest.to_ascii_lowercase().starts_with("if not exists") {
            if_not_exists = true;
            rest = rest["if not exists".len()..].trim_start();
        }

        if rest.is_empty() {
            return None;
        }

        let (raw_user, mut remaining) = if let Some(quote) = rest.chars().next().filter(|ch| matches!(ch, '\'' | '"' | '`')) {
            let content = &rest[1..];
            let end = content.find(quote)?;
            (content[..end].to_string(), content[end + 1..].trim_start())
        } else {
            let token_end = rest
                .find(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';')
                .unwrap_or(rest.len());
            (rest[..token_end].to_string(), rest[token_end..].trim_start())
        };

        if remaining.starts_with(',') {
            return None;
        }

        if !remaining.to_ascii_lowercase().starts_with("identified by") {
            return None;
        }

        remaining = remaining["identified by".len()..].trim_start();

        let quote = remaining.chars().next().filter(|ch| matches!(ch, '\'' | '"'))?;
        let password_content = &remaining[1..];
        let password_end = password_content.find(quote)?;

        let password = password_content[..password_end].to_string();
        let trailing = password_content[password_end + 1..].trim_start();
        if !trailing.is_empty() {
            return None;
        }

        if password.trim().is_empty() {
            return None;
        }

        let user_without_host = raw_user.split('@').next().unwrap_or(raw_user.as_str());
        let user_id = common::normalize_identifier!(
            user_without_host
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches('\'')
        );

        if user_id.is_empty() {
            None
        } else {
            Some((user_id, password, if_not_exists))
        }

    }

    fn apply_create_user_requests(
        &mut self,
        request_id: &str,
        session_id: &str,
        query_database_id: &str,
        parsed_requests: &[SqlRequest],
    ) -> Option<ConnectorResponse> {

        let mut saw_create_user_statement = false;
        let create_user_targets = parsed_requests
            .iter()
            .map(|request| {
                let is_create_user = request
                    .sql
                    .trim()
                    .to_ascii_lowercase()
                    .starts_with("create user");

                if is_create_user {
                    saw_create_user_statement = true;
                    Self::parse_create_user_spec(&request.sql)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if !saw_create_user_statement {
            return None;
        }

        if create_user_targets.iter().any(|item| item.is_none()) || create_user_targets.len() != parsed_requests.len() {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                "CREATE USER requires syntax: CREATE USER '<userid>' IDENTIFIED BY '<password>' and cannot be combined with non-CREATE USER statements in the same request".to_string(),
            ));
        }

        let session_user = self.session_user_for_authorization(session_id);
        if !session_user.eq_ignore_ascii_case("root") {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "permission denied for user '{}': only root can execute CREATE USER",
                    session_user,
                ),
            ));
        }

        let normalized_database = common::normalize_identifier!(query_database_id);
        let database_name = if normalized_database.is_empty() {
            "main".to_string()
        } else {
            normalized_database
        };

        let Some(catalog_key) = self.resolve_catalog_key(&database_name) else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found for CREATE USER", database_name),
            ));
        };

        let mut affected_rows = 0_u64;

        let first_schema_wal_ts = self.first_wal_record_timestamp_for_database(&database_name);

        for (target, _) in create_user_targets.iter().zip(parsed_requests.iter()) {
            let (user_id, password, if_not_exists) = target
                .as_ref()
                .expect("create user targets are pre-validated as Some");

            let already_exists = self.catalogs.get(&catalog_key).is_some_and(|catalog| {
                catalog.effective_account_acl_entry(user_id).is_some()
                    || catalog.effective_user_credential(user_id).is_some()
            });

            if already_exists {
                if *if_not_exists {
                    continue;
                }

                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create user failed: user '{}' already exists", user_id),
                ));
            }

            let credential = UserCredential::from_database_user_password(
                UserId(user_id.clone()),
                &database_name,
                password,
                &self.config.node_id,
                first_schema_wal_ts,
            );

            let mut entry = AccountAclEntry::new(UserId(user_id.clone()), database_name.clone());
            entry.database_id = database_name.clone();

            if let Err(err) = self.append_user_credential_change_record(
                &database_name,
                &session_user,
                &credential,
            ) {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("failed to persist CREATE USER credential to WAL: {}", err),
                ));
            }

            if let Err(err) = self.append_account_acl_change_record(&database_name, &session_user, &entry) {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("failed to persist CREATE USER mutation to WAL: {}", err),
                ));
            }

            let Some(catalog) = self.catalogs.get_mut(&catalog_key) else {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' catalog is unavailable", database_name),
                ));
            };

            catalog.upsert_account_acl_entry(entry);
            catalog.upsert_user_credential(credential);
            
            affected_rows += 1;

        }

        Some(ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Mutation(MutationResult { affected_rows }),
        ))

    }

    fn session_user_for_authorization(&self, session_id: &str) -> String {
        self
            .get_session(session_id)
            .map(|state| state.user_id)
            .unwrap_or_else(|| "root".to_string())
    }

    fn parse_database_and_object_from_acl_target(
        database_hint: &str,
        target_database_name: Option<&str>,
        target_object_name: Option<&str>,
    ) -> (String, Option<String>) {

        let normalize_database = |value: &str| {
            let normalized = common::normalize_identifier!(value);
            if normalized.is_empty() {
                "main".to_string()
            } else {
                normalized
            }
        };

        let mut database_name = target_database_name
            .map(normalize_database)
            .unwrap_or_else(|| normalize_database(database_hint));

        let mut object_name = target_object_name
            .map(|value| common::normalize_identifier!(value))
            .filter(|value| !value.is_empty());

        if let Some(qualified) = object_name.as_ref()
            && let Some((database_part, object_part)) = qualified.split_once('.')
        {
            let normalized_database = normalize_database(database_part);
            let normalized_object = common::normalize_identifier!(object_part);

            if !normalized_object.is_empty() {
                database_name = normalized_database;
                object_name = Some(normalized_object);
            }
        }

        (database_name, object_name)

    }

    fn apply_acl_mutation_requests(
        &mut self,
        request_id: &str,
        session_id: &str,
        query_database_id: &str,
        parsed_requests: &[SqlRequest],
    ) -> Option<ConnectorResponse> {

        let contains_acl_mutation = parsed_requests
            .iter()
            .any(|request| !request.acl_mutation_plans().is_empty());

        if !contains_acl_mutation {
            return None;
        }

        if parsed_requests
            .iter()
            .any(|request| request.acl_mutation_plans().is_empty())
        {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                "GRANT/REVOKE cannot be combined with non-ACL statements in the same request"
                    .to_string(),
            ));
        }

        let session_user = self.session_user_for_authorization(session_id);

        if !session_user.eq_ignore_ascii_case("root") {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "permission denied for user '{}': only root can execute GRANT/REVOKE",
                    session_user,
                ),
            ));
        }

        let mut applied_mutations = 0_u64;

        for request in parsed_requests {

            for plan in request.acl_mutation_plans() {

                let (database_name, object_name) = Self::parse_database_and_object_from_acl_target(
                    query_database_id,
                    plan.database_name.as_deref(),
                    plan.object_name.as_deref(),
                );

                let Some(catalog_key) = self.resolve_catalog_key(&database_name) else {
                    return Some(ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("database '{}' not found for ACL mutation", database_name),
                    ));
                };

                let mut entry = self
                    .catalogs
                    .get(&catalog_key)
                    .and_then(|catalog| catalog.effective_account_acl_entry(&plan.grantee).cloned())
                    .unwrap_or_else(|| {
                        AccountAclEntry::new(UserId(plan.grantee.clone()), database_name.clone())
                    });

                entry.database_id = database_name.clone();

                match plan.kind {

                    AclMutationKind::Grant => {
                        if let Some(object_name) = object_name.as_deref() {
                            entry.append_object_privilege(object_name, plan.privilege);
                        } else {
                            entry.append_privilege(plan.privilege);

                            if plan.with_grant_option {
                                entry.append_grant_option_for_privilege(plan.privilege);
                            }
                        }
                    },

                    AclMutationKind::Revoke => {
                        if let Some(object_name) = object_name.as_deref() {
                            entry.revoke_object_privilege(object_name, plan.privilege);
                        } else {
                            entry.revoke_privilege(plan.privilege);
                        }
                    },

                }

                if let Err(err) = self.append_account_acl_change_record(
                    &database_name,
                    &session_user,
                    &entry,
                ) {
                    return Some(ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("failed to persist ACL mutation to WAL: {}", err),
                    ));
                }

                let Some(catalog) = self.catalogs.get_mut(&catalog_key) else {
                    return Some(ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("database '{}' catalog is unavailable", database_name),
                    ));
                };

                catalog.upsert_account_acl_entry(entry);
                applied_mutations += 1;

            }

        }

        Some(ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Mutation(MutationResult {
                affected_rows: applied_mutations,
            }),
        ))

    }

    fn authorization_objects_for_request(request: &SqlRequest) -> Vec<String> {

        if matches!(request.operation, SqlOperation::CreateDatabase | SqlOperation::DropDatabase) {
            return Vec::new();
        }

        request
            .referenced_object_names()
            .into_iter()
            .filter_map(|object_name| {
                let leaf = object_name
                    .rsplit('.')
                    .next()
                    .map(str::trim)
                    .unwrap_or(object_name.as_str());

                let normalized = common::normalize_identifier!(leaf);
                if normalized.is_empty() {
                    None
                } else {
                    Some(normalized)
                }
            })
            .collect()

    }

    fn authorization_database_for_request(
        query_database_id: &str,
        request: &SqlRequest,
    ) -> String {

        if matches!(request.operation, SqlOperation::CreateDatabase) {
            return "main".to_string();
        }

        if let Some(object_name) = request.object_name.as_deref()
            && let Some((database_id, _)) = object_name.split_once('.')
        {
            let normalized = common::normalize_identifier!(database_id);
            if !normalized.is_empty() {
                return normalized;
            }
        }

        let normalized_query_database = common::normalize_identifier!(query_database_id);
        if !normalized_query_database.is_empty() {
            return normalized_query_database;
        }

        "main".to_string()

    }

    fn authorize_sql_requests_for_session(
        &self,
        session_id: &str,
        query_database_id: &str,
        parsed_requests: &[SqlRequest],
    ) -> Result<(), String> {

        let session_user = self.session_user_for_authorization(session_id);

        if session_user.eq_ignore_ascii_case("root") {
            return Ok(());
        }

        for request in parsed_requests {

            let Some(required_privilege) = request.required_privilege else {
                continue;
            };

            let authorization_database =
                Self::authorization_database_for_request(query_database_id, request);

            let Some(catalog_key) = self.resolve_catalog_key(&authorization_database) else {
                return Err(format!(
                    "permission denied for user '{}' on database '{}'",
                    session_user, authorization_database,
                ));
            };

            let Some(catalog) = self.catalogs.get(&catalog_key) else {
                return Err(format!(
                    "permission denied for user '{}' on database '{}'",
                    session_user, authorization_database,
                ));
            };

            let authorization_objects = Self::authorization_objects_for_request(request);

            let Some(acl_entry) = catalog.effective_account_acl_entry(&session_user) else {
                return Err(format!(
                    "permission denied for user '{}' on database '{}'",
                    session_user, authorization_database,
                ));
            };

            if authorization_objects.is_empty() {

                let has_required_privilege = acl_entry
                    .has_privilege_for_object(required_privilege, None);

                if !has_required_privilege {
                    return Err(format!(
                        "permission denied for user '{}': missing '{}' on database '{}'",
                        session_user,
                        required_privilege.as_str(),
                        authorization_database,
                    ));
                }

            } else {

                for object_name in authorization_objects {

                    let has_required_privilege = acl_entry
                        .has_privilege_for_object(required_privilege, Some(&object_name));

                    if !has_required_privilege {
                        return Err(format!(
                            "permission denied for user '{}': missing '{}' on object '{}.{}'",
                            session_user,
                            required_privilege.as_str(),
                            authorization_database,
                            object_name,
                        ));
                    }

                }
            }

        }

        Ok(())

    }

    pub fn handle_read_only_connector_request_for_session(
        &self,
        request: &ConnectorRequest,
        session_id: &str,
    ) -> Option<ConnectorResponse> {

        let ConnectorCommand::Query { query } = &request.command else {
            return None;
        };

        if self
            .transaction_coordinator
            .is_active(session_id)
            .unwrap_or(false)
        {
            return None;
        }

        let parse_start = Instant::now();
        let parsed = parse_mysql8_sql_requests(&query.sql, &query.database_id).ok()?;
        let parse_ms = parse_start.elapsed().as_millis() as u64;
        if parsed.is_empty() {
            return None;
        }

        if let Err(message) = self.authorize_sql_requests_for_session(session_id, &query.database_id, &parsed) {
            return Some(ConnectorResponse::rejected(request.request_id.clone(), message));
        }

        let read_only = parsed.iter().all(|statement| {
            matches!(statement.operation, SqlOperation::Select | SqlOperation::UnionQuery)
        });

        if !read_only {
            return None;
        }

        let mut catalogs = self.catalogs.clone();
        let mut runtime_indexes = self.runtime_indexes.clone();
        let session_state = self.get_session(session_id);
        let connection_id = session_state.map(|s| s.connection_id).unwrap_or(0);

        Some(handle_query_command_with_parsed(
            &request.request_id,
            query,
            &mut catalogs,
            &self.wal,
            &self.node_data_dir,
            &mut runtime_indexes,
            parsed,
            parse_ms,
            session_id,
            connection_id,
            Some("root@localhost".to_string()),
        ))

    }

    fn finalize_group_table_writes(&mut self, table_ids: &HashSet<String>) -> Result<(), String> {

        for table_id in table_ids {

            for catalog in self.catalogs.values_mut() {

                let should_finalize = catalog
                    .table(table_id)
                    .is_some_and(|table| table.status() == serverlib::ObjectStatus::Lock);

                if !should_finalize {
                    continue;
                }

                catalog
                    .finalize_table_write(table_id)
                    .map_err(|err| format!("table write finalize failed table='{}': {}", table_id, err))?;
            
            }
        
        }

        Ok(())

    }

    fn abort_group_table_writes(&mut self, table_ids: &HashSet<String>) {

        for table_id in table_ids {

            for catalog in self.catalogs.values_mut() {

                let should_abort = catalog
                    .table(table_id)
                    .is_some_and(|table| table.status() == serverlib::ObjectStatus::Lock);

                if should_abort {
                    let _ = catalog.abort_table_write(table_id);
                }

            }

        }

    }

    fn parse_lock_table_ids(sql: &str) -> Vec<String> {

        let trimmed = sql.trim().trim_end_matches(';').trim();
        let lowered = trimmed.to_ascii_lowercase();

        let prefix = if lowered.starts_with("lock tables ") {
            "lock tables "
        } else if lowered.starts_with("lock table ") {
            "lock table "
        } else {
            return Vec::new();
        };

        let Some(remainder) = trimmed.get(prefix.len()..) else {
            return Vec::new();
        };

        let mut out = Vec::new();

        for segment in remainder.split(',') {

            let segment = segment.trim();
            if segment.is_empty() {
                continue;
            }

            let table_token = segment.split_whitespace().next().unwrap_or("").trim();
            if table_token.is_empty() {
                continue;
            }

            let table_token = table_token.trim_matches('`').trim_matches('"');
            let table_id = table_token
                .split('.')
                .next_back()
                .unwrap_or(table_token)
                .trim_matches('`')
                .trim_matches('"');

            let normalized = common::normalize_identifier!(table_id);
            if !normalized.is_empty() && !out.iter().any(|existing| existing == &normalized) {
                out.push(normalized);
            }

        }

        out

    }

    pub fn apply_remote_table_lock_state(
        &mut self,
        owner_node_id: &str,
        owner_session_id: &str,
        table_ids: &[String],
        locked: bool,
    ) {

        let owner_id = format!("remote:{}:{}", owner_node_id, owner_session_id);

        if locked {
            self.transaction_coordinator
                .apply_remote_table_locks(&owner_id, table_ids.to_vec());
        } else {
            self.transaction_coordinator
                .release_remote_table_locks(&owner_id, table_ids.to_vec());
        }

    }

    fn staged_query_validation_plan(
        staged_queries: &[connector::DataQuery],
    ) -> Option<(HashSet<String>, bool)> {

        let mut table_ids = HashSet::new();
        let mut insert_only = true;

        for query in staged_queries {
            let parsed = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id).ok()?;
            if parsed.is_empty() {
                return None;
            }

            for statement in parsed {
                let table_id = statement.object_name?;
                table_ids.insert(table_id);

                if !matches!(statement.operation, serverlib::SqlOperation::Insert) {
                    insert_only = false;
                }
            }
        }

        if table_ids.is_empty() {
            return None;
        }

        // For pure INSERT dry-runs, runtime index state is sufficient for duplicate checks.
        // Skipping WAL seed avoids replaying full table snapshots into the sandbox.
        Some((table_ids, insert_only))

    }
    
    pub fn handle_connector_request(&mut self, request: &ConnectorRequest) -> ConnectorResponse {
        self.handle_connector_request_for_session(request, &request.request_id)
    }

    pub fn handle_connector_request_for_session(
        &mut self,
        request: &ConnectorRequest,
        session_id: &str,
    ) -> ConnectorResponse {

        let command_info = command_info(&request.command);
        let command_path = command_info.path;

        // log::info!(
        //     "connector request dispatch request_id={} path={}",
        //     request.request_id,
        //     command_path
        // );

        let response = match command_info.kind {

            CommandKind::CreateDatabase => {

                let ConnectorCommand::CreateDatabase { database_name } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                match DatabaseCatalog::create_new_database(database_name, &self.node_data_dir) {

                    Ok(catalog) => {
                        self.catalogs.insert(catalog.database_id.0.clone(), catalog);

                        ConnectorResponse::applied(
                            request.request_id.clone(),
                            ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
                        )
                    },

                    Err(err) => ConnectorResponse::rejected(
                        request.request_id.clone(),
                        format!("create database failed: {err}"),
                    ),

                }

            },

            CommandKind::Query => {

                let ConnectorCommand::Query { query } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                if let Some(response) =
                    self.handle_transaction_control_query(&request.request_id, session_id, query)
                {
                    response
                } else {
                    let parse_start = Instant::now();
                    let parsed_requests = parse_mysql8_sql_requests(&query.sql, &query.database_id).ok();
                    let parsed_requests_parse_ms = parse_start.elapsed().as_millis() as u64;

                    if let Some(parsed_requests) = parsed_requests.as_ref() {
                        if let Err(message) = self.authorize_sql_requests_for_session(
                            session_id,
                            &query.database_id,
                            parsed_requests,
                        ) {
                            return ConnectorResponse::rejected(request.request_id.clone(), message);
                        }

                        if let Some(response) = self.apply_acl_mutation_requests(
                            &request.request_id,
                            session_id,
                            &query.database_id,
                            parsed_requests,
                        ) {
                            return response;
                        }

                        if let Some(response) = self.apply_create_user_requests(
                            &request.request_id,
                            session_id,
                            &query.database_id,
                            parsed_requests,
                        ) {
                            return response;
                        }

                    }

                    let transaction_active = self
                        .transaction_coordinator
                        .is_active(session_id)
                        .unwrap_or(false);

                    let transactional_read = parsed_requests
                        .as_ref()
                        .map(|parsed| is_transactional_read_requests(parsed))
                        .unwrap_or_else(|| is_transactional_read_query(query));

                    let staged_dml = parsed_requests
                        .as_ref()
                        .map(|parsed| is_staged_dml_requests(parsed))
                        .unwrap_or_else(|| is_staged_dml_query(query));

                    if transaction_active && transactional_read {
                        
                        self.execute_transactional_read(&request.request_id, session_id, query)

                    } else {

                        match self.transaction_coordinator.route_query(
                            session_id,
                            query.clone(),
                            staged_dml,
                        ) {
                            
                            Ok(QueryRoutingDecision::ExecuteImmediately) => {
                                let session_state = self.get_session(session_id);
                                let (connection_id, session_user) = session_state
                                    .map(|s| (s.connection_id, Some(format!("{}@localhost", s.user_id))))
                                    .unwrap_or((0, None));

                                let response = if let Some(parsed_requests) = parsed_requests.clone() {
                                    handle_query_command_with_parsed(
                                        &request.request_id,
                                        query,
                                        &mut self.catalogs,
                                        &self.wal,
                                        &self.node_data_dir,
                                        &mut self.runtime_indexes,
                                        parsed_requests,
                                        parsed_requests_parse_ms as u64,
                                        session_id,
                                        connection_id,
                                        session_user,
                                    )
                                } else {
                                    handle_query_command(
                                        &request.request_id,
                                        query,
                                        &mut self.catalogs,
                                        &self.wal,
                                        &self.node_data_dir,
                                        &mut self.runtime_indexes,
                                        session_id,
                                        connection_id,
                                        session_user,
                                    )
                                };

                                // Update session last_insert_id if INSERT just happened
                                use crate::core::mappings::query::get_and_clear_last_insert_id;
                                if let Some(last_insert_id) = get_and_clear_last_insert_id() {
                                    self.set_last_insert_id(session_id, last_insert_id);
                                }

                                response
                            },
                            
                            Ok(QueryRoutingDecision::Staged) => ConnectorResponse::applied(
                                request.request_id.clone(),
                                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
                            ),
                            
                            Ok(QueryRoutingDecision::Rejected(message)) => {
                                ConnectorResponse::rejected(request.request_id.clone(), message)
                            },
                            
                            Err(err) => ConnectorResponse::rejected(request.request_id.clone(), err),
                            
                        }


                    }

                }
            
            },

            CommandKind::Schema => {

                let ConnectorCommand::Schema { database_id, command } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                let query = Self::query_from_schema_command(database_id, command);
                let translated_request = ConnectorRequest::new(
                    request.request_id.clone(),
                    ConnectorCommand::Query { query },
                );

                self.handle_connector_request_for_session(&translated_request, session_id)

            }

            CommandKind::Mutation => {

                let ConnectorCommand::Mutation {
                    database_id,
                    mutation,
                } = &request.command
                else {
                    unreachable!("command info kind must align with command variant")
                };

                let query = Self::query_from_mutation_command(database_id, mutation);
                let translated_request = ConnectorRequest::new(
                    request.request_id.clone(),
                    ConnectorCommand::Query { query },
                );

                self.handle_connector_request_for_session(&translated_request, session_id)

            }

        };

        match &response.result {

            ConnectorResult::Error(message) => {
                log::warn!(
                    "connector request completed request_id={} path={} status={:?} error={}",
                    request.request_id,
                    command_path,
                    response.status,
                    message
                );
            },

            _ => {
                log::debug!(
                    "connector request completed request_id={} path={} status={:?}",
                    request.request_id,
                    command_path,
                    response.status
                );
            }

        }

        response

    }

    fn handle_transaction_control_query(
        &mut self,
        request_id: &str,
        session_id: &str,
        query: &connector::DataQuery,
    ) -> Option<ConnectorResponse> {

        let normalized = query
            .sql
            .trim()
            .trim_end_matches(';')
            .trim()
            .to_ascii_lowercase();

        let is_lock_tables =
            normalized.starts_with("lock table") || normalized.starts_with("lock tables");

        let is_unlock_tables =
            normalized.starts_with("unlock table") || normalized.starts_with("unlock tables");

        if normalized.starts_with("begin") ||
            normalized.starts_with("start transaction") ||
            is_lock_tables
        {

            if is_lock_tables {

                let table_ids = Self::parse_lock_table_ids(&query.sql);

                if let Err(err) = self
                    .transaction_coordinator
                    .begin_with_table_locks(session_id, table_ids)
                {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                }

            } else if let Err(err) = self.transaction_coordinator.begin(session_id) {
                return Some(ConnectorResponse::rejected(request_id.to_string(), err));
            }

            self.tx_begin_epoch_ms_by_session
                .insert(session_id.to_string(), common::epoch_nanos!());

            let is_lightweight_import_begin = normalized.contains("distdb_import") || is_lock_tables;

            if !is_lightweight_import_begin {

                let snapshot_wal = ConcurrentWalManager::new();

                if let Err(err) = self.seed_sandbox_wal(&snapshot_wal) {
                    let _ = self.transaction_coordinator.rollback(session_id);
                    self.tx_begin_epoch_ms_by_session.remove(session_id);
                    return Some(ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("failed to capture transaction snapshot: {}", err),
                    ));
                }

                self.tx_snapshot_by_session.insert(
                    session_id.to_string(),
                    SessionSnapshot {
                        catalogs: self.catalogs.clone(),
                        runtime_indexes: self.runtime_indexes.clone(),
                        wal: snapshot_wal,
                    },
                );

                self.tx_read_observations_by_session
                    .insert(session_id.to_string(), Vec::new());

            }

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                request_id,
                SessionTxMarkerType::Begin,
                0,
            ) {
                log::warn!("failed to append transaction begin marker: {}", err);
            }

            return Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            ));

        }

        if normalized.starts_with("commit") || is_unlock_tables {

            let staged_queries = match self.transaction_coordinator.take_for_commit(session_id) {
                Ok(staged) => staged,
                Err(err) => {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                }
            };

            let staged_count = staged_queries.len();
            let has_snapshot = self.tx_snapshot_by_session.contains_key(session_id);
            let snapshot_epoch_ms = self
                .tx_begin_epoch_ms_by_session
                .get(session_id)
                .copied()
                .unwrap_or(0);

            let total_affected_rows = match self.commit_staged_queries(
                request_id,
                session_id,
                &staged_queries,
                snapshot_epoch_ms,
            ) {
                
                Ok(total) => total,

                Err(err) => {

                    if has_snapshot {
                        
                        if let Err(rollback_err) = self.transaction_coordinator.rollback(session_id) {
                            log::error!(
                                "failed to rollback transaction after commit error: {}",
                                rollback_err
                            );
                        }

                    } else {

                        if let Err(rollback_err) = self.transaction_coordinator.rollback(session_id) {
                            log::error!(
                                "failed to rollback snapshotless transaction after commit error: {}",
                                rollback_err
                            );
                        }

                        self.tx_begin_epoch_ms_by_session.remove(session_id);
                        self.tx_snapshot_by_session.remove(session_id);
                        self.tx_read_observations_by_session.remove(session_id);
                    }

                    if let Err(marker_err) = self.append_session_tx_marker(
                        session_id,
                        request_id,
                        SessionTxMarkerType::CommitFailed,
                        staged_count,
                    ) {
                        log::warn!("failed to append transaction failure marker: {}", marker_err);
                    }

                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                
                }

            };

            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);

            if let Err(err) = self.transaction_coordinator.finalize_commit(session_id) {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("failed to finalize transaction lock release: {}", err),
                ));
            }

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                request_id,
                SessionTxMarkerType::Commit,
                staged_count,
            ) {
                log::warn!("failed to append transaction commit marker: {}", err);
            }

            return Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult {
                    affected_rows: total_affected_rows,
                }),
            ));

        }

        if normalized.starts_with("rollback") {

            let rolled_back = match self.transaction_coordinator.rollback(session_id) {
                Ok(result) => result,
                Err(err) => {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                }
            };

            if !rolled_back {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    "no active transaction for this session",
                ));
            }

            // cleanup transaction state for this session
            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                request_id,
                SessionTxMarkerType::Rollback,
                0,
            ) {
                log::warn!("failed to append transaction rollback marker: {}", err);
            }

            return Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            ));

        }

        None
        
    }

    fn commit_staged_queries(
        &mut self,
        request_id: &str,
        session_id: &str,
        staged_queries: &[connector::DataQuery],
        snapshot_epoch_ms: u64,
    ) -> Result<u64, String> {

        if snapshot_epoch_ms > 0 {

            if let Some(conflict) = self.detect_write_write_conflict(snapshot_epoch_ms, staged_queries) {
                return Err(conflict);
            }

            if let Some(conflict) = self.detect_predicate_read_conflicts(session_id, snapshot_epoch_ms) {
                return Err(conflict);
            }

        }

        self.validate_staged_queries(request_id, session_id, staged_queries)?;

        let session_state = self.get_session(session_id);
        let (connection_id, session_user) = session_state
            .map(|s| (s.connection_id, Some(format!("{}@localhost", s.user_id))))
            .unwrap_or((0, None));

        let mut total_affected_rows = 0u64;
        let write_group_id = TransactionId(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(common::epoch_nanos!()),
        );
        let mut touched_tables = std::collections::HashSet::new();

        for (idx, staged_query) in staged_queries.iter().enumerate() {

            let apply_request_id = format!("{}::apply{}", request_id, idx + 1);
            let response = handle_query_command_in_write_group(
                &apply_request_id,
                staged_query,
                &mut self.catalogs,
                &self.wal,
                &self.node_data_dir,
                &mut self.runtime_indexes,
                write_group_id,
                &mut touched_tables,
                session_id,
                connection_id,
                session_user.clone(),
            );

            if matches!(response.status, connector::ResponseStatus::Rejected) {
                
                abort_external_write_group(
                    &self.wal,
                    &self.catalogs,
                    &mut self.runtime_indexes,
                    &touched_tables,
                    write_group_id,
                );

                let error = match response.result {
                    ConnectorResult::Error(message) => message,
                    _ => "staged query apply failed".to_string(),
                };

                return Err(format!(
                    "transaction apply failed at staged statement {} after successful dry-run validation: {}",
                    idx + 1,
                    error
                ));

            }

            if let ConnectorResult::Mutation(mutation) = response.result {
                total_affected_rows = total_affected_rows.saturating_add(mutation.affected_rows);
            }

        }

        if let Err(err) = commit_external_write_group(
            &self.wal,
            Some(&self.catalogs),
            Some(&mut self.runtime_indexes),
            &touched_tables,
            write_group_id,
        ) {

            abort_external_write_group(
                &self.wal,
                &self.catalogs,
                &mut self.runtime_indexes,
                &touched_tables,
                write_group_id,
            );

            self.abort_group_table_writes(&touched_tables);
            
            return Err(format!("transaction commit marker append failed: {err}"));

        }

        if let Err(err) = self.finalize_group_table_writes(&touched_tables) {
            self.abort_group_table_writes(&touched_tables);
            return Err(err);
        }

        Ok(total_affected_rows)

    }

    fn validate_staged_queries(
        &self,
        request_id: &str,
        session_id: &str,
        staged_queries: &[connector::DataQuery],
    ) -> Result<(), String> {

        let validation_plan = Self::staged_query_validation_plan(staged_queries);

        let (mut sandbox_catalogs, snapshot_runtime_indexes, snapshot_wal) =        
            if let Some(snapshot) = self.tx_snapshot_by_session.get(session_id) {
                
                (
                    snapshot.catalogs.clone(),
                    snapshot.runtime_indexes.clone(),
                    Some(&snapshot.wal),
                )

            } else if let Some((_table_ids, insert_only)) = &validation_plan {

                if !insert_only {
                    return Err("missing transaction snapshot for validation".to_string());
                }
                
                (
                    self.catalogs.clone(),
                    self.runtime_indexes.clone(),
                    None,
                )

            } else {

                return Err("missing transaction snapshot for validation".to_string());

            };

        let sandbox_wal = ConcurrentWalManager::new();

        let mut sandbox_indexes = if let Some((table_ids, skip_wal_seed)) = validation_plan {
            let scoped_indexes = snapshot_runtime_indexes.clone_for_tables(&sandbox_catalogs, &table_ids);

            if skip_wal_seed {

            } else {
                let Some(source_wal) = snapshot_wal else {
                    return Err("missing transaction snapshot for validation".to_string());
                };

                self.seed_sandbox_wal_from_source_for_tables(&sandbox_catalogs, source_wal, &sandbox_wal, &table_ids)
                    .map_err(|err| format!("failed to seed validation WAL snapshot: {}", err))?;
            }

            scoped_indexes

        } else {
            
            let Some(source_wal) = snapshot_wal else {
                return Err("missing transaction snapshot for validation".to_string());
            };

            self.seed_sandbox_wal_from_source(&sandbox_catalogs, source_wal, &sandbox_wal)
                .map_err(|err| format!("failed to seed validation WAL snapshot: {}", err))?;
            let cloned = snapshot_runtime_indexes.clone();

            cloned

        };

        let session_state = self.get_session(session_id);
        let (connection_id, session_user) = session_state
            .map(|s| (s.connection_id, Some(format!("{}@localhost", s.user_id))))
            .unwrap_or((0, None));

        let validation_write_group_id = TransactionId(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(common::epoch_nanos!()),
        );

        let mut validation_touched_tables = std::collections::HashSet::new();

        for (idx, staged_query) in staged_queries.iter().enumerate() {

            let dry_run_request_id = format!("{}::dryrun{}", request_id, idx + 1);
            
            let response = handle_query_command_in_write_group(
                &dry_run_request_id,
                staged_query,
                &mut sandbox_catalogs,
                &sandbox_wal,
                &self.node_data_dir,
                &mut sandbox_indexes,
                validation_write_group_id,
                &mut validation_touched_tables,
                session_id,
                connection_id,
                session_user.clone(),
            );

            if matches!(response.status, connector::ResponseStatus::Rejected) {

                let error = match response.result {
                    ConnectorResult::Error(message) => message,
                    _ => "staged query dry-run failed".to_string(),
                };

                return Err(format!(
                    "transaction validation failed at staged statement {}: {}",
                    idx + 1,
                    error
                ));

            }

        }

        Ok(())

    }
    
}

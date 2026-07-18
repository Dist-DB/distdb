mod catalogs;
mod core;
mod explain;
mod timings;

use std::collections::{HashMap, HashSet};
use std::path::Path;

use connector::{ConnectorResponse, DataQuery};
use serverlib::{
	ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore, TransactionId,
};

#[allow(unused_imports)]
pub(crate) use core::{
	abort_external_write_group, commit_external_write_group,
	handle_query_command_with_parsed, handle_query_command_with_parsed_and_session_variables,
	get_and_clear_last_insert_id,
	SessionVariableOverrides,
};

pub(crate) fn handle_query_command(
	request_id: &str,
	query: &DataQuery,
	catalogs: &mut HashMap<String, DatabaseCatalog>,
	wal: &ConcurrentWalManager,
	node_data_dir: &Path,
	runtime_indexes: &mut RuntimeIndexStore,
	session_id: &str,
	connection_id: usize,
	session_user: Option<String>,
) -> ConnectorResponse {
	core::handle_query_command(
		request_id,
		query.database_id.as_str(),
		query.sql.as_str(),
		catalogs,
		wal,
		node_data_dir,
		runtime_indexes,
		session_id,
		connection_id,
		session_user,
	)
}

pub(crate) fn handle_query_command_with_session_variables(
	request_id: &str,
	query: &DataQuery,
	catalogs: &mut HashMap<String, DatabaseCatalog>,
	wal: &ConcurrentWalManager,
	node_data_dir: &Path,
	runtime_indexes: &mut RuntimeIndexStore,
	session_id: &str,
	connection_id: usize,
	session_user: Option<String>,
	session_variable_overrides: &mut SessionVariableOverrides,
) -> ConnectorResponse {
	core::handle_query_command_with_session_variables(
		request_id,
		query.database_id.as_str(),
		query.sql.as_str(),
		catalogs,
		wal,
		node_data_dir,
		runtime_indexes,
		session_id,
		connection_id,
		session_user,
		session_variable_overrides,
	)
}

pub(crate) fn handle_query_command_in_write_group(
	request_id: &str,
	query: &DataQuery,
	catalogs: &mut HashMap<String, DatabaseCatalog>,
	wal: &ConcurrentWalManager,
	node_data_dir: &Path,
	runtime_indexes: &mut RuntimeIndexStore,
	write_group_id: TransactionId,
	touched_tables: &mut HashSet<String>,
	session_id: &str,
	connection_id: usize,
	session_user: Option<String>,
) -> ConnectorResponse {
	core::handle_query_command_in_write_group(
		request_id,
		query.database_id.as_str(),
		query.sql.as_str(),
		catalogs,
		wal,
		node_data_dir,
		runtime_indexes,
		write_group_id,
		touched_tables,
		session_id,
		connection_id,
		session_user,
	)
}

pub(crate) fn handle_query_command_in_write_group_with_session_variables(
	request_id: &str,
	query: &DataQuery,
	catalogs: &mut HashMap<String, DatabaseCatalog>,
	wal: &ConcurrentWalManager,
	node_data_dir: &Path,
	runtime_indexes: &mut RuntimeIndexStore,
	write_group_id: TransactionId,
	touched_tables: &mut HashSet<String>,
	session_id: &str,
	connection_id: usize,
	session_user: Option<String>,
	session_variable_overrides: &mut SessionVariableOverrides,
) -> ConnectorResponse {
	core::handle_query_command_in_write_group_with_session_variables(
		request_id,
		query.database_id.as_str(),
		query.sql.as_str(),
		catalogs,
		wal,
		node_data_dir,
		runtime_indexes,
		write_group_id,
		touched_tables,
		session_id,
		connection_id,
		session_user,
		session_variable_overrides,
	)
}

#[cfg(test)]
use self::catalogs::{resolve_catalog, resolve_catalog_mut};
#[cfg(test)]
use self::explain::{explain_inner_statement, explain_join_mutation_plan, explain_mutation_plan};

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;

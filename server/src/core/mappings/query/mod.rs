mod catalogs;
mod core;
mod explain;
mod timings;

#[allow(unused_imports)]
pub(crate) use core::{
	abort_external_write_group, commit_external_write_group, handle_query_command,
	handle_query_command_in_write_group, handle_query_command_in_write_group_with_session_variables,
	handle_query_command_with_parsed, handle_query_command_with_parsed_and_session_variables,
	handle_query_command_with_session_variables,
	get_and_clear_last_insert_id,
	SessionVariableOverrides,
};

#[cfg(test)]
use self::catalogs::{resolve_catalog, resolve_catalog_mut};
#[cfg(test)]
use self::explain::{explain_inner_statement, explain_join_mutation_plan, explain_mutation_plan};

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;

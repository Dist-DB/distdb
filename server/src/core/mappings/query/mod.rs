mod catalogs;
mod core;
mod explain;
mod timings;

pub(crate) use core::handle_query_command;

#[cfg(test)]
use self::catalogs::{resolve_catalog, resolve_catalog_mut};
#[cfg(test)]
use self::explain::{explain_inner_statement, explain_join_mutation_plan, explain_mutation_plan};

#[cfg(test)]
mod tests;

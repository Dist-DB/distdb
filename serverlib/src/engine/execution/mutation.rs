use std::collections::{HashMap, HashSet};

use crate::{
    ConcurrentWalManager, DatabaseCatalog, MaterializedRelationRow, RuntimeIndexStore,
    SelectCondition, SelectJoin, SelectRelation,
};

use super::{
    build_joined_row_tuples, row_matches_select_condition, JoinedRowTuple,
};

pub fn select_mutation_target_rows<E>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    relations: &[SelectRelation],
    pushdown_conditions: &[Option<SelectCondition>],
    joins: &[SelectJoin],
    where_condition: Option<&SelectCondition>,
    row_matches: &mut E,
) -> Result<Vec<MaterializedRelationRow>, String>
where
    E: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> bool,
{
    let joined_rows = build_joined_row_tuples(
        catalog,
        wal,
        runtime_indexes,
        relations,
        pushdown_conditions,
        joins,
        row_matches,
    )?;

    Ok(deduplicate_target_rows(joined_rows, catalog, wal, runtime_indexes, where_condition))
}

fn deduplicate_target_rows(
    joined_rows: Vec<JoinedRowTuple>,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    where_condition: Option<&SelectCondition>,
) -> Vec<MaterializedRelationRow> {

    let mut selected_rows = Vec::new();
    let mut seen_row_ids = HashSet::new();

    for row_tuple in joined_rows {
        if !row_matches_select_condition(
            &row_tuple,
            where_condition,
            catalog,
            wal,
            runtime_indexes,
        ) {
            continue;
        }

        let Some(row) = row_tuple.first_relation_row() else {
            continue;
        };

        if seen_row_ids.insert(row.row_id) {
            selected_rows.push(row);
        }
    }

    selected_rows

}

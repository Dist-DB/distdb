use std::collections::HashMap;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::engine::execution::{
    row_matches_select_condition_result, ConditionValueProvider,
};
use crate::{
    collect_indexable_equality_filters_for_schema, execute_joined_select_plan,
    execute_projection_only_select_plan, execute_relation_select_plan,
    plan_relation_access,
    ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore, SelectReadPlan,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CursorDiagnostics {
    pub fetched_rows: usize,
    pub not_found: bool,
    pub opened: bool,
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlCursorFrame {
    current_row: Option<HashMap<String, Vec<u8>>>,
    local_bindings: HashMap<String, Vec<u8>>,
    pub diagnostics: CursorDiagnostics,
}

pub trait SqlCursorSource {
    fn open(&mut self) -> Result<(), String>;
    fn fetch_next(&mut self) -> Result<Option<HashMap<String, Vec<u8>>>, String>;
    fn close(&mut self) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorDirective<R> {
    Next,
    Break,
    Return(R),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VecSqlCursorSource {
    rows: Vec<HashMap<String, Vec<u8>>>,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SelectReadPlanCursorSource {
    rows: Vec<HashMap<String, Vec<u8>>>,
    index: usize,
}

impl VecSqlCursorSource {
    pub fn new(rows: Vec<HashMap<String, Vec<u8>>>) -> Self {
        Self { rows, index: 0 }
    }
}

impl SelectReadPlanCursorSource {
    pub fn from_read_plan(
        catalog: &DatabaseCatalog,
        wal: &ConcurrentWalManager,
        runtime_indexes: &RuntimeIndexStore,
        read_plan: &SelectReadPlan,
    ) -> Result<Self, String> {

        let execution_result = if !read_plan.joins.is_empty() {

            execute_joined_select_plan(
                catalog,
                wal,
                runtime_indexes,
                read_plan,
                &mut |function| evaluate_inbuilt_sql_function(function),
                &mut |row_map, condition| {
                    row_matches_select_condition_result(
                        row_map,
                        condition,
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                },
                &mut |row_tuple, condition| {
                    row_matches_select_condition_result(
                        row_tuple,
                        condition,
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                },
            )?
        
        } else if read_plan.table_id.is_empty() {

            execute_projection_only_select_plan(read_plan, &mut |function| {
                evaluate_inbuilt_sql_function(function)
            })?
        
        } else {

            let table_id = read_plan.table_id.as_str();

            let table = catalog
                .table(table_id)
                .ok_or_else(|| format!("cursor source select failed: table '{}' not found", table_id))?;

            let schema = catalog
                .table_schema(table_id)
                .ok_or_else(|| format!("cursor source select failed: table '{}' not found", table_id))?;

            let mut index_filter_map = HashMap::new();

            let allow_index_short_circuit = read_plan
                .where_condition
                .as_ref()
                .map(|condition| {
                    collect_indexable_equality_filters_for_schema(
                        schema,
                        condition,
                        &mut index_filter_map,
                    )
                })
                .unwrap_or(true);

            let access_plan = plan_relation_access(table, allow_index_short_circuit, index_filter_map);

            execute_relation_select_plan(
                wal,
                table,
                schema,
                runtime_indexes,
                read_plan,
                &access_plan,
                &mut |function| evaluate_inbuilt_sql_function(function),
                &mut |row_map, condition| {
                    row_matches_select_condition_result(
                        row_map,
                        condition,
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                },
            )?

        };

        let rows = execution_result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .filter_map(|(index, value)| {
                        execution_result.columns.get(index).map(|column| {
                            (common::normalize_identifier!(&column.field_name), value)
                        })
                    })
                    .collect::<HashMap<String, Vec<u8>>>()
            })
            .collect::<Vec<_>>();

        Ok(Self { rows, index: 0 })
    
    }

}

impl SqlCursorFrame {
    
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current_row(&self) -> Option<&HashMap<String, Vec<u8>>> {
        self.current_row.as_ref()
    }

    pub fn set_local_binding(&mut self, binding_name: impl Into<String>, value: Vec<u8>) {
        self.local_bindings
            .insert(common::normalize_identifier!(binding_name.into()), value);
    }

    pub fn remove_local_binding(&mut self, binding_name: &str) {
        self.local_bindings
            .remove(&common::normalize_identifier!(binding_name));
    }

    pub fn clear_local_bindings(&mut self) {
        self.local_bindings.clear();
    }

    pub fn local_binding(&self, binding_name: &str) -> Option<&Vec<u8>> {
        self.local_bindings
            .get(&common::normalize_identifier!(binding_name))
    }

    fn set_current_row(&mut self, row: HashMap<String, Vec<u8>>) {
        self.current_row = Some(row);
        self.diagnostics.not_found = false;
        self.diagnostics.fetched_rows += 1;
    }

    fn mark_not_found(&mut self) {
        self.current_row = None;
        self.diagnostics.not_found = true;
    }

}

impl ConditionValueProvider for SqlCursorFrame {
    
    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {

        let normalized = common::normalize_identifier!(field_name);

        if let Some(row) = self.current_row.as_ref() {
            if let Some(value) = row.get(&normalized) {
                return Some(value);
            }

            if let Some((_, unqualified)) = normalized.split_once('.') {
                if let Some(value) = row.get(unqualified) {
                    return Some(value);
                }
            }

            if !normalized.contains('.') {
                if let Some((_, value)) = row.iter().find(|(field_name, _)| {
                    field_name
                        .split_once('.')
                        .is_some_and(|(_, column_name)| column_name == normalized)
                }) {
                    return Some(value);
                }
            }
        }

        self.local_bindings.get(&normalized)
    
    }

}

impl SqlCursorSource for VecSqlCursorSource {

    fn open(&mut self) -> Result<(), String> {
        self.index = 0;
        Ok(())
    }

    fn fetch_next(&mut self) -> Result<Option<HashMap<String, Vec<u8>>>, String> {
        if self.index >= self.rows.len() {
            return Ok(None);
        }

        let row = self
            .rows
            .get(self.index)
            .cloned()
            .ok_or_else(|| "cursor fetch failed: row index out of bounds".to_string())?;

        self.index += 1;

        Ok(Some(row))
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }

}

impl SqlCursorSource for SelectReadPlanCursorSource {
    fn open(&mut self) -> Result<(), String> {
        self.index = 0;
        Ok(())
    }

    fn fetch_next(&mut self) -> Result<Option<HashMap<String, Vec<u8>>>, String> {
        if self.index >= self.rows.len() {
            return Ok(None);
        }

        let row = self
            .rows
            .get(self.index)
            .cloned()
            .ok_or_else(|| "cursor fetch failed: row index out of bounds".to_string())?;

        self.index += 1;

        Ok(Some(row))
    }

    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

pub fn execute_sql_cursor<S, F, R>(
    source: &mut S,
    frame: &mut SqlCursorFrame,
    on_row: &mut F,
) -> Result<Option<R>, String>
where
    S: SqlCursorSource,
    F: FnMut(&mut SqlCursorFrame) -> Result<CursorDirective<R>, String>,
{

    source.open()?;
    frame.diagnostics.opened = true;
    frame.diagnostics.closed = false;
    frame.diagnostics.not_found = false;
    frame.diagnostics.fetched_rows = 0;

    loop {
        let next_row = match source.fetch_next() {
            
            Ok(row) => row,
            
            Err(err) => {
                let _ = source.close();
                frame.diagnostics.closed = true;
                return Err(err);
            }
            
        };

        let Some(row) = next_row else {
            frame.mark_not_found();
            break;
        };

        frame.set_current_row(row);

        match on_row(frame) {

            Ok(CursorDirective::Next) => {}

            Ok(CursorDirective::Break) => break,

            Ok(CursorDirective::Return(value)) => {
                source.close()?;
                frame.diagnostics.closed = true;
                return Ok(Some(value));
            },

            Err(err) => {
                let _ = source.close();
                frame.diagnostics.closed = true;
                return Err(err);
            }

        }

    }

    source.close()?;
    frame.diagnostics.closed = true;

    Ok(None)

}

#[cfg(test)]
#[path = "cursor_test.rs"]
mod tests;

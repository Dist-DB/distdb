use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    SelectComparisonOp, SelectCondition, SelectJoin, SelectPredicate, SelectReadPlan,
    SelectRelation,
};

use super::super::sql::{compare_like_value, compare_regex_value, compare_row_value};

#[derive(Debug, Clone)]
pub struct MaterializedRelationRow {
    pub row_id: u64,
    pub row_map: Arc<HashMap<String, Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct JoinedRowMember {
    pub qualifier: String,
    pub row_id: Option<u64>,
    pub row_map: Option<Arc<HashMap<String, Vec<u8>>>>,
}

#[derive(Debug, Clone)]
pub struct JoinedRowTuple {
    pub members: Vec<JoinedRowMember>,
}

pub trait ConditionValueProvider {
    fn value(&self, field_name: &str) -> Option<&Vec<u8>>;
}

pub struct JoinedRowCandidateProvider<'a> {
    pub left: &'a JoinedRowTuple,
    pub right_relation: &'a SelectRelation,
    pub right_row: &'a MaterializedRelationRow,
}

pub struct QualifiedRowMapProvider<'a> {
    pub qualifier: &'a str,
    pub row_map: &'a HashMap<String, Vec<u8>>,
}

pub struct ChainedConditionValueProvider<'a> {
    pub primary: &'a dyn ConditionValueProvider,
    pub fallback: &'a dyn ConditionValueProvider,
}

pub struct UnqualifiedFieldFallbackProvider<'a> {
    pub provider: &'a dyn ConditionValueProvider,
}

impl ConditionValueProvider for HashMap<String, Vec<u8>> {
    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {
        self.get(field_name)
    }
}

impl ConditionValueProvider for QualifiedRowMapProvider<'_> {

    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {

        if let Some(value) = self.row_map.get(field_name) {
            return Some(value);
        }

        let (qualifier, column_name) = field_name.split_once('.')?;
        if qualifier != self.qualifier {
            return None;
        }

        self.row_map.get(column_name)
    }
    
}

impl ConditionValueProvider for JoinedRowCandidateProvider<'_> {

    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {

        if let Some(value) = self.left.value(field_name) {
            return Some(value);
        }

        let (qualifier, column_name) = field_name.split_once('.')?;
        if qualifier != relation_qualifier(self.right_relation) {
            return None;
        }

        self.right_row.row_map.get(column_name)
    }

}

impl ConditionValueProvider for ChainedConditionValueProvider<'_> {

    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {
        self.primary.value(field_name).or_else(|| self.fallback.value(field_name))
    }

}

impl ConditionValueProvider for UnqualifiedFieldFallbackProvider<'_> {

    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {

        self.provider.value(field_name).or_else(|| {
            field_name
                .split_once('.')
                .and_then(|(_, column_name)| self.provider.value(column_name))
        })
    
    }

}

impl JoinedRowTuple {

    pub fn from_relation_row(relation: &SelectRelation, row: MaterializedRelationRow) -> Self {

        Self {
            members: vec![JoinedRowMember {
                qualifier: relation_qualifier(relation).to_string(),
                row_id: Some(row.row_id),
                row_map: Some(row.row_map),
            }],
        }

    }

    pub fn from_missing_relations(relations: &[SelectRelation]) -> Self {
        
        Self {
            members: relations
                .iter()
                .map(|relation| JoinedRowMember {
                    qualifier: relation_qualifier(relation).to_string(),
                    row_id: None,
                    row_map: None,
                })
                .collect(),
        }
    }

    pub fn append(&self, relation: &SelectRelation, row: &MaterializedRelationRow) -> Self {

        let mut members = self.members.clone();
        members.push(JoinedRowMember {
            qualifier: relation_qualifier(relation).to_string(),
            row_id: Some(row.row_id),
            row_map: Some(Arc::clone(&row.row_map)),
        });
        
        Self { members }

    }

    pub fn append_missing_relation(&self, relation: &SelectRelation) -> Self {

        let mut members = self.members.clone();
        members.push(JoinedRowMember {
            qualifier: relation_qualifier(relation).to_string(),
            row_id: None,
            row_map: None,
        });
        
        Self { members }

    }

    pub fn first_relation_row(&self) -> Option<MaterializedRelationRow> {
        
        let member = self.members.first()?;
        Some(MaterializedRelationRow {
            row_id: member.row_id?,
            row_map: Arc::clone(member.row_map.as_ref()?),
        })

    }

    pub fn value(&self, field_name: &str) -> Option<&Vec<u8>> {

        let (qualifier, column_name) = field_name.split_once('.')?;

        self.members
            .iter()
            .find(|member| member.qualifier == qualifier)
            .and_then(|member| member.row_map.as_ref())
            .and_then(|row_map| row_map.get(column_name))

    }

}

impl ConditionValueProvider for JoinedRowTuple {

    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {
        JoinedRowTuple::value(self, field_name)
    }
    
}

pub fn row_matches_condition_with(
    provider: &dyn ConditionValueProvider,
    condition: Option<&SelectCondition>,
    subquery_values: &mut impl FnMut(&dyn ConditionValueProvider, &SelectReadPlan) -> HashSet<Vec<u8>>,
    subquery_exists: &mut impl FnMut(&dyn ConditionValueProvider, &SelectReadPlan) -> bool,
    subquery_scalar: &mut impl FnMut(&dyn ConditionValueProvider, &SelectReadPlan) -> Option<Vec<u8>>,
) -> bool {

    row_matches_condition_with_result(
        provider,
        condition,
        &mut |provider, subquery| Ok(subquery_values(provider, subquery)),
        &mut |provider, subquery| Ok(subquery_exists(provider, subquery)),
        &mut |provider, subquery| Ok(subquery_scalar(provider, subquery)),
    )
    .unwrap_or(false)

}

pub fn row_matches_condition_with_result(
    provider: &dyn ConditionValueProvider,
    condition: Option<&SelectCondition>,
    subquery_values: &mut impl FnMut(&dyn ConditionValueProvider, &SelectReadPlan) -> Result<HashSet<Vec<u8>>, String>,
    subquery_exists: &mut impl FnMut(&dyn ConditionValueProvider, &SelectReadPlan) -> Result<bool, String>,
    subquery_scalar: &mut impl FnMut(&dyn ConditionValueProvider, &SelectReadPlan) -> Result<Option<Vec<u8>>, String>,
) -> Result<bool, String> {

    let Some(condition) = condition else {
        return Ok(true);
    };

    match condition {

        SelectCondition::And(children) => {
            for child in children {
                if !row_matches_condition_with_result(
                    provider,
                    Some(child),
                    subquery_values,
                    subquery_exists,
                    subquery_scalar,
                )? {
                    return Ok(false);
                }
            }

            Ok(true)
        },

        SelectCondition::Or(children) => {
            for child in children {
                if row_matches_condition_with_result(
                    provider,
                    Some(child),
                    subquery_values,
                    subquery_exists,
                    subquery_scalar,
                )? {
                    return Ok(true);
                }
            }

            Ok(false)
        },

        SelectCondition::Not(child) => row_matches_condition_with_result(
                provider,
                Some(child),
                subquery_values,
                subquery_exists,
                subquery_scalar,
            )
            .map(|matched| !matched),

        SelectCondition::Predicate(predicate) => Ok(match predicate {

            SelectPredicate::Comparison {
                field_name,
                op,
                value,
            } => match provider.value(field_name) {
                Some(actual) => compare_row_value(actual, value, op),
                None => false,
            },

            SelectPredicate::Like {
                field_name,
                pattern,
                negated,
                case_insensitive,
                escape_char,
            } => {
                let found = provider
                    .value(field_name)
                    .map(|actual| {
                        compare_like_value(actual, pattern, *case_insensitive, *escape_char)
                    })
                    .unwrap_or(false);

                if *negated {
                    !found
                } else {
                    found
                }
            },

            SelectPredicate::Regex {
                field_name,
                pattern,
                negated,
                case_insensitive,
            } => {
                let found = provider
                    .value(field_name)
                    .map(|actual| compare_regex_value(actual, pattern, *case_insensitive))
                    .unwrap_or(false);

                if *negated {
                    !found
                } else {
                    found
                }
            },

            SelectPredicate::FieldComparison {
                left_field_name,
                op,
                right_field_name,
            } => compare_provider_fields(provider, left_field_name, right_field_name, op),

            SelectPredicate::InList {
                field_name,
                values,
                negated,
            } => {
                let found = provider
                    .value(field_name)
                    .map(|actual| {
                        values
                            .iter()
                            .any(|candidate| compare_row_value(actual, candidate, &SelectComparisonOp::Eq))
                    })
                    .unwrap_or(false);
                if *negated {
                    !found
                } else {
                    found
                }
            },

            SelectPredicate::IsNull { field_name, negated } => {
                let is_null = provider.value(field_name).is_none();
                if *negated {
                    !is_null
                } else {
                    is_null
                }
            },

            SelectPredicate::InSubquery {
                field_name,
                subquery,
                negated,
            } => {
                let Some(actual) = provider.value(field_name) else {
                    return Ok(false);
                };

                let values = subquery_values(provider, subquery)?;
                let found = values
                    .iter()
                    .any(|candidate| compare_row_value(actual, candidate, &SelectComparisonOp::Eq));

                if *negated {
                    !found
                } else {
                    found
                }
            },

            SelectPredicate::AnySubqueryComparison {
                field_name,
                op,
                subquery,
            } => {
                let Some(actual) = provider.value(field_name) else {
                    return Ok(false);
                };

                let values = subquery_values(provider, subquery)?;
                values.iter().any(|candidate| compare_row_value(actual, candidate, op))
            },

            SelectPredicate::AllSubqueryComparison {
                field_name,
                op,
                subquery,
            } => {
                let Some(actual) = provider.value(field_name) else {
                    return Ok(false);
                };

                let values = subquery_values(provider, subquery)?;
                values.iter().all(|candidate| compare_row_value(actual, candidate, op))
            },

            SelectPredicate::Exists { subquery, negated } => {
                let found = subquery_exists(provider, subquery)?;

                if *negated {
                    !found
                } else {
                    found
                }
            },

            SelectPredicate::ScalarSubqueryComparison {
                field_name,
                op,
                subquery,
            } => {
                let Some(actual) = provider.value(field_name) else {
                    return Ok(false);
                };

                let Some(subquery_value) = subquery_scalar(provider, subquery)? else {
                    return Ok(false);
                };

                compare_row_value(actual, &subquery_value, op)
            },

        }),

    }

}

pub fn relation_qualifier(relation: &SelectRelation) -> &str {
    relation.alias.as_deref().unwrap_or(&relation.table_id)
}

pub fn join_condition_matches_provider(
    provider: &impl ConditionValueProvider,
    condition: &SelectCondition,
) -> bool {
    
    match condition {

        SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name,
            op,
            right_field_name,
        }) => compare_provider_fields(provider, left_field_name, right_field_name, op),
        
        _ => false,

    }

}

pub fn join_condition_field_names(join: &SelectJoin) -> Option<(&str, &str)> {

    match &join.on_condition {
        
        SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name,
            op: SelectComparisonOp::Eq,
            right_field_name,
        }) => Some((left_field_name.as_str(), right_field_name.as_str())),

        _ => None,

    }

}

pub fn compare_provider_fields(
    provider: &dyn ConditionValueProvider,
    left_field_name: &str,
    right_field_name: &str,
    op: &SelectComparisonOp,
) -> bool
{
    let Some(left_value) = provider.value(left_field_name) else {
        return false;
    };

    let Some(right_value) = provider.value(right_field_name) else {
        return false;
    };

    compare_row_value(left_value, right_value, op)
}


#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;

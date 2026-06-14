use std::collections::HashMap;

use crate::{
    SelectComparisonOp, SelectCondition, SelectJoin, SelectPredicate, SelectReadPlan,
    SelectRelation,
};

#[derive(Debug, Clone)]
pub struct MaterializedRelationRow {
    pub row_id: u64,
    pub row_map: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct JoinedRowMember {
    pub qualifier: String,
    pub row_id: Option<u64>,
    pub row_map: Option<HashMap<String, Vec<u8>>>,
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

impl ConditionValueProvider for HashMap<String, Vec<u8>> {
    fn value(&self, field_name: &str) -> Option<&Vec<u8>> {
        self.get(field_name)
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
            row_map: Some(row.row_map.clone()),
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
            row_map: member.row_map.as_ref()?.clone(),
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

pub fn row_matches_condition_with<F>(
    provider: &impl ConditionValueProvider,
    condition: Option<&SelectCondition>,
    subquery_contains: &mut F,
) -> bool
where
    F: FnMut(&[u8], &SelectReadPlan) -> bool,
{
    let Some(condition) = condition else {
        return true;
    };

    match condition {
        SelectCondition::And(children) => children
            .iter()
            .all(|child| row_matches_condition_with(provider, Some(child), subquery_contains)),

        SelectCondition::Or(children) => children
            .iter()
            .any(|child| row_matches_condition_with(provider, Some(child), subquery_contains)),

        SelectCondition::Not(child) => {
            !row_matches_condition_with(provider, Some(child), subquery_contains)
        }

        SelectCondition::Predicate(predicate) => match predicate {
            SelectPredicate::Comparison {
                field_name,
                op,
                value,
            } => match provider.value(field_name) {
                Some(actual) => compare_row_value(actual, value, op),
                None => false,
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
                    .map(|actual| values.iter().any(|candidate| candidate == actual))
                    .unwrap_or(false);
                if *negated {
                    !found
                } else {
                    found
                }
            }

            SelectPredicate::IsNull { field_name, negated } => {
                let is_null = provider.value(field_name).is_none();
                if *negated {
                    !is_null
                } else {
                    is_null
                }
            }

            SelectPredicate::InSubquery {
                field_name,
                subquery,
                negated,
            } => {
                let Some(actual) = provider.value(field_name) else {
                    return false;
                };

                let found = subquery_contains(actual, subquery);

                if *negated {
                    !found
                } else {
                    found
                }
            }
        },
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
    provider: &impl ConditionValueProvider,
    left_field_name: &str,
    right_field_name: &str,
    op: &SelectComparisonOp,
) -> bool {
    let Some(left_value) = provider.value(left_field_name) else {
        return false;
    };

    let Some(right_value) = provider.value(right_field_name) else {
        return false;
    };

    compare_row_value(left_value, right_value, op)
}

pub fn compare_row_value(actual: &[u8], expected: &[u8], op: &SelectComparisonOp) -> bool {
    let ordering = compare_scalar_bytes(actual, expected);

    match op {
        SelectComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        SelectComparisonOp::NotEq => ordering != std::cmp::Ordering::Equal,
        SelectComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        SelectComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
        SelectComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        SelectComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
    }
}

fn compare_scalar_bytes(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let left_text = String::from_utf8_lossy(left);
    let right_text = String::from_utf8_lossy(right);

    match (left_text.parse::<i128>(), right_text.parse::<i128>()) {
        (Ok(lhs), Ok(rhs)) => lhs.cmp(&rhs),
        _ => left_text.cmp(&right_text),
    }
}


#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;

use super::*;
use std::sync::Arc;
use crate::{SelectComparisonOp, SelectCondition, SelectJoin, SelectJoinKind, SelectPredicate};

fn relation(table_id: &str, alias: &str) -> SelectRelation {
    SelectRelation {
        table_id: table_id.to_string(),
        alias: Some(alias.to_string()),
    }
}

#[test]
fn joined_tuple_field_comparison_matches() {
    let left_relation = relation("users", "u");
    let right_relation = relation("profiles", "p");

    let mut left_map = HashMap::new();
    left_map.insert("id".to_string(), b"1".to_vec());
    let left_tuple = JoinedRowTuple::from_relation_row(
        &left_relation,
        MaterializedRelationRow {
            row_id: 1,
            row_map: Arc::new(left_map),
        },
    );

    let mut right_map = HashMap::new();
    right_map.insert("user_id".to_string(), b"1".to_vec());
    let provider = JoinedRowCandidateProvider {
        left: &left_tuple,
        right_relation: &right_relation,
        right_row: &MaterializedRelationRow {
            row_id: 2,
            row_map: Arc::new(right_map),
        },
    };

    let condition = SelectCondition::Predicate(SelectPredicate::FieldComparison {
        left_field_name: "u.id".to_string(),
        op: SelectComparisonOp::Eq,
        right_field_name: "p.user_id".to_string(),
    });

    assert!(join_condition_matches_provider(&provider, &condition));
}

#[test]
fn compare_row_value_supports_numeric_and_text_ordering() {
    assert!(compare_row_value(b"10", b"2", &SelectComparisonOp::Gt));
    assert!(compare_row_value(
        b"alpha",
        b"beta",
        &SelectComparisonOp::Lt
    ));
    assert!(compare_row_value(b"sam", b"sam", &SelectComparisonOp::Eq));
    assert!(compare_row_value(
        b"sam",
        b"alex",
        &SelectComparisonOp::NotEq
    ));
}

#[test]
fn compare_provider_fields_reads_from_tuple_and_candidate_provider() {
    let left_relation = relation("users", "u");
    let right_relation = relation("profiles", "p");

    let mut left_map = HashMap::new();
    left_map.insert("id".to_string(), b"1".to_vec());
    let left_tuple = JoinedRowTuple::from_relation_row(
        &left_relation,
        MaterializedRelationRow {
            row_id: 1,
            row_map: Arc::new(left_map),
        },
    );

    let mut right_map = HashMap::new();
    right_map.insert("user_id".to_string(), b"1".to_vec());
    let provider = JoinedRowCandidateProvider {
        left: &left_tuple,
        right_relation: &right_relation,
        right_row: &MaterializedRelationRow {
            row_id: 2,
            row_map: Arc::new(right_map),
        },
    };

    assert!(compare_provider_fields(
        &provider,
        "u.id",
        "p.user_id",
        &SelectComparisonOp::Eq,
    ));
    assert!(!compare_provider_fields(
        &provider,
        "u.id",
        "p.user_id",
        &SelectComparisonOp::NotEq,
    ));
}

#[test]
fn joined_row_tuple_resolves_qualified_field_names_without_linear_scan() {
    let relation = relation("users", "u");

    let mut row_map = HashMap::new();
    row_map.insert("id".to_string(), b"1".to_vec());
    let tuple = JoinedRowTuple::from_relation_row(
        &relation,
        MaterializedRelationRow {
            row_id: 1,
            row_map: Arc::new(row_map),
        },
    );

    assert_eq!(tuple.value("u.id"), Some(&b"1".to_vec()));
}

#[test]
fn joined_row_tuple_value_lookup_benchmark_style() {
    let mut tuple = JoinedRowTuple::from_relation_row(
        &relation("users", "u"),
        MaterializedRelationRow {
            row_id: 1,
            row_map: Arc::new(HashMap::new()),
        },
    );

    for index in 0..4096 {
        let relation = relation(&format!("tbl{index}"), &format!("r{index}"));
        let mut row_map = HashMap::new();
        row_map.insert("id".to_string(), index.to_string().into_bytes());
        tuple = tuple.append(
            &relation,
            &MaterializedRelationRow {
                row_id: index as u64 + 10,
                row_map: Arc::new(row_map),
            },
        );
    }

    let field_name = "r2048.id";

    let start = std::time::Instant::now();
    for _ in 0..200_000 {
        let _ = tuple.value(field_name);
    }
    let cached_elapsed = start.elapsed();

    let start = std::time::Instant::now();
    for _ in 0..200_000 {
        let (qualifier, column_name) = field_name.split_once('.').unwrap();
        let _ = tuple
            .members
            .iter()
            .find(|member| member.qualifier == qualifier)
            .and_then(|member| member.row_map.as_ref())
            .and_then(|row_map| row_map.get(column_name));
    }
    let linear_elapsed = start.elapsed();

    println!(
        "joined_row_tuple_value_lookup_benchmark_style cached={cached_elapsed:?} linear={linear_elapsed:?}"
    );

    let speedup = linear_elapsed.as_nanos() as f64 / cached_elapsed.as_nanos().max(1) as f64;
    println!("joined_row_tuple_value_lookup_benchmark_style speedup={speedup:.2}x");

    assert!(cached_elapsed < linear_elapsed, "expected cached lookup to be faster than linear scan");
}

#[test]
fn joined_row_tuple_missing_relation_helpers_preserve_available_rows() {
    
    let relation = relation("users", "u");
    let missing = JoinedRowTuple::from_missing_relations(std::slice::from_ref(&relation));
    
    assert!(missing.first_relation_row().is_none());
    assert!(missing.value("u.id").is_none());

    let mut row_map = HashMap::new();
    row_map.insert("id".to_string(), b"1".to_vec());
    let tuple = JoinedRowTuple::from_relation_row(
        &relation,
        MaterializedRelationRow {
            row_id: 1,
            row_map: Arc::new(row_map),
        },
    );

    assert_eq!(tuple.first_relation_row().map(|row| row.row_id), Some(1));
    assert_eq!(tuple.value("u.id"), Some(&b"1".to_vec()));
}

#[test]
fn join_condition_field_names_extracts_eq_operands() {
    let join = SelectJoin {
        kind: SelectJoinKind::Inner,
        relation: relation("profiles", "p"),
        on_condition: SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name: "u.id".to_string(),
            op: SelectComparisonOp::Eq,
            right_field_name: "p.user_id".to_string(),
        }),
    };

    assert_eq!(
        join_condition_field_names(&join),
        Some(("u.id", "p.user_id"))
    );
}

use sqlparser::ast::{
    Action, Delete, FromTable, Function, GrantObjects, NamedWindowDefinition, ObjectName,
    Privileges, Query, SetExpr, Statement, TableFactor, TableWithJoins,
};

use crate::engine::security::AccountPrivilege;
use crate::{FieldDef, FieldType};
use super::SelectExpression;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCompatibilityTarget {
    Mysql80,
}

pub const DEFAULT_SQL_COMPATIBILITY_TARGET: SqlCompatibilityTarget = SqlCompatibilityTarget::Mysql80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDirective {
    Create,
    Retrieve,
    Union,
    Update,
    Delete,
    AlterSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOperation {
    Select,
    UnionQuery,
    Insert,
    Update,
    Delete,
    TruncateTable,
    CreateDatabase,
    CreateTable,
    CreateView,
    CreateOlapView,
    CreateTrigger,
    CreateStoredProcedure,
    CallStoredProcedure,
    CreateOther,
    DropDatabase,
    DropTable,
    DropView,
    DropTrigger,
    DropStoredProcedure,
    DropOther,
    AlterTable,
    AlterView,
    AlterOther,
    ShowSlices,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRequest {
    pub database_id: String,
    pub sql: String,
    pub parsed_statement: Option<Statement>,
    pub parsed_insert_plan: Option<InsertRowsPlan>,
    pub directive: SqlDirective,
    pub operation: SqlOperation,
    pub object_name: Option<String>,
    pub required_privilege: Option<AccountPrivilege>,
    pub compatibility_target: SqlCompatibilityTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclMutationKind {
    Grant,
    Revoke,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclMutationPlan {
    pub kind: AclMutationKind,
    pub grantee: String,
    pub privilege: AccountPrivilege,
    pub database_name: Option<String>,
    pub object_name: Option<String>,
    pub with_grant_option: bool,
}

impl SqlRequest {

    pub fn referenced_object_names(&self) -> Vec<String> {

        let mut collected = Vec::<String>::new();
        let mut seen = HashSet::<String>::new();

        let mut push_object = |name: String| {
            if !name.is_empty() && seen.insert(name.clone()) {
                collected.push(name);
            }
        };

        if let Some(statement) = &self.parsed_statement {

            match statement {

                Statement::Query(query) => {
                    collect_set_expr_objects(&query.body, &mut push_object);
                },

                Statement::Insert(insert) => {
                    push_object(insert.table_name.to_string());
                },

                Statement::Update { table, .. } => {
                    collect_table_with_joins_objects(table, &mut push_object);
                },

                Statement::Delete(delete) => {
                    collect_delete_objects(delete, &mut push_object);
                },

                Statement::CreateTable(create_table) => {
                    push_object(create_table.name.to_string());
                },

                Statement::CreateView { name, .. } => {
                    push_object(name.to_string());
                },

                Statement::Drop {
                    names, ..
                } => {
                    for name in names {
                        push_object(name.to_string());
                    }
                },

                Statement::AlterTable { name, .. } => {
                    push_object(name.to_string());
                },

                Statement::AlterView { name, .. } => {
                    push_object(name.to_string());
                },

                Statement::Truncate { table_names, .. } => {
                    for target in table_names {
                        push_object(target.name.to_string());
                    }
                },

                Statement::ShowCreate { obj_name, .. } => {
                    push_object(obj_name.to_string());
                },

                Statement::ShowColumns { table_name, .. } => {
                    push_object(table_name.to_string());
                },

                Statement::ExplainTable { table_name, .. } => {
                    push_object(table_name.to_string());
                },

                _ => {
                    if let Some(name) = self.object_name.as_ref() {
                        push_object(name.clone());
                    }
                }

            }

        } else if let Some(name) = self.object_name.as_ref() {
            push_object(name.clone());
        }

        collected

    }

    pub fn acl_mutation_plans(&self) -> Vec<AclMutationPlan> {

        let Some(statement) = &self.parsed_statement else {
            return Vec::new();
        };

        match statement {

            Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                ..
            } => build_acl_mutation_plans(
                AclMutationKind::Grant,
                privileges,
                objects,
                grantees,
                *with_grant_option,
            ),

            Statement::Revoke {
                privileges,
                objects,
                grantees,
                ..
            } => build_acl_mutation_plans(
                AclMutationKind::Revoke,
                privileges,
                objects,
                grantees,
                false,
            ),

            _ => Vec::new(),

        }

    }

}

fn build_acl_mutation_plans(
    kind: AclMutationKind,
    privileges: &Privileges,
    objects: &GrantObjects,
    grantees: &[sqlparser::ast::Ident],
    with_grant_option: bool,
) -> Vec<AclMutationPlan> {

    let resolved_privileges = privileges_from_ast(privileges);
    if resolved_privileges.is_empty() {
        return Vec::new();
    }

    let resolved_grantees = grantees
        .iter()
        .map(|ident| ident.value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();

    if resolved_grantees.is_empty() {
        return Vec::new();
    }

    let resolved_targets = mutation_targets_from_grant_objects(objects);

    let mut plans = Vec::new();

    if resolved_targets.is_empty() {
        
        for grantee in &resolved_grantees {

            for privilege in &resolved_privileges {

                plans.push(AclMutationPlan {
                    kind: kind.clone(),
                    grantee: grantee.clone(),
                    privilege: *privilege,
                    database_name: None,
                    object_name: None,
                    with_grant_option,
                });

            }

        }
        
        return plans;

    }

    for grantee in &resolved_grantees {

        for privilege in &resolved_privileges {

            for (database_name, object_name) in &resolved_targets {

                plans.push(AclMutationPlan {
                    kind: kind.clone(),
                    grantee: grantee.clone(),
                    privilege: *privilege,
                    database_name: database_name.clone(),
                    object_name: object_name.clone(),
                    with_grant_option,
                });

            }

        }

    }

    plans

}

fn privileges_from_ast(privileges: &Privileges) -> Vec<AccountPrivilege> {

    match privileges {

        Privileges::All {
            ..
        } => AccountPrivilege::all().to_vec(),

        Privileges::Actions(actions) => actions
            .iter()
            .filter_map(account_privilege_from_action)
            .collect(),

    }

}

fn account_privilege_from_action(action: &Action) -> Option<AccountPrivilege> {

    match action {

        Action::Connect         => None,
        
        Action::Create          => Some(AccountPrivilege::Create),
        
        Action::Delete          => Some(AccountPrivilege::Delete),
        
        Action::Execute         => Some(AccountPrivilege::Execute),
        
        Action::Insert {
            ..
        }                       => Some(AccountPrivilege::Insert),
        
        Action::References {
            ..
        }                       => Some(AccountPrivilege::References),
        
        Action::Select {
            ..
        }                       => Some(AccountPrivilege::Select),
        
        Action::Temporary       => Some(AccountPrivilege::CreateTemporaryTables),
        
        Action::Trigger         => Some(AccountPrivilege::Trigger),
        
        Action::Truncate        => Some(AccountPrivilege::Delete),
        
        Action::Update {
            ..
        }                       => Some(AccountPrivilege::Update),
        
        Action::Usage           => None,

    }

}

fn mutation_targets_from_grant_objects(
    objects: &GrantObjects,
) -> Vec<(Option<String>, Option<String>)> {

    match objects {

        GrantObjects::Schemas(schemas) |
        GrantObjects::AllSequencesInSchema { schemas, } |
        GrantObjects::AllTablesInSchema { schemas, } => schemas
            .iter()
            .filter_map(|name| {
                let database_name = object_name_leaf(name)?;
                Some((Some(database_name), None))
            })
            .collect(),

        GrantObjects::Tables(names) |
        GrantObjects::Sequences(names) => names
            .iter()
            .filter_map(|name| {
                let (database_name, object_name) = split_database_and_object_name(name);
                object_name.map(|value| (database_name, Some(value)))
            })
            .collect(),

    }

}

fn split_database_and_object_name(name: &ObjectName) -> (Option<String>, Option<String>) {

    let normalized_parts = name
        .0
        .iter()
        .filter_map(normalize_ident)
        .collect::<Vec<_>>();

    if normalized_parts.is_empty() {
        return (None, None);
    }

    if normalized_parts.len() == 1 {
        return (None, normalized_parts.first().cloned());
    }

    let object_name = normalized_parts.last().cloned();
    let database_name = normalized_parts
        .get(normalized_parts.len().saturating_sub(2))
        .cloned();

    (database_name, object_name)

}

fn object_name_leaf(name: &ObjectName) -> Option<String> {
    name.0.last().and_then(normalize_ident)
}

fn normalize_ident(ident: &sqlparser::ast::Ident) -> Option<String> {
    
    let normalized = ident
        .value
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }

}

fn collect_set_expr_objects(
    set_expr: &SetExpr,
    push_object: &mut impl FnMut(String),
) {

    match set_expr {

        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_table_with_joins_objects(table_with_joins, push_object);
            }
        },

        SetExpr::Query(query) => collect_query_objects(query, push_object),

        SetExpr::SetOperation {
            left,
            right,
            ..
        } => {
            collect_set_expr_objects(left, push_object);
            collect_set_expr_objects(right, push_object);
        },

        _ => {}

    }

}

fn collect_query_objects(
    query: &Query,
    push_object: &mut impl FnMut(String),
) {

    collect_set_expr_objects(&query.body, push_object);

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_query_objects(&cte.query, push_object);
        }
    }

}

fn collect_table_with_joins_objects(
    table: &TableWithJoins,
    push_object: &mut impl FnMut(String),
) {

    collect_table_factor_objects(&table.relation, push_object);

    for join in &table.joins {
        collect_table_factor_objects(&join.relation, push_object);
    }

}

fn collect_table_factor_objects(
    factor: &TableFactor,
    push_object: &mut impl FnMut(String),
) {

    match factor {

        TableFactor::Table { name, .. } => {
            collect_object_name(name, push_object);
        },

        TableFactor::Derived { subquery, .. } => {
            collect_query_objects(subquery, push_object);
        },

        TableFactor::NestedJoin {
            table_with_joins,
            ..
        } => {
            collect_table_with_joins_objects(table_with_joins, push_object);
        },

        _ => {}

    }

}

fn collect_delete_objects(
    delete: &Delete,
    push_object: &mut impl FnMut(String),
) {

    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            for table in tables {
                collect_table_with_joins_objects(table, push_object);
            }
        }
    }

}

fn collect_object_name(
    name: &ObjectName,
    push_object: &mut impl FnMut(String),
) {

    let raw = name.to_string();
    if !raw.is_empty() {
        push_object(raw);
    }

}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableChangePlan {
    pub table_id: String,
    pub operations: Vec<AlterTableChangeOp>,
}

#[expect(clippy::large_enum_variant, reason="the enum variants are large but necessary for the expression representation")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertRowsSource {
    Values(Vec<Vec<Option<Vec<u8>>>>),
    Select(SelectReadPlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertRowsPlan {
    pub table_id: String,
    pub ignore: bool,
    pub replace_into: bool,
    pub columns: Vec<String>,
    pub source: InsertRowsSource,
    pub on_duplicate_update: Vec<InsertOnDuplicateAssignment>,
    pub returning: Option<MutationReturningPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertOnDuplicateAssignment {
    pub field_name: String,
    pub value: InsertOnDuplicateAssignmentValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOnDuplicateAssignmentValue {
    Literal(Option<Vec<u8>>),
    FunctionExpression(String),
    IncomingColumn(String),
    ExistingColumn(String),
    Arithmetic {
        left: InsertOnDuplicateAssignmentOperand,
        op: InsertOnDuplicateArithmeticOp,
        right: InsertOnDuplicateAssignmentOperand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOnDuplicateAssignmentOperand {
    Literal(Option<Vec<u8>>),
    FunctionExpression(String),
    IncomingColumn(String),
    ExistingColumn(String),
    Unary {
        op: UnaryArithmeticOp,
        operand: Box<InsertOnDuplicateAssignmentOperand>,
    },
    Arithmetic {
        left: Box<InsertOnDuplicateAssignmentOperand>,
        op: InsertOnDuplicateArithmeticOp,
        right: Box<InsertOnDuplicateAssignmentOperand>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryArithmeticOp {
    Plus,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOnDuplicateArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationReturningItem {
    Wildcard,
    Column {
        field_name: String,
        output_name: String,
    },
}

pub type MutationReturningPlan = Vec<MutationReturningItem>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAssignment {
    pub field_name: String,
    pub value: UpdateAssignmentValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateAssignmentValue {
    Literal(Option<Vec<u8>>),
    FunctionExpression(String),
    ExistingColumn(String),
    Arithmetic {
        left: UpdateAssignmentOperand,
        op: UpdateArithmeticOp,
        right: UpdateAssignmentOperand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateAssignmentOperand {
    Literal(Option<Vec<u8>>),
    FunctionExpression(String),
    ExistingColumn(String),
    Unary {
        op: UnaryArithmeticOp,
        operand: Box<UpdateAssignmentOperand>,
    },
    Arithmetic {
        left: Box<UpdateAssignmentOperand>,
        op: UpdateArithmeticOp,
        right: Box<UpdateAssignmentOperand>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRowsPlan {
    pub table_id: String,
    pub relations: Vec<SelectRelation>,
    pub joins: Vec<SelectJoin>,
    pub pushdown_conditions: Vec<Option<SelectCondition>>,
    pub order_by: Vec<SelectOrderByItem>,
    pub limit: Option<usize>,
    pub assignments: Vec<UpdateAssignment>,
    pub where_condition: Option<SelectCondition>,
    pub returning: Option<MutationReturningPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRowsPlan {
    pub table_id: String,
    pub relations: Vec<SelectRelation>,
    pub joins: Vec<SelectJoin>,
    pub pushdown_conditions: Vec<Option<SelectCondition>>,
    pub order_by: Vec<SelectOrderByItem>,
    pub limit: Option<usize>,
    pub where_condition: Option<SelectCondition>,
    pub returning: Option<MutationReturningPlan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEventKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerInvocationBinding {
    pub table_id: String,
    pub timing: TriggerTiming,
    pub event: TriggerEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IfElseEndBranchPlan {
    pub condition: SelectCondition,
    pub action_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IfElseEndPlan {
    pub branches: Vec<IfElseEndBranchPlan>,
    pub else_action_sql: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineParameterMode {
    In,
    Out,
    InOut,
}

impl std::fmt::Display for RoutineParameterMode {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutineParameterMode::In => write!(f, "IN"),
            RoutineParameterMode::Out => write!(f, "OUT"),
            RoutineParameterMode::InOut => write!(f, "INOUT"),
        }
    }
    
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineParameterDeclaration {
    pub name: String,
    pub mode: RoutineParameterMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineArgumentBinding {
    pub name: String,
    pub mode: RoutineParameterMode,
    pub value: Vec<u8>,
    pub output_target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectRelation {
    pub table_id: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectJoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl std::fmt::Display for SelectJoinKind {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectJoinKind::Inner => write!(f, "inner"),
            SelectJoinKind::Left => write!(f, "left"),
            SelectJoinKind::Right => write!(f, "right"),
            SelectJoinKind::Full => write!(f, "full"),
            SelectJoinKind::Cross => write!(f, "cross"),
        }
    }

}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectJoin {
    pub kind: SelectJoinKind,
    pub relation: SelectRelation,
    pub on_condition: SelectCondition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectOrderByItem {
    pub field_name: String,
    pub descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectSetBoundaryOp {
    UnionAll,
    UnionDistinct,
    ExceptDistinct,
    IntersectDistinct,
}

#[expect(clippy::large_enum_variant, reason="step lists are short and branch plans are consumed in parser/runtime pipelines where enum size is acceptable")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectSetQueryStep {
    Branch(SelectReadPlan),
    BoundaryOperation(SelectSetBoundaryOp),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectComparisonOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectPredicate {
    Comparison {
        field_name: String,
        op: SelectComparisonOp,
        value: Vec<u8>,
    },
    Like {
        field_name: String,
        pattern: Vec<u8>,
        negated: bool,
        case_insensitive: bool,
        escape_char: Option<char>,
    },
    Regex {
        field_name: String,
        pattern: Vec<u8>,
        negated: bool,
        case_insensitive: bool,
    },
    FieldComparison {
        left_field_name: String,
        op: SelectComparisonOp,
        right_field_name: String,
    },
    InList {
        field_name: String,
        values: Vec<Vec<u8>>,
        negated: bool,
    },
    IsNull {
        field_name: String,
        negated: bool,
    },
    InSubquery {
        field_name: String,
        subquery: Box<SelectReadPlan>,
        negated: bool,
    },
    ScalarSubqueryComparison {
        field_name: String,
        op: SelectComparisonOp,
        subquery: Box<SelectReadPlan>,
    },
    AnySubqueryComparison {
        field_name: String,
        op: SelectComparisonOp,
        subquery: Box<SelectReadPlan>,
    },
    AllSubqueryComparison {
        field_name: String,
        op: SelectComparisonOp,
        subquery: Box<SelectReadPlan>,
    },
    Exists {
        subquery: Box<SelectReadPlan>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectCondition {
    And(Vec<SelectCondition>),
    Or(Vec<SelectCondition>),
    Not(Box<SelectCondition>),
    Predicate(SelectPredicate),
}

#[expect(clippy::large_enum_variant, reason="the variants are sufficiently distinct in their usage and the enum is not expected to be used in performance-critical code paths where the size difference would be a concern")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectCaseWhen {
    Condition(SelectCondition),
    Equals(SelectExpression),
}

#[expect(clippy::large_enum_variant, reason="the variants are sufficiently distinct in their usage and the enum is not expected to be used in performance-critical code paths where the size difference would be a concern")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjectionItem {
    Column {
        field_name: String,
        output_name: String,
    },
    Case {
        output_name: String,
        operand: Option<SelectExpression>,
        branches: Vec<(SelectCaseWhen, SelectExpression)>,
        else_value: Option<SelectExpression>,
    },
    Wildcard {
        relation: Option<String>,
    },
    InbuiltFunction {
        output_name: String,
        function: Function,
    },
    WindowFunction {
        output_name: String,
        function: Function,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectReadPlan {
    // Empty when SELECT omits FROM and contains only inbuilt projection functions.
    pub table_id: String,
    pub ctes: Vec<SelectCtePlan>,
    pub relations: Vec<SelectRelation>,
    pub joins: Vec<SelectJoin>,
    pub pushdown_conditions: Vec<Option<SelectCondition>>,
    pub named_windows: Vec<NamedWindowDefinition>,
    pub projection: Option<Vec<String>>,
    pub projection_items: Vec<SelectProjectionItem>,
    pub projection_is_wildcard: bool,
    pub distinct: bool,
    pub order_by: Vec<SelectOrderByItem>,
    pub group_by: Vec<String>,
    pub having_condition: Option<SelectCondition>,
    pub has_window_clause: bool,
    pub limit_by: Option<SelectLimitByPlan>,
    pub top_percent: Option<usize>,
    pub top_percent_with_ties: Option<usize>,
    pub top_with_ties_limit: Option<usize>,
    pub fetch_percent: Option<usize>,
    pub fetch_percent_with_ties: Option<usize>,
    pub fetch_with_ties_limit: Option<usize>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub where_condition: Option<SelectCondition>,
    pub qualify_condition: Option<SelectCondition>,
    pub lock_mode: SelectLockMode,
    pub is_explain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectLimitByPlan {
    pub per_key_limit: usize,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectLockMode {
    None,
    ForUpdate,
    ForShare,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectCtePlan {
    pub table_id: String,
    pub read_plan: Box<SelectReadPlan>,
    pub recursive_read_plan: Option<Box<SelectReadPlan>>,
    pub recursive_union_all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableChangeOp {
    AddField(FieldDef),
    DropField(String),
    RenameField {
        from: String,
        to: String,
    },
    ModifyField {
        field_name: String,
        new_type: FieldType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlParseError {
    EmptyStatement,
    MissingIdentifier { keyword: &'static str, statement: String },
    UnsupportedStatement(String),
}

impl std::fmt::Display for SqlParseError {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        
        match self {
            
            Self::EmptyStatement => write!(f, "sql statement is empty"),

            Self::MissingIdentifier { keyword, statement } => {
                write!(f, "sql statement '{statement}' is missing an identifier after '{keyword}'")
            }

            Self::UnsupportedStatement(statement) => {
                write!(f, "unsupported sql statement '{statement}'")
            }
            
        }

    }

}

impl std::error::Error for SqlParseError {}

pub(super) enum ParsedOrFallback {
    Parsed(Vec<Statement>),
    Fallback {
        trimmed_sql: String,
        metadata: (SqlDirective, SqlOperation, Option<String>, Option<AccountPrivilege>),
    },
}

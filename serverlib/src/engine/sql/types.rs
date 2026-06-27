use sqlparser::ast::{Function, Statement};

use crate::{FieldDef, FieldType};
use super::SelectExpression;

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
    CreateTrigger,
    CreateStoredProcedure,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRequest {
    pub database_id: String,
    pub sql: String,
    pub directive: SqlDirective,
    pub operation: SqlOperation,
    pub object_name: Option<String>,
    pub compatibility_target: SqlCompatibilityTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableChangePlan {
    pub table_id: String,
    pub operations: Vec<AlterTableChangeOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertRowsSource {
    Values(Vec<Vec<Option<Vec<u8>>>>),
    Select(SelectReadPlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertRowsPlan {
    pub table_id: String,
    pub columns: Vec<String>,
    pub source: InsertRowsSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAssignment {
    pub field_name: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateRowsPlan {
    pub table_id: String,
    pub relations: Vec<SelectRelation>,
    pub joins: Vec<SelectJoin>,
    pub pushdown_conditions: Vec<Option<SelectCondition>>,
    pub assignments: Vec<UpdateAssignment>,
    pub where_condition: Option<SelectCondition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteRowsPlan {
    pub table_id: String,
    pub relations: Vec<SelectRelation>,
    pub joins: Vec<SelectJoin>,
    pub pushdown_conditions: Vec<Option<SelectCondition>>,
    pub where_condition: Option<SelectCondition>,
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
    Gte,
    Lt,
    Lte,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectReadPlan {
    // Empty when SELECT omits FROM and contains only inbuilt projection functions.
    pub table_id: String,
    pub ctes: Vec<SelectCtePlan>,
    pub relations: Vec<SelectRelation>,
    pub joins: Vec<SelectJoin>,
    pub pushdown_conditions: Vec<Option<SelectCondition>>,
    pub projection: Option<Vec<String>>,
    pub projection_items: Vec<SelectProjectionItem>,
    pub projection_is_wildcard: bool,
    pub distinct: bool,
    pub order_by: Vec<SelectOrderByItem>,
    pub group_by: Vec<String>,
    pub having_condition: Option<SelectCondition>,
    pub has_window_clause: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub where_condition: Option<SelectCondition>,
    pub is_explain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectCtePlan {
    pub table_id: String,
    pub read_plan: Box<SelectReadPlan>,
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
        metadata: (SqlDirective, SqlOperation, Option<String>),
    },
}

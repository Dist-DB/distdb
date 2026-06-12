use sqlparser::ast::{Function, Statement};

use crate::{FieldDef, FieldType};

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
pub struct InsertRowsPlan {
    pub table_id: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectCondition {
    And(Vec<SelectCondition>),
    Or(Vec<SelectCondition>),
    Not(Box<SelectCondition>),
    Predicate(SelectPredicate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjectionItem {
    Column {
        field_name: String,
        output_name: String,
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
    pub projection: Option<Vec<String>>,
    pub projection_items: Vec<SelectProjectionItem>,
    pub projection_is_wildcard: bool,
    pub where_condition: Option<SelectCondition>,
    pub is_explain: bool,
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

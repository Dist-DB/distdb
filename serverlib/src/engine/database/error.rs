#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    InvalidDatabaseName,
    DuplicateTable,
    TableNotFound,
    InvalidStatusTransition,
    NotReadyForWrite,
    SyncPending,
    CatalogRead,
    CatalogInvalidHeader,
    CatalogPayloadMissing,
    CatalogDeserialize,
    CatalogSerialize,
    CatalogWrite,
    SchemaPayloadDeserialize,
    SchemaRevisionOutOfOrder,
    SchemaChange(super::schema_error::SchemaError),
    TableNotLocked,
    DuplicateView,
    ViewNotFound,
    ViewNotWritable,
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDatabaseName => write!(f, "database name must not be empty"),
            Self::DuplicateTable => write!(f, "table already registered in database catalog"),
            Self::TableNotFound => write!(f, "table not found in database catalog"),
            Self::InvalidStatusTransition => write!(f, "invalid database/table status transition"),
            Self::NotReadyForWrite => write!(f, "database/table is not ready for write operations"),
            Self::SyncPending => write!(f, "database/table sync has not been acknowledged yet"),
            Self::CatalogRead => write!(f, "failed to read catalog file"),
            Self::CatalogInvalidHeader => write!(f, "invalid catalog file header/version"),
            Self::CatalogPayloadMissing => write!(f, "catalog payload missing"),
            Self::CatalogDeserialize => write!(f, "failed to deserialize catalog file"),
            Self::CatalogSerialize => write!(f, "failed to serialize catalog"),
            Self::CatalogWrite => write!(f, "failed to write catalog file"),
            Self::SchemaPayloadDeserialize => {
                write!(f, "failed to deserialize schema change payload")
            }
            Self::SchemaRevisionOutOfOrder => {
                write!(f, "schema revision must advance monotonically")
            }
            Self::SchemaChange(e) => write!(f, "schema mutation error: {e}"),
            Self::TableNotLocked => write!(
                f,
                "table must be locked before a schema change can be prepared or committed"
            ),
            Self::DuplicateView => write!(f, "view already registered in database catalog"),
            Self::ViewNotFound => write!(f, "view not found in database catalog"),
            Self::ViewNotWritable => {
                write!(f, "views are read-only; write operations are not permitted")
            }
        }
    }
}

impl std::error::Error for DatabaseError {}

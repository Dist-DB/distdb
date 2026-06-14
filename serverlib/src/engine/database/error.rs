
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    InvalidDatabaseName,
    DuplicateEntity,
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
    MetadataPayloadDeserialize,
    SqlDefinitionPayloadDeserialize,
    SchemaRevisionOutOfOrder,
    SchemaChange(super::schema_error::SchemaError),
    SchemaChangeInProgress,
    TableNotLocked,
    DuplicateView,
    ViewNotFound,
    ViewNotWritable,
    DuplicateTrigger,
    TriggerNotFound,
    DuplicateStoredProcedure,
    StoredProcedureNotFound,
    EntityNotFound,
    UnsupportedSqlObjectKind,
}

impl std::fmt::Display for DatabaseError {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        match self {

            Self::InvalidDatabaseName               => write!(f, "database name must not be empty"),
            Self::DuplicateEntity                   => write!(f, "entity id already registered in database catalog"),
            Self::DuplicateTable                    => write!(f, "table already registered in database catalog"),
            Self::TableNotFound                     => write!(f, "table not found in database catalog"),
            Self::InvalidStatusTransition           => write!(f, "invalid database/table status transition"),
            Self::NotReadyForWrite                  => write!(f, "database/table is not ready for write operations"),
            Self::SyncPending                       => write!(f, "database/table sync has not been acknowledged yet"),
            Self::CatalogRead                       => write!(f, "failed to read catalog file"),
            Self::CatalogInvalidHeader              => write!(f, "invalid catalog file header/version"),
            Self::CatalogPayloadMissing             => write!(f, "catalog payload missing"),
            Self::CatalogDeserialize                => write!(f, "failed to deserialize catalog file"),
            Self::CatalogSerialize                  => write!(f, "failed to serialize catalog"),
            Self::CatalogWrite                      => write!(f, "failed to write catalog file"),
            Self::SchemaPayloadDeserialize          => write!(f, "failed to deserialize schema change payload"),            
            Self::MetadataPayloadDeserialize        => write!(f, "failed to deserialize metadata change payload"),
            Self::SqlDefinitionPayloadDeserialize   => write!(f, "failed to deserialize sql definition payload"),
            Self::SchemaRevisionOutOfOrder          => write!(f, "schema revision must advance monotonically"),
            Self::SchemaChange(e)     => write!(f, "schema mutation error: {e}"),
            Self::SchemaChangeInProgress            => write!(f, "another schema change is currently in progress"),
            Self::TableNotLocked                    => write!(f, "table must be locked before the requested write operation can proceed"),
            Self::DuplicateView                     => write!(f, "view already registered in database catalog"),
            Self::ViewNotFound                      => write!(f, "view not found in database catalog"),
            Self::ViewNotWritable                   => write!(f, "views are read-only; write operations are not permitted"),
            Self::DuplicateTrigger                  => write!(f, "trigger already registered in database catalog"),
            Self::TriggerNotFound                   => write!(f, "trigger not found in database catalog"),
            Self::DuplicateStoredProcedure          => write!(f, "stored procedure already registered in database catalog"),
            Self::StoredProcedureNotFound           => write!(f, "stored procedure not found in database catalog"),
            Self::EntityNotFound                    => write!(f, "entity not found in database catalog"),
            Self::UnsupportedSqlObjectKind          => write!(f, "sql object kind is not yet supported by this catalog"),

        }

    }

}

impl std::error::Error for DatabaseError {}

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use super::catalog::DatabaseCatalog;
use super::id::DatabaseId;

/// Catalogs of every attached database, published for the duration of a statement so that
/// schema-qualified references (`locations.fnnearesttown`) can reach outside the active database.
pub type ForeignCatalogs = Arc<HashMap<String, DatabaseCatalog>>;

thread_local! {
    static FOREIGN_CATALOG_STACK: RefCell<Vec<ForeignCatalogs>> = const { RefCell::new(Vec::new()) };
}

pub fn with_foreign_catalogs<R>(catalogs: ForeignCatalogs, callback: impl FnOnce() -> R) -> R {

    FOREIGN_CATALOG_STACK.with(|stack| {
        stack.borrow_mut().push(catalogs);
    });

    let outcome = callback();

    FOREIGN_CATALOG_STACK.with(|stack| {
        let _ = stack.borrow_mut().pop();
    });

    outcome

}

/// Catalog clones share entity handles, so the returned catalog observes live data.
pub fn resolve_foreign_catalog(database_name: &str) -> Option<DatabaseCatalog> {

    let requested = database_name.trim();

    if requested.is_empty() {
        return None;
    }

    let normalized = DatabaseId::from_database_name(requested).ok().map(|id| id.0);

    FOREIGN_CATALOG_STACK.with(|stack| {

        let stack = stack.borrow();
        let catalogs = stack.last()?;

        catalogs
            .get(requested)
            .or_else(|| normalized.as_deref().and_then(|key| catalogs.get(key)))
            .cloned()

    })

}

/// Splits `db.routine` into its parts, stripping delimiters and folding case.
pub fn split_qualified_object_name(object_name: &str) -> (Option<String>, String) {

    let cleaned = object_name
        .chars()
        .filter(|ch| *ch != '`' && *ch != '"')
        .collect::<String>()
        .to_ascii_lowercase();

    match cleaned.rsplit_once('.') {
        Some((qualifier, name)) => {
            let qualifier = qualifier.trim();
            let name = name.trim().to_string();
            if qualifier.is_empty() || name.is_empty() {
                (None, cleaned.trim().to_string())
            } else {
                (Some(qualifier.to_string()), name)
            }
        }
        None => (None, cleaned.trim().to_string()),
    }

}

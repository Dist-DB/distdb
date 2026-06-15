use std::collections::HashMap;

use serverlib::{DatabaseCatalog, DatabaseId};

pub(super) fn resolve_catalog<'a>(
    catalogs: &'a HashMap<String, DatabaseCatalog>,
    database_id: &str,
) -> Option<&'a DatabaseCatalog> {
    catalogs.get(database_id).or_else(|| {
        DatabaseId::from_database_name(database_id)
            .ok()
            .and_then(|dbid| catalogs.get(&dbid.0))
    })
}

pub(super) fn resolve_catalog_mut<'a>(
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    database_id: &str,
) -> Option<&'a mut DatabaseCatalog> {

    if catalogs.contains_key(database_id) {
        return catalogs.get_mut(database_id);
    }

    let normalized = DatabaseId::from_database_name(database_id).ok()?.0;
    catalogs.get_mut(&normalized)
    
}

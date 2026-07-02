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

pub(super) fn resolve_catalog_for_table_reference<'a>(
    catalogs: &'a HashMap<String, DatabaseCatalog>,
    active_database_id: &str,
    object_name: &str,
) -> Option<(&'a DatabaseCatalog, String)> {

    let table_id = common::normalize_identifier!(object_name);

    if !active_database_id.trim().is_empty() {
        return resolve_catalog(catalogs, active_database_id).map(|catalog| (catalog, table_id));
    }

    let (database_name, referenced_table_id) = object_name.rsplit_once('.')?;
    let referenced_table_id = common::normalize_identifier!(referenced_table_id);

    if referenced_table_id.is_empty() {
        return None;
    }

    resolve_catalog(catalogs, database_name).map(|catalog| (catalog, referenced_table_id))

}

pub(super) fn resolve_catalog_for_table_reference_mut<'a>(
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    active_database_id: &str,
    object_name: &str,
) -> Option<(&'a mut DatabaseCatalog, String)> {

    let table_id = common::normalize_identifier!(object_name);

    if !active_database_id.trim().is_empty() {
        return resolve_catalog_mut(catalogs, active_database_id).map(|catalog| (catalog, table_id));
    }

    let (database_name, referenced_table_id) = object_name.rsplit_once('.')?;
    let referenced_table_id = common::normalize_identifier!(referenced_table_id);

    if referenced_table_id.is_empty() {
        return None;
    }

    resolve_catalog_mut(catalogs, database_name).map(|catalog| (catalog, referenced_table_id))

}

use crate::core::app::ServerApp;
use common::helpers::utils::md5_hash;

pub fn resolve_schema_catalog<'a>(
    app: &'a ServerApp,
    database_id: &str,
) -> Option<&'a serverlib::DatabaseCatalog> {

    if let Some(catalog) = app.catalogs().get(database_id) {
        return Some(catalog);
    }

    if let Ok(normalized_id) = serverlib::DatabaseId::from_database_name(database_id)
        && let Some(catalog) = app.catalogs().get(&normalized_id.0) {
            return Some(catalog);
    }

    app.catalogs()
        .values()
        .find(|catalog| catalog.database_id.0 == database_id)
}

pub fn load_schema_catalog_from_disk(
    app: &ServerApp,
    database_id: &str,
) -> Option<serverlib::DatabaseCatalog> {
    let mut candidate_ids = vec![database_id.to_string()];

    if let Ok(normalized_id) = serverlib::DatabaseId::from_database_name(database_id)
        && !candidate_ids.contains(&normalized_id.0) {
            candidate_ids.push(normalized_id.0);
    }

    for candidate_id in candidate_ids {
        let catalog_path = app.node_data_dir().join(
            common::helpers::format::FileKind::Catalog.file_name(&candidate_id),
        );

        if !catalog_path.exists() {
            continue;
        }

        match serverlib::DatabaseCatalog::load_from_path(&catalog_path) {
            Ok(catalog) => return Some(catalog),
            Err(err) => {
                log::warn!(
                    "failed loading schema catalog from disk database_id={} path={} err={}",
                    database_id,
                    catalog_path.display(),
                    err
                );
            }
        }
    }

    None
}

pub fn schema_catalog_signature(catalog: &serverlib::DatabaseCatalog) -> (u64, Option<String>) {
    let mut table_ids = catalog.table_ids();
    table_ids.sort();

    let schema_identifier = catalog.schema_epoch().max(1);
    let schema_hash = md5_hash(
        format!(
            "{}:{}:{}",
            catalog.database_id.0,
            schema_identifier,
            table_ids.join(",")
        )
        .as_str(),
    );

    (schema_identifier, Some(schema_hash))
}

pub fn build_schema_definitions_for_database(
    app: &ServerApp,
    database_id: &str,
) -> Result<Vec<String>, String> {
    let catalog = resolve_schema_catalog(app, database_id)
        .cloned()
        .or_else(|| load_schema_catalog_from_disk(app, database_id))
        .ok_or_else(|| format!("database '{}' not found", database_id))?;

    let mut table_ids = catalog.table_ids();
    table_ids.sort();

    let mut statements = Vec::new();

    for table_id in table_ids {
        let Some(schema) = catalog.table_schema(&table_id) else {
            continue;
        };

        let mut fields = schema.fields.clone();
        fields.sort_by_key(|f| f.seqno);

        let mut parts = fields
            .iter()
            .map(|field| {
                field
                    .to_sql_string()
                    .replace(" BIGINT SIGNED", " BIGINT")
                    .replace(" INT SIGNED", " INT")
            })
            .collect::<Vec<_>>();

        let primary_keys = fields
            .iter()
            .filter(|field| matches!(field.indexed, common::schema::FieldIndex::PrimaryKey))
            .map(|field| field.field_name.clone())
            .collect::<Vec<_>>();

        if !primary_keys.is_empty() {
            parts.push(format!("PRIMARY KEY ({})", primary_keys.join(", ")));
        }

        statements.push(format!(
            "CREATE TABLE {} ({});",
            table_id,
            parts.join(", ")
        ));
    }

    Ok(statements)
}

pub fn apply_schema_definitions_to_local_database(
    app: &mut ServerApp,
    database_id: &str,
    schema_definitions: &[String],
) -> Result<(), String> {
    app.apply_affinity_schema_definitions(database_id, schema_definitions)
}

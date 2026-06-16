use std::collections::HashMap;

use super::table_schema::TableSchema;

type OrdinalRowPayload = Vec<Option<Vec<u8>>>;

fn field_names_by_ordinal(schema: &TableSchema) -> Vec<String> {
    
    let mut fields = schema
        .fields
        .iter()
        .map(|field| (field.seqno, field.field_name.clone()))
        .collect::<Vec<_>>();

    fields.sort_by(|(lhs_seqno, lhs_name), (rhs_seqno, rhs_name)| {
        lhs_seqno
            .cmp(rhs_seqno)
            .then_with(|| lhs_name.cmp(rhs_name))
    });

    fields.into_iter().map(|(_, field_name)| field_name).collect()

}

pub fn encode_row_payload(
    schema: &TableSchema,
    row_map: &HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>, String> {

    let ordered_field_names = field_names_by_ordinal(schema);

    if ordered_field_names.is_empty() && !row_map.is_empty() {
        return bincode::serialize(row_map).map_err(|err| err.to_string());
    }

    let payload = ordered_field_names
        .into_iter()
        .map(|field_name| row_map.get(&field_name).cloned())
        .collect::<OrdinalRowPayload>();

    bincode::serialize(&payload).map_err(|err| err.to_string())

}

pub fn decode_row_payload(
    schema: &TableSchema,
    payload: &[u8],
) -> Result<HashMap<String, Vec<u8>>, String> {
    
    let ordered_field_names = field_names_by_ordinal(schema);

    if let Ok(ordinal_row) = bincode::deserialize::<OrdinalRowPayload>(payload) {
        let mut row_map = HashMap::with_capacity(ordered_field_names.len());

        for (idx, field_name) in ordered_field_names.iter().enumerate() {
            let maybe_value = ordinal_row.get(idx).cloned().flatten();
            if let Some(value) = maybe_value {
                row_map.insert(field_name.clone(), value);
            }
        }

        return Ok(row_map);
    }

    if let Ok(legacy_row) = bincode::deserialize::<HashMap<String, Vec<u8>>>(payload) {
        return Ok(legacy_row);
    }

    if let Ok(legacy_ordinal_row) = bincode::deserialize::<Vec<Vec<u8>>>(payload) {

        let mut row_map = HashMap::with_capacity(ordered_field_names.len());

        for (idx, field_name) in ordered_field_names.iter().enumerate() {
            if let Some(value) = legacy_ordinal_row.get(idx) {
                row_map.insert(field_name.clone(), value.clone());
            }
        }

        return Ok(row_map);
    }

    Err("row payload decode failed".to_string())
    
}


#[cfg(test)]
#[path = "row_payload_test.rs"]
mod tests;

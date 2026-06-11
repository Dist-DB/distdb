
use super::field_def::FieldDef;
pub use super::index_id::IndexId;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseIndex {
	pub index_id: IndexId,
	pub table_id: String,
	pub field_name: String,
}

impl DatabaseIndex {

	pub fn from_table_field(table_id: &str, field: &FieldDef) -> Self {

		let table_id = common::normalize_identifier!(table_id);
		let field_name = common::normalize_identifier!(&field.field_name);
		let index_id = IndexId(format!("{}:{}", table_id, field_name));

		Self {
			index_id,
			table_id,
			field_name,
		}
		
	}

}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::engine::database::field_types::{FieldIndex, FieldType};

	#[test]
	fn index_id_is_normalized_from_table_and_field() {

		let field = FieldDef {
			seqno: 1,
			field_name: "UserId".to_string(),
			field_type: FieldType::UInt(64),
			nullable: false,
			indexed: FieldIndex::Indexed,
			default_value: None,
			metadata: None,
		};

		let index = DatabaseIndex::from_table_field("UserAccounts", &field);

		assert_eq!(index.table_id, "useraccounts");
		assert_eq!(index.field_name, "userid");
		assert_eq!(index.index_id.0, "useraccounts:userid");
	
	}

}


use super::field_def::FieldDef;
pub use super::index_id::IndexId;
use super::field_types::FieldIndex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DatabaseIndexKind {
	PrimaryKey,
	Indexed,

}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DatabaseIndexOrigin {
	Derived,
	Relationship,
	Temporary,

}

impl DatabaseIndexOrigin {

	pub fn prefix(self) -> &'static str {
		match self {
			Self::Derived => "drv",
			Self::Relationship => "rel",
			Self::Temporary => "tmp",
		}
	}

}

impl Default for DatabaseIndexOrigin {

	fn default() -> Self {
		Self::Derived
	}

}

impl DatabaseIndexKind {

	pub fn prefix(self) -> &'static str {
		match self {
			Self::PrimaryKey => "pri",
			Self::Indexed => "ind",
		}
	}

}

impl Default for DatabaseIndexKind {

	fn default() -> Self {
		Self::Indexed
	}

}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseIndex {
	pub index_id: IndexId,
	pub table_id: String,
	#[serde(default)]
	pub kind: DatabaseIndexKind,
	#[serde(default)]
	pub origin: DatabaseIndexOrigin,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub temp_id: Option<String>,
	#[serde(default)]
	pub field_names: Vec<String>,
	#[serde(default)]
	pub field_name: String,
}

impl DatabaseIndex {

	pub fn from_table_field(table_id: &str, field: &FieldDef) -> Self {
		let kind = match field.indexed {
			FieldIndex::PrimaryKey => DatabaseIndexKind::PrimaryKey,
			_ => DatabaseIndexKind::Indexed,
		};

		Self::from_table_fields_with_origin(
			table_id,
			kind,
			DatabaseIndexOrigin::Derived,
			None,
			vec![common::normalize_identifier!(&field.field_name)],
		)
	}

	pub fn from_table_fields(
		table_id: &str,
		kind: DatabaseIndexKind,
		field_names: Vec<String>,
	) -> Self {
		Self::from_table_fields_with_origin(
			table_id,
			kind,
			DatabaseIndexOrigin::Derived,
			None,
			field_names,
		)
	}

	pub fn from_table_fields_with_origin(
		table_id: &str,
		kind: DatabaseIndexKind,
		origin: DatabaseIndexOrigin,
		temp_id: Option<String>,
		field_names: Vec<String>,
	) -> Self {

		let table_id = common::normalize_identifier!(table_id);
		let field_names = field_names
			.into_iter()
			.map(|field_name| common::normalize_identifier!(field_name))
			.collect::<Vec<_>>();
		let field_name = field_names.first().cloned().unwrap_or_default();
		let index_id = Self::compose_index_id(kind, origin, temp_id.as_deref(), &field_names);

		Self {
			index_id,
			table_id,
			kind,
			origin,
			temp_id,
			field_names,
			field_name,
		}
		
	}

	pub fn temporary(
		table_id: &str,
		kind: DatabaseIndexKind,
		temp_id: impl Into<String>,
		field_names: Vec<String>,
	) -> Self {

		Self::from_table_fields_with_origin(
			table_id,
			kind,
			DatabaseIndexOrigin::Temporary,
			Some(temp_id.into()),
			field_names,
		)

	}

	pub fn refresh_index_id(&mut self) {

		if self.field_names.is_empty() && !self.field_name.is_empty() {
			self.field_names = vec![self.field_name.clone()];
		}

		self.field_names = self
			.field_names
			.iter()
			.map(|field_name| common::normalize_identifier!(field_name))
			.collect::<Vec<_>>();
		
		self.field_name = self.field_names.first().cloned().unwrap_or_default();
		self.index_id = Self::compose_index_id(self.kind, self.origin, self.temp_id.as_deref(), &self.field_names);

	}

	pub fn is_primary_key(&self) -> bool {
		matches!(self.kind, DatabaseIndexKind::PrimaryKey)
	}

	pub fn is_temporary(&self) -> bool {
		matches!(self.origin, DatabaseIndexOrigin::Temporary)
	}

	pub fn is_relationship_driven(&self) -> bool {
		matches!(self.origin, DatabaseIndexOrigin::Relationship)
	}

	fn compose_index_id(
		kind: DatabaseIndexKind,
		origin: DatabaseIndexOrigin,
		temp_id: Option<&str>,
		field_names: &[String],
	) -> IndexId {

		let field_list = field_names.join(",");
		
		match origin {
			
			DatabaseIndexOrigin::Derived => IndexId(format!("{}:{}", kind.prefix(), field_list)),

			DatabaseIndexOrigin::Relationship => IndexId(format!("{}:{}:{}", origin.prefix(), kind.prefix(), field_list)),

			DatabaseIndexOrigin::Temporary => {
				let temp_id = temp_id.unwrap_or("temp");
				IndexId(format!("{}:{}:{}:{}", origin.prefix(), temp_id, kind.prefix(), field_list))
			}

		}
	}

}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::engine::database::field_types::{FieldIndex, FieldType};

	#[test]
	fn index_id_is_normalized_from_kind_and_field() {

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
		assert_eq!(index.kind, DatabaseIndexKind::Indexed);
		assert_eq!(index.origin, DatabaseIndexOrigin::Derived);
		assert_eq!(index.index_id.0, "ind:userid");
		assert_eq!(index.temp_id, None);
	
	}

	#[test]
	fn primary_key_index_uses_pri_prefix() {
		let field = FieldDef {
			seqno: 1,
			field_name: "Uid".to_string(),
			field_type: FieldType::UInt(64),
			nullable: false,
			indexed: FieldIndex::PrimaryKey,
			default_value: None,
			metadata: None,
		};

		let index = DatabaseIndex::from_table_field("UserAccounts", &field);

		assert_eq!(index.kind, DatabaseIndexKind::PrimaryKey);
		assert_eq!(index.origin, DatabaseIndexOrigin::Derived);
		assert_eq!(index.index_id.0, "pri:uid");
	}

	#[test]
	fn composite_index_id_uses_field_list() {
		let index = DatabaseIndex::from_table_fields_with_origin(
			"UserAccounts",
			DatabaseIndexKind::Indexed,
			DatabaseIndexOrigin::Relationship,
			None,
			vec!["Uid".to_string(), "IdPerson".to_string()],
		);

		assert_eq!(index.origin, DatabaseIndexOrigin::Relationship);
		assert_eq!(index.index_id.0, "rel:ind:uid,idperson");
	}

	#[test]
	fn temporary_index_uses_temp_id_in_identity() {
		let index = DatabaseIndex::temporary(
			"UserAccounts",
			DatabaseIndexKind::Indexed,
			"join-1",
			vec!["Uid".to_string()],
		);

		assert_eq!(index.origin, DatabaseIndexOrigin::Temporary);
		assert_eq!(index.temp_id.as_deref(), Some("join-1"));
		assert_eq!(index.index_id.0, "tmp:join-1:ind:uid");
	}

}


#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IndexId(pub String);

impl IndexId {

	pub fn new(kind_prefix: &str, field_names: &[String]) -> Self {
		Self(format!("{}:{}", kind_prefix, field_names.join(",")))
	}

}

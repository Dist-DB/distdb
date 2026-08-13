use common::schema::FieldKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeIndexKeyStrategy {
    String { case_insensitive: bool },
    Numeric,
    DateTime,
    Binary,
}

impl RuntimeIndexKeyStrategy {

    pub fn for_field_kind(field_kind: &FieldKind, case_insensitive: bool) -> Self {

        match field_kind {

            FieldKind::StringFixed(_) | FieldKind::Text | FieldKind::Enum(_) => {
                Self::String { case_insensitive }
            },

            FieldKind::Int(_) | FieldKind::UInt(_) | FieldKind::Float(_) => Self::Numeric,

            FieldKind::Date | FieldKind::DateTime | FieldKind::Timestamp => Self::DateTime,

            _ => Self::Binary,

        }

    }

    pub fn normalize(&self, value: &[u8]) -> Vec<u8> {
        match self {
            Self::String { case_insensitive } => normalize_runtime_index_string_key(value, *case_insensitive),
            Self::Numeric | Self::DateTime | Self::Binary => value.to_vec(),
        }
    }

    pub fn page_head(&self, value: &[u8], head_len: usize) -> Vec<u8> {
        if head_len == 0 {
            return Vec::new();
        }

        let normalized = self.normalize(value);
        let end = normalized.len().min(head_len);
        normalized[..end].to_vec()
    }

}

pub fn normalize_runtime_index_string_key(value: &[u8], case_insensitive: bool) -> Vec<u8> {

    if !case_insensitive {
        return value.to_vec();
    }

    let mut normalized = value.to_vec();
    for byte in &mut normalized {
        if (*byte >= b'A') && (*byte <= b'Z') {
            *byte = byte.wrapping_add(b'a' - b'A');
        }
    }

    normalized

}

pub fn runtime_index_string_page_head(value: &[u8], head_len: usize, case_insensitive: bool) -> Vec<u8> {
    RuntimeIndexKeyStrategy::String { case_insensitive }.page_head(value, head_len)
}

pub fn runtime_index_string_probe_variants(value: &[u8], case_insensitive: bool) -> Vec<Vec<u8>> {

    let normalized = normalize_runtime_index_string_key(value, case_insensitive);
    
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut variants = vec![normalized.clone()];

    for head_len in [5usize, 4usize, 3usize] {
        if normalized.len() >= head_len {
            variants.push(normalized[..head_len].to_vec());
        }
    }

    variants

}

pub fn encode_runtime_index_entry_key(key: &[Vec<u8>]) -> Option<Vec<u8>> {
    common::helpers::bincode_compat::serialize(key).ok()
}

pub fn decode_runtime_index_entry_key(key: &[u8]) -> Option<Vec<Vec<u8>>> {
    common::helpers::bincode_compat::deserialize::<Vec<Vec<u8>>>(key).ok()
}

#[cfg(test)]
mod tests {
    
    use common::schema::FieldKind;

    use super::{
        normalize_runtime_index_string_key, runtime_index_string_page_head,
        runtime_index_string_probe_variants, RuntimeIndexKeyStrategy,
    };

    #[test]
    fn runtime_index_string_key_normalization_is_ascii_case_insensitive() {
        let value = b"Alpha-42";
        let normalized = normalize_runtime_index_string_key(value, true);

        assert_eq!(normalized, b"alpha-42");
    }

    #[test]
    fn runtime_index_string_page_head_uses_normalized_prefix_only() {
        let head = runtime_index_string_page_head(b"Alpha-42", 5, true);
        assert_eq!(head, b"alpha");
    }

    #[test]
    fn runtime_index_key_strategy_matches_field_kind_family() {
        
        assert_eq!(
            RuntimeIndexKeyStrategy::for_field_kind(&FieldKind::Text, true),
            RuntimeIndexKeyStrategy::String { case_insensitive: true }
        );
        
        assert_eq!(
            RuntimeIndexKeyStrategy::for_field_kind(&FieldKind::Int(64), false),
            RuntimeIndexKeyStrategy::Numeric
        );
        
        assert_eq!(
            RuntimeIndexKeyStrategy::for_field_kind(&FieldKind::DateTime, false),
            RuntimeIndexKeyStrategy::DateTime
        );

    }

    #[test]
    fn runtime_index_numeric_and_datetime_page_heads_are_stable() {

        let numeric = RuntimeIndexKeyStrategy::Numeric.page_head(b"123456", 3);
        let datetime = RuntimeIndexKeyStrategy::DateTime.page_head(b"2026-08-12 15:31:00", 4);

        assert_eq!(numeric, b"123");
        assert_eq!(datetime, b"2026");

    }

    #[test]
    fn runtime_index_string_probe_variants_include_normalized_prefix_heads() {

        let variants = runtime_index_string_probe_variants(b"Alpha-42", true);

        assert!(variants.iter().any(|value| value == b"alpha-42"));
        assert!(variants.iter().any(|value| value == b"alpha"));

    }
}

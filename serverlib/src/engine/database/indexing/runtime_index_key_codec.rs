use common::schema::FieldKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeIndexKeyStrategy {
    String { case_insensitive: bool },
    Numeric,
    DateTime,
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeIndexNumericKind {
    Signed,
    Unsigned,
    Float,
}

pub fn numeric_gate_depth(bit_width: usize) -> usize {
    bit_width.saturating_sub(8).div_ceil(8)
}

pub fn numeric_gate_prefix(value: &[u8], kind: RuntimeIndexNumericKind, bit_width: usize) -> Option<Vec<u8>> {
    let sortable = encode_sortable_numeric(value, kind)?;
    let byte_width = bit_width.div_ceil(8).min(sortable.len());
    let depth = numeric_gate_depth(bit_width).min(byte_width);
    Some(sortable[..depth].to_vec())
}

pub fn encode_sortable_numeric(value: &[u8], kind: RuntimeIndexNumericKind) -> Option<Vec<u8>> {
    match kind {
        RuntimeIndexNumericKind::Signed => {
            let value = std::str::from_utf8(value).ok()?.parse::<i64>().ok()?;
            Some(((value as u64) ^ (1u64 << 63)).to_be_bytes().to_vec())
        }
        RuntimeIndexNumericKind::Unsigned => {
            let value = std::str::from_utf8(value).ok()?.parse::<u64>().ok()?;
            Some(value.to_be_bytes().to_vec())
        }
        RuntimeIndexNumericKind::Float => {
            let value = std::str::from_utf8(value).ok()?.trim().parse::<f64>().ok()?;

            // NaN has no position in a total order, so it stays out of the index.
            if value.is_nan() {
                return None;
            }

            let bits = value.to_bits();

            // Flip the sign bit for positives and every bit for negatives so the
            // big-endian bytes sort in numeric order.
            let sortable = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };

            Some(sortable.to_be_bytes().to_vec())
        }
    }
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
#[path = "runtime_index_key_codec_test.rs"]
mod tests;

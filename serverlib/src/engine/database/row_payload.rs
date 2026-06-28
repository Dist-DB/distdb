use std::collections::HashMap;

use super::schema_migration::{convert_value_to_field_type, TypeConversionPolicy};
use super::table_schema::TableSchema;
use super::transaction::transaction_record::{
    PayloadTransformError, TransactionPayloadContext, TransactionPayloadTransform,
    TransactionPayloadWriteTransform,
};
use super::transaction::transaction_kind::TransactionKind;

type OrdinalRowPayload = Vec<Option<Vec<u8>>>;

const ENCRYPTED_ROW_PAYLOAD_MAGIC: [u8; 4] = *b"dbrw";
pub const ENCRYPTED_ROW_PAYLOAD_ENVELOPE_VERSION: u8 = 1;
const ENCRYPTED_ROW_PAYLOAD_NONCE_SIZE_BYTES: usize = 12;
const ENCRYPTED_ROW_PAYLOAD_AUTH_TAG_SIZE_BYTES: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EncryptedRowPayloadEnvelope {
    pub key_version: u32,
    pub nonce: Vec<u8>,
    pub auth_tag: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptedRowPayloadTransformPolicy {
    PreserveOpaque,
    RejectEncrypted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncryptedRowPayloadTransform {
    policy: EncryptedRowPayloadTransformPolicy,
}

impl EncryptedRowPayloadTransform {
    pub fn preserve_opaque() -> Self {
        Self {
            policy: EncryptedRowPayloadTransformPolicy::PreserveOpaque,
        }
    }

    pub fn reject_encrypted() -> Self {
        Self {
            policy: EncryptedRowPayloadTransformPolicy::RejectEncrypted,
        }
    }
}

impl TransactionPayloadTransform for EncryptedRowPayloadTransform {
    fn transform_payload(
        &self,
        payload: &[u8],
        _context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
        match decode_encrypted_row_payload_envelope(payload) {
            Ok(Some(_)) => match self.policy {
                EncryptedRowPayloadTransformPolicy::PreserveOpaque => {
                    Ok(Some(payload.to_vec()))
                }
                EncryptedRowPayloadTransformPolicy::RejectEncrypted => {
                    Err(PayloadTransformError::DecryptFailed)
                }
            },
            Ok(None) => Ok(None),
            Err(message) => {
                if message.starts_with("unsupported encrypted row payload version") {
                    Err(PayloadTransformError::UnsupportedFormat)
                } else {
                    Err(PayloadTransformError::InvalidEncryptedPayload)
                }
            }
        }
    }
}

impl TransactionPayloadWriteTransform for EncryptedRowPayloadTransform {
    fn transform_payload_for_write(
        &self,
        _record: &super::transaction::transaction_record::TransactionRecord,
        payload: &[u8],
        _context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
        match decode_encrypted_row_payload_envelope(payload) {
            Ok(Some(_)) => match self.policy {
                EncryptedRowPayloadTransformPolicy::PreserveOpaque => {
                    Ok(Some(payload.to_vec()))
                }
                EncryptedRowPayloadTransformPolicy::RejectEncrypted => {
                    Err(PayloadTransformError::InvalidEncryptedPayload)
                }
            },
            Ok(None) => Ok(None),
            Err(message) => {
                if message.starts_with("unsupported encrypted row payload version") {
                    Err(PayloadTransformError::UnsupportedFormat)
                } else {
                    Err(PayloadTransformError::InvalidEncryptedPayload)
                }
            }
        }
    }
}

pub trait RowPayloadEncryptionProvider: Send + Sync {
    fn encrypt_row_payload(
        &self,
        context: &TransactionPayloadContext,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, PayloadTransformError>;
}

pub trait RowPayloadDecryptionProvider: Send + Sync {
    fn decrypt_row_payload(
        &self,
        context: &TransactionPayloadContext,
        envelope: &EncryptedRowPayloadEnvelope,
    ) -> Result<Vec<u8>, PayloadTransformError>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnconfiguredRowPayloadEncryptionProvider;

impl RowPayloadEncryptionProvider for UnconfiguredRowPayloadEncryptionProvider {
    fn encrypt_row_payload(
        &self,
        _context: &TransactionPayloadContext,
        _plaintext: &[u8],
    ) -> Result<Vec<u8>, PayloadTransformError> {
        Err(PayloadTransformError::EncryptionNotConfigured)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnconfiguredRowPayloadDecryptionProvider;

impl RowPayloadDecryptionProvider for UnconfiguredRowPayloadDecryptionProvider {
    fn decrypt_row_payload(
        &self,
        _context: &TransactionPayloadContext,
        _envelope: &EncryptedRowPayloadEnvelope,
    ) -> Result<Vec<u8>, PayloadTransformError> {
        Err(PayloadTransformError::EncryptionNotConfigured)
    }
}

pub struct RowPayloadEncryptionWriteTransform<P> {
    provider: P,
}

impl<P> RowPayloadEncryptionWriteTransform<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

pub struct RowPayloadDecryptionTransform<P> {
    provider: P,
}

impl<P> RowPayloadDecryptionTransform<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }
}

impl<P: RowPayloadEncryptionProvider> TransactionPayloadWriteTransform
    for RowPayloadEncryptionWriteTransform<P>
{
    fn transform_payload_for_write(
        &self,
        record: &super::transaction::transaction_record::TransactionRecord,
        payload: &[u8],
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
        if !matches!(record.kind, TransactionKind::Insert | TransactionKind::Update) {
            return Ok(None);
        }

        if looks_like_encrypted_row_payload(payload) {
            return Ok(None);
        }

        let Some(_key_ref) = context.at_rest_encryption_key_ref() else {
            return Ok(None);
        };

        if context.at_rest_encryption_key_version().unwrap_or(0) == 0 {
            return Err(PayloadTransformError::EncryptionNotConfigured);
        }

        self.provider
            .encrypt_row_payload(context, payload)
            .map(Some)
    }
}

impl<P: RowPayloadDecryptionProvider> TransactionPayloadTransform for RowPayloadDecryptionTransform<P> {
    fn transform_payload(
        &self,
        payload: &[u8],
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
        let Some(key_ref) = context.at_rest_encryption_key_ref() else {
            return Ok(None);
        };

        let key_version = context.at_rest_encryption_key_version().unwrap_or(0);
        if key_version == 0 || key_ref.is_empty() {
            return Err(PayloadTransformError::EncryptionNotConfigured);
        }

        match decode_encrypted_row_payload_envelope(payload) {
            Ok(Some(envelope)) => self
                .provider
                .decrypt_row_payload(context, &envelope)
                .map(Some),
            Ok(None) => Ok(None),
            Err(message) => {
                if message.starts_with("unsupported encrypted row payload version") {
                    Err(PayloadTransformError::UnsupportedFormat)
                } else {
                    Err(PayloadTransformError::InvalidEncryptedPayload)
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct LegacyEncryptedRowPayloadEnvelope {
    magic: [u8; 4],
    version: u8,
    key_version: u32,
    nonce: Vec<u8>,
    auth_tag: Vec<u8>,
    ciphertext: Vec<u8>,
}

impl EncryptedRowPayloadEnvelope {
    pub fn new(key_version: u32, nonce: Vec<u8>, auth_tag: Vec<u8>, ciphertext: Vec<u8>) -> Self {
        Self {
            key_version,
            nonce,
            auth_tag,
            ciphertext,
        }
    }
}

pub fn encode_encrypted_row_payload_envelope(
    key_version: u32,
    nonce: Vec<u8>,
    auth_tag: Vec<u8>,
    ciphertext: Vec<u8>,
) -> Result<Vec<u8>, String> {
    let envelope = EncryptedRowPayloadEnvelope::new(key_version, nonce, auth_tag, ciphertext);
    bincode::serialize(&envelope).map_err(|err| err.to_string())
}

pub fn decode_encrypted_row_payload_envelope(
    payload: &[u8],
) -> Result<Option<EncryptedRowPayloadEnvelope>, String> {

    if let Ok(envelope) = bincode::deserialize::<EncryptedRowPayloadEnvelope>(payload) {
        if looks_like_valid_encrypted_payload_envelope(&envelope) {
            return Ok(Some(envelope));
        }
    }

    let Ok(legacy_envelope) = bincode::deserialize::<LegacyEncryptedRowPayloadEnvelope>(payload) else {
        return Ok(None);
    };

    if legacy_envelope.magic != ENCRYPTED_ROW_PAYLOAD_MAGIC {
        return Ok(None);
    }

    if legacy_envelope.version != ENCRYPTED_ROW_PAYLOAD_ENVELOPE_VERSION {
        return Err(format!(
            "unsupported encrypted row payload version {}",
            legacy_envelope.version
        ));
    }

    let envelope = EncryptedRowPayloadEnvelope {
        key_version: legacy_envelope.key_version,
        nonce: legacy_envelope.nonce,
        auth_tag: legacy_envelope.auth_tag,
        ciphertext: legacy_envelope.ciphertext,
    };

    if !looks_like_valid_encrypted_payload_envelope(&envelope) {
        return Ok(None);
    }

    Ok(Some(envelope))
    
}

pub fn looks_like_encrypted_row_payload(payload: &[u8]) -> bool {
    matches!(decode_encrypted_row_payload_envelope(payload), Ok(Some(_)))
}

fn looks_like_valid_encrypted_payload_envelope(
    envelope: &EncryptedRowPayloadEnvelope,
) -> bool {
    envelope.key_version > 0
        && envelope.nonce.len() == ENCRYPTED_ROW_PAYLOAD_NONCE_SIZE_BYTES
        && envelope.auth_tag.len() == ENCRYPTED_ROW_PAYLOAD_AUTH_TAG_SIZE_BYTES
        && !envelope.ciphertext.is_empty()
}

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
        .map(|field_name| {
            row_map.get(&field_name).map(|value| {
                schema
                    .field(&field_name)
                    .and_then(|field| {
                        convert_value_to_field_type(
                            value,
                            &field.field_type,
                            TypeConversionPolicy::Safe,
                        )
                        .ok()
                    })
                    .unwrap_or_else(|| value.clone())
            })
        })
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

    if decode_encrypted_row_payload_envelope(payload)?.is_some() {
        return Err(
            "row payload is encrypted at rest and must be decrypted before decode".to_string(),
        );
    }

    Err("row payload decode failed".to_string())
    
}

pub fn decode_row_field_value(
    schema: &TableSchema,
    payload: &[u8],
    field_name: &str,
) -> Result<Option<Vec<u8>>, String> {

    let ordered_field_names = field_names_by_ordinal(schema);

    if let Some(position) = ordered_field_names.iter().position(|name| name == field_name)
        && let Ok(ordinal_row) = bincode::deserialize::<OrdinalRowPayload>(payload) {
            return Ok(ordinal_row.get(position).cloned().flatten());
        }

    if let Ok(legacy_row) = bincode::deserialize::<HashMap<String, Vec<u8>>>(payload) {
        return Ok(legacy_row.get(field_name).cloned());
    }

    if let Some(position) = ordered_field_names.iter().position(|name| name == field_name)
        && let Ok(legacy_ordinal_row) = bincode::deserialize::<Vec<Vec<u8>>>(payload) {
            return Ok(legacy_ordinal_row.get(position).cloned());
        }

    if decode_encrypted_row_payload_envelope(payload)?.is_some() {
        return Err(
            "row payload is encrypted at rest and must be decrypted before field decode".to_string(),
        );
    }

    Err("row payload decode failed".to_string())

}


#[cfg(test)]
#[path = "row_payload_test.rs"]
mod tests;

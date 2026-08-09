use std::io::Read;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn serialize<T: Serialize>(value: T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy())
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(bytes, bincode::config::legacy()).map(|(value, _)| value)
}

pub fn deserialize_from<R: Read, T: DeserializeOwned>(
    reader: &mut R,
) -> Result<T, bincode::error::DecodeError> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|_| bincode::error::DecodeError::Other("failed to read bincode payload"))?;
    deserialize(&bytes)
}
use std::io::Read;

use serde::Serialize;
use serde::de::DeserializeOwned;

const MAX_DECODE_BYTES: usize = 256 * 1024 * 1024;

fn decode_config() -> impl bincode::config::Config {
    bincode::config::legacy().with_limit::<MAX_DECODE_BYTES>()
}

pub fn serialize<T: Serialize>(value: T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(value, bincode::config::legacy())
}

pub fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::error::DecodeError> {
    bincode::serde::decode_from_slice(bytes, decode_config()).map(|(value, _)| value)
}

pub fn deserialize_from<R: Read, T: DeserializeOwned>(
    reader: &mut R,
) -> Result<T, bincode::error::DecodeError> {
    // Bound streaming decode input so corrupted compressed payloads cannot inflate indefinitely.
    let mut limited = reader.take((MAX_DECODE_BYTES as u64) + 1);
    let mut bytes = Vec::new();
    limited
        .read_to_end(&mut bytes)
        .map_err(|_| bincode::error::DecodeError::Other("failed to read bincode payload"))?;

    if bytes.len() > MAX_DECODE_BYTES {
        return Err(bincode::error::DecodeError::Other("bincode payload exceeds decode limit"));
    }

    deserialize(&bytes)
}
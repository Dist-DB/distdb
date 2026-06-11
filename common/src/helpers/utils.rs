
use uuid::Uuid;
use std::time::{SystemTime};
use chrono::{DateTime};


pub fn unique_id() -> String {
    Uuid::now_v7().to_string().replace('-', "")
}


pub fn normalize_identifier(value: impl AsRef<str>) -> String {
    value.as_ref().trim().to_ascii_lowercase()
}


pub fn epoch() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(_n) => _n.as_secs().try_into().unwrap(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}


pub fn epochabs() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(_n) => _n.as_nanos().try_into().unwrap(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}


pub fn epoch_to_utcdate(epochin: i64, format: &str) -> String {    
    let datetime = DateTime::from_timestamp(epochin, 0).unwrap();
    datetime.format(format).to_string()
}


pub fn md5_hash(stringin: &str) -> String {
    md5(stringin.as_bytes())
}


pub fn md5(bytes : &[u8]) -> String {
    let _digest = md5::compute(bytes);
    format!("{:x}", _digest)
}
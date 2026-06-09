
use uuid::Uuid;
use std::time::{SystemTime};
use chrono::{DateTime};


pub fn unique_id() -> String {
    Uuid::now_v7().to_string().replace('-', "")
}


pub fn epoch() -> i64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(_n) => _n.as_secs().try_into().unwrap(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}


pub fn epoch_to_utcdate(epochin: i64, format: &str) -> String {    
    let datetime = DateTime::from_timestamp(epochin, 0).unwrap();
    datetime.format(format).to_string()
}
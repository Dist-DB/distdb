
use uuid::Uuid;


pub fn unique_id() -> String {
    Uuid::now_v7().to_string().replace('-', "")
}
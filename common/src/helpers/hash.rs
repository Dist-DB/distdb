
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn stable_id(parts: &[&str]) -> String {

    let mut hasher = DefaultHasher::new();
    for part in parts {
        part.hash(&mut hasher);
        ":".hash(&mut hasher);
    }
    
    format!("{:016x}", hasher.finish())

}
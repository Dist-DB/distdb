
pub fn stable_id(parts: &[&str]) -> String {

    let mut joined = String::new();
    for part in parts {
        joined.push_str(part);
        joined.push(':');
    }

    let digest = md5::compute(joined.as_bytes());
    // Keep 64-bit style identifier width for compatibility with existing callers.
    format!("{:x}", digest)[..16].to_string()

}
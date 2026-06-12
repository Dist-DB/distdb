
pub(super) fn parse_default_value(value: String) -> Option<Vec<u8>> {

    let trimmed = value.trim();
    
    if trimmed.eq_ignore_ascii_case("null") {
        return None;
    }

    Some(
        trimmed
            .trim_matches('`')
            .trim_matches('"')
            .trim_matches('\'')
            .as_bytes()
            .to_vec(),
    )
    
}


pub fn parse_tls_mode_from_args(args: &[String]) -> Result<common::TlsMode, String> {

    match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
        Some(raw) => common::TlsMode::parse(raw)
            .ok_or_else(|| format!("invalid tls mode '{raw}'; use off|optional|required")),
        None => Ok(common::TlsMode::Required),
    }
    
}

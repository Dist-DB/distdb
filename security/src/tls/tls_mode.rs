
pub fn parse_tls_mode_from_args(args: &[String]) -> Result<common::TlsMode, String> {

    match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
        Some(raw) => Err(format!(
            "tls mode is fixed to required; remove unsupported argument tls={raw}"
        )),
        None => Ok(common::TlsMode::Required),
    }
    
}

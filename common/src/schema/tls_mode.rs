#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    #[default]
    Off,
    Optional,
    Required,
}

impl TlsMode {

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Optional => "optional",
            Self::Required => "required",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "false" | "0" => Some(Self::Off),
            "optional" => Some(Self::Optional),
            "required" | "on" | "true" | "1" => Some(Self::Required),
            _ => None,
        }
    }
    
}

/// Standard file extension for all distdb table data files.
pub const FILE_EXTENSION: &str = "dtbl";

/// Magic bytes written at the start of every distdb file.
/// ASCII "DTBL" — identifies the file as belonging to this service.
pub const MAGIC: [u8; 4] = *b"DTBL";

/// Format version embedded in the file header after the magic bytes.
/// Increment when the on-disk layout changes in a backwards-incompatible way.
pub const FORMAT_VERSION: u8 = 1;

/// Total header size in bytes: 4 magic + 1 version.
pub const HEADER_SIZE: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    TooShort,
    BadMagic,
    UnsupportedVersion(u8),
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "file too short to contain a valid header"),
            Self::BadMagic => write!(f, "file magic bytes do not match DTBL"),
            Self::UnsupportedVersion(v) => {
                write!(f, "unsupported format version {v} (expected {FORMAT_VERSION})")
            }
        }
    }
}

impl std::error::Error for HeaderError {}

/// Returns the 5-byte header that must be prepended to every distdb file.
pub fn make_header() -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[..4].copy_from_slice(&MAGIC);
    header[4] = FORMAT_VERSION;
    header
}

/// Validates the header slice from the start of a file's bytes.
/// Returns `Ok(format_version)` on success.
pub fn verify_header(bytes: &[u8]) -> Result<u8, HeaderError> {
    if bytes.len() < HEADER_SIZE {
        return Err(HeaderError::TooShort);
    }
    if bytes[..4] != MAGIC {
        return Err(HeaderError::BadMagic);
    }
    let version = bytes[4];
    if version != FORMAT_VERSION {
        return Err(HeaderError::UnsupportedVersion(version));
    }
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_header_round_trips_through_verify() {
        let header = make_header();
        let version = verify_header(&header).expect("header should be valid");
        assert_eq!(version, FORMAT_VERSION);
    }

    #[test]
    fn verify_rejects_bad_magic() {
        let mut header = make_header();
        header[0] = b'X';
        assert_eq!(verify_header(&header), Err(HeaderError::BadMagic));
    }

    #[test]
    fn verify_rejects_wrong_version() {
        let mut header = make_header();
        header[4] = 99;
        assert_eq!(verify_header(&header), Err(HeaderError::UnsupportedVersion(99)));
    }

    #[test]
    fn verify_rejects_too_short() {
        assert_eq!(verify_header(&[0u8; 3]), Err(HeaderError::TooShort));
    }
}

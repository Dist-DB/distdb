
/// File kinds used by the distdb on-disk layout.
/// The formatter keeps file naming consistent across services.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Data,
    Catalog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MagicCode([u8; 4]);

impl MagicCode {
    pub const fn new(bytes: [u8; 4]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(self) -> [u8; 4] {
        self.0
    }
}

impl AsRef<[u8]> for MagicCode {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl FileKind {

    pub fn extension(self) -> &'static str {
        match self {
            Self::Data => "dtbl",
            Self::Catalog => "dbcat",
        }
    }

    pub fn magic(self) -> MagicCode {
        match self {
            Self::Data => MagicCode::new(*b"DTBL"),
            Self::Catalog => MagicCode::new(*b"DBCT"),
        }
    }

    pub fn file_name(self, stem: impl AsRef<str>) -> String {
        format!("{}.{}", stem.as_ref(), self.extension())
    }
    
}

impl std::fmt::Display for FileKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.extension())
    }
}

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

/// Returns the 5-byte header that must be prepended to a file of the given kind.
pub fn make_header(kind: FileKind) -> [u8; HEADER_SIZE] {
    let mut header = [0u8; HEADER_SIZE];
    header[..4].copy_from_slice(kind.magic().as_ref());
    header[4] = FORMAT_VERSION;
    header
}

/// Validates the header slice from the start of a file's bytes for the given kind.
/// Returns `Ok(format_version)` on success.
pub fn verify_header(kind: FileKind, bytes: &[u8]) -> Result<u8, HeaderError> {

    if bytes.len() < HEADER_SIZE {
        return Err(HeaderError::TooShort);
    }
    
    if &bytes[..4] != kind.magic().as_ref() {
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
        let header = make_header(FileKind::Data);
        let version = verify_header(FileKind::Data, &header).expect("header should be valid");
        assert_eq!(version, FORMAT_VERSION);
    }

    #[test]
    fn verify_rejects_bad_magic() {
        let mut header = make_header(FileKind::Data);
        header[0] = b'X';
        assert_eq!(verify_header(FileKind::Data, &header), Err(HeaderError::BadMagic));
    }

    #[test]
    fn verify_rejects_wrong_version() {
        let mut header = make_header(FileKind::Data);
        header[4] = 99;
        assert_eq!(verify_header(FileKind::Data, &header), Err(HeaderError::UnsupportedVersion(99)));
    }

    #[test]
    fn verify_rejects_too_short() {
        assert_eq!(verify_header(FileKind::Data, &[0u8; 3]), Err(HeaderError::TooShort));
    }

    #[test]
    fn file_kind_formats_extension_and_name() {
        assert_eq!(FileKind::Data.extension(), "dtbl");
        assert_eq!(FileKind::Catalog.extension(), "dbcat");
        assert_eq!(FileKind::Catalog.file_name("demo-db"), "demo-db.dbcat");
        assert_eq!(FileKind::Data.magic().as_bytes(), *b"DTBL");
        assert_eq!(FileKind::Catalog.magic().as_bytes(), *b"DBCT");
    }
    
}

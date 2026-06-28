
pub fn b64_encode_bytes(bytesin: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    if bytesin.is_empty() {
        return String::new();
    }

    let mut out = String::with_capacity((bytesin.len() * 4).div_ceil(3));
    let mut i = 0usize;

    while i + 3 <= bytesin.len() {
        let n = ((bytesin[i] as u32) << 16)
            | ((bytesin[i + 1] as u32) << 8)
            | (bytesin[i + 2] as u32);

        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        out.push(TABLE[(n & 0x3f) as usize] as char);

        i += 3;
    }

    match bytesin.len() - i {
        1 => {
            let n = (bytesin[i] as u32) << 16;
            out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        }
        2 => {
            let n = ((bytesin[i] as u32) << 16) | ((bytesin[i + 1] as u32) << 8);
            out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        }
        _ => {}
    }

    out
}

fn decode_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' | b'-' => Some(62),
        b'/' | b'_' => Some(63),
        _ => None,
    }
}

fn b64_decode_impl(stringin: &str) -> Option<Vec<u8>> {
    if stringin.is_empty() {
        return Some(Vec::new());
    }

    let mut data = stringin.as_bytes().to_vec();
    let mut pad_count = 0usize;

    while matches!(data.last(), Some(b'=')) {
        data.pop();
        pad_count += 1;
    }

    if pad_count > 2 {
        return None;
    }

    let remainder = data.len() % 4;
    if remainder == 1 {
        return None;
    }

    let mut out = Vec::with_capacity(data.len() * 3 / 4 + 2);
    let mut i = 0usize;

    while i + 4 <= data.len() {
        let a = decode_char(data[i])? as u32;
        let b = decode_char(data[i + 1])? as u32;
        let c = decode_char(data[i + 2])? as u32;
        let d = decode_char(data[i + 3])? as u32;

        let n = (a << 18) | (b << 12) | (c << 6) | d;
        out.push(((n >> 16) & 0xff) as u8);
        out.push(((n >> 8) & 0xff) as u8);
        out.push((n & 0xff) as u8);

        i += 4;
    }

    match data.len() - i {
        0 => {}
        2 => {
            let a = decode_char(data[i])? as u32;
            let b = decode_char(data[i + 1])? as u32;
            let n = (a << 18) | (b << 12);
            out.push(((n >> 16) & 0xff) as u8);
        }
        3 => {
            let a = decode_char(data[i])? as u32;
            let b = decode_char(data[i + 1])? as u32;
            let c = decode_char(data[i + 2])? as u32;
            let n = (a << 18) | (b << 12) | (c << 6);
            out.push(((n >> 16) & 0xff) as u8);
            out.push(((n >> 8) & 0xff) as u8);
        }
        _ => return None,
    }

    Some(out)
}

pub fn b64_decode(stringin: &str) -> Vec<u8> {
    b64_decode_impl(stringin).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{b64_decode, b64_encode_bytes};

    #[test]
    fn encode_matches_standard_no_pad_vectors() {
        assert_eq!(b64_encode_bytes(b""), "");
        assert_eq!(b64_encode_bytes(b"f"), "Zg");
        assert_eq!(b64_encode_bytes(b"fo"), "Zm8");
        assert_eq!(b64_encode_bytes(b"foo"), "Zm9v");
        assert_eq!(b64_encode_bytes(b"hello world"), "aGVsbG8gd29ybGQ");
    }

    #[test]
    fn decode_accepts_standard_and_padded_input() {
        assert_eq!(b64_decode("aGVsbG8gd29ybGQ"), b"hello world");
        assert_eq!(b64_decode("aGVsbG8gd29ybGQ="), b"hello world");
    }

    #[test]
    fn decode_accepts_url_safe_variants() {
        assert_eq!(b64_decode("-w"), vec![0xfb]);
        assert_eq!(b64_decode("_w"), vec![0xff]);
        assert_eq!(b64_decode("AAE_"), vec![0x00, 0x01, 0x3f]);
    }

    #[test]
    fn decode_invalid_returns_empty() {
        assert!(b64_decode("abc$").is_empty());
        assert!(b64_decode("A").is_empty());
    }
}

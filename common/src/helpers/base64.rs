
// Due to the fact that the base64 crate is not no_std compatible, we implement our own base64 encoder/decoder here

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

const INVALID_BASE64: u8 = 0xff;

const fn build_decode_table() -> [u8; 256] {
    let mut table = [INVALID_BASE64; 256];
    let mut i = 0usize;

    while i < 26 {
        table[b'A' as usize + i] = i as u8;
        table[b'a' as usize + i] = (26 + i) as u8;
        i += 1;
    }

    i = 0;
    while i < 10 {
        table[b'0' as usize + i] = (52 + i) as u8;
        i += 1;
    }

    table[b'+' as usize] = 62;
    table[b'-' as usize] = 62;
    table[b'/' as usize] = 63;
    table[b'_' as usize] = 63;
    table
}

const DECODE_TABLE: [u8; 256] = build_decode_table();

#[inline]
fn decode_char(c: u8) -> Option<u8> {
    let v = DECODE_TABLE[c as usize];
    if v == INVALID_BASE64 {
        None
    } else {
        Some(v)
    }
}

fn b64_decode_impl(stringin: &str) -> Option<Vec<u8>> {

    if stringin.is_empty() {
        return Some(Vec::new());
    }

    let data = stringin.as_bytes();
    let mut data_len = data.len();
    let mut pad_count = 0usize;

    while data_len > 0 && data[data_len - 1] == b'=' {
        data_len -= 1;
        pad_count += 1;
    }

    if pad_count > 2 {
        return None;
    }

    let remainder = data_len % 4;
    if remainder == 1 {
        return None;
    }

    let mut out = Vec::with_capacity(data_len * 3 / 4 + 2);
    let mut i = 0usize;

    while i + 4 <= data_len {
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

    match data_len - i {
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
#[path = "base64_test.rs"]
mod tests;

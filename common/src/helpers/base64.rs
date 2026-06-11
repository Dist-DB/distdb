
use base64::{Engine as _, engine::general_purpose};


pub fn b64_encode_bytes(bytesin: &[u8]) -> String {
    general_purpose::STANDARD_NO_PAD.encode(bytesin)
}


pub fn b64_decode_withengine(stringin: &str, engine: general_purpose::GeneralPurpose) -> Vec<u8> {
    engine.decode(stringin).unwrap_or_default()
}


pub fn b64_decode(stringin: &str) -> Vec<u8> {
    let mut _result = b64_decode_withengine(stringin, general_purpose::STANDARD);
    if _result.is_empty() {
        _result = b64_decode_withengine(stringin, general_purpose::STANDARD_NO_PAD);
        if _result.is_empty() {
            _result = b64_decode_withengine(stringin, general_purpose::URL_SAFE);
            if _result.is_empty() {
                _result = b64_decode_withengine(stringin, general_purpose::URL_SAFE_NO_PAD);
            }
        }    
    }
    _result
}

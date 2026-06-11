
// using OpenSSL for AES encryption/decryption

use openssl::hash::{hash, MessageDigest as HashMessageDigest};
use openssl::symm::Cipher;
use openssl::rand::rand_bytes;


use crate::helpers::base64::{
    b64_encode_bytes,
    b64_decode,
};


fn random_salt() -> [u8; 8] {
    let mut salt = [0u8; 8];
    rand_bytes(&mut salt).unwrap();
    salt
}


// EVP_BytesToKey implementation for CryptoJS compatibility
fn evp_bytes_to_key(password: &[u8], salt: &[u8], key_len: usize, iv_len: usize) -> (Vec<u8>, Vec<u8>) {

    let mut key = Vec::new();
    let mut iv = Vec::new();
    let mut derived = Vec::new();
    let mut hash_input = Vec::new();
    
    while derived.len() < key_len + iv_len {
        hash_input.clear();
        if !derived.is_empty() {
            hash_input.extend_from_slice(&derived[derived.len() - 16..]);
        }
        hash_input.extend_from_slice(password);
        hash_input.extend_from_slice(salt);
        
        let hashed = hash(HashMessageDigest::md5(), &hash_input).unwrap();
        derived.extend_from_slice(hashed.as_ref());
    }
    
    key.extend_from_slice(&derived[0..key_len]);
    iv.extend_from_slice(&derived[key_len..key_len + iv_len]);
    
    (key, iv)

}


pub fn aes_encrypt(message: &str, secret: &str, salt: &[u8]) -> String {

    let cipher = Cipher::aes_256_cbc();
        
    // Derive key and IV using EVP_BytesToKey (CryptoJS default)
    let (key, iv) = evp_bytes_to_key(secret.as_bytes(), salt, 32, 16);
    
    let ciphertext = openssl::symm::encrypt(
        cipher,
        &key,
        Some(&iv),
        message.as_bytes(),
    ).unwrap();
    
    // Prepend "Salted__" + salt to ciphertext (CryptoJS format)
    let mut result = Vec::new();
    result.extend_from_slice(b"Salted__");
    result.extend_from_slice(salt);
    result.extend_from_slice(&ciphertext);

    b64_encode_bytes(&result)
    
}


pub fn aes_decrypt(encrypted_message: &str, secret: &str) -> String {

    let cipher = Cipher::aes_256_cbc();
    let decoded = b64_decode(encrypted_message);
    
    // Extract salt from the message (CryptoJS format)
    // Format: "Salted__" (8 bytes) + salt (8 bytes) + ciphertext
    if decoded.len() < 16 || &decoded[0..8] != b"Salted__" {
        panic!("Invalid encrypted message format");
    }
    
    let salt = &decoded[8..16];
    let ciphertext = &decoded[16..];
    
    // Derive key and IV using EVP_BytesToKey (CryptoJS default)
    let (key, iv) = evp_bytes_to_key(secret.as_bytes(), salt, 32, 16);

    let decrypted_data = openssl::symm::decrypt(
        cipher,
        &key,
        Some(&iv),
        ciphertext,
    ).unwrap();

    String::from_utf8_lossy(&decrypted_data).to_string()

}


// Tests for validating functionality..
#[cfg(test)]
mod test {

    #[test]
    fn aes_tests() {

        use super::{aes_encrypt, aes_decrypt, random_salt};

        let secret = "maryhadalittlelamb".to_string(); // 32 bytes for AES-256
        let original_message = "hello world";

        // To use random: let mut salt = [0u8; 8]; rand_bytes(&mut salt).unwrap();
        let salt = random_salt();

        let encrypted_message = aes_encrypt(original_message, &secret, &salt);
        let decrypted_message = aes_decrypt(&encrypted_message, &secret);

        println!("Original Message: {}", original_message);
        println!("Encrypted Message: {}", encrypted_message);
        println!("Decrypted Message: {}", decrypted_message);

        assert_eq!(original_message, decrypted_message);

    }
    
}
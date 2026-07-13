
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
    
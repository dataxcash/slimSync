use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hmac::Mac;
use rand::Rng;
use sha2::Sha256;

type HmacSha256 = hmac::Hmac<Sha256>;

/// 生成 Blind-ID: HMAC-SHA256(chunk_text, group_salt)[0..16]
pub fn generate_blind_id(data: &[u8], salt: &[u8]) -> [u8; 16] {
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(salt).expect("HMAC key length");
    Mac::update(&mut mac, data);
    let result = mac.finalize().into_bytes();
    let mut blind_id = [0u8; 16];
    blind_id.copy_from_slice(&result[..16]);
    blind_id
}

/// ChaCha20-Poly1305 加密 Chunk 明文
pub fn encrypt_chunk(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let mut ciphertext = plaintext.to_vec();
    cipher
        .encrypt_in_place(nonce, &[], &mut ciphertext)
        .expect("encryption failed");

    [nonce_bytes.as_slice(), ciphertext.as_slice()].concat()
}

/// 解密（当前未使用，保留给后续 slimRagSvr 重放校验）
#[allow(dead_code)]
pub fn decrypt_chunk(key: &[u8; 32], data: &[u8]) -> Vec<u8> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);

    let mut plaintext = ciphertext.to_vec();
    cipher
        .decrypt_in_place(nonce, &[], &mut plaintext)
        .expect("decryption failed");
    plaintext
}

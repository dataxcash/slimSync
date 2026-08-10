use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::Rng;

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

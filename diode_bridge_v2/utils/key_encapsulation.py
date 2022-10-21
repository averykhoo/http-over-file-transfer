import secrets

from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

KEY_LEN: int = 32
TOKEN_LEN = 60  # len(encrypt_key(generate_key_bytes(), generate_key_fernet()))


def generate_hash_key():
    return secrets.token_bytes(KEY_LEN)


def generate_secret_key():
    return ChaCha20Poly1305.generate_key()


def encrypt_key(hash_key: bytes, secret_key: bytes):
    nonce = secrets.token_bytes(12)
    assert len(hash_key) == KEY_LEN
    out = ChaCha20Poly1305(secret_key).encrypt(nonce, hash_key, None)
    assert len(out) == TOKEN_LEN - len(nonce), len(out)
    return nonce + out


def decrypt_key(encapsulated_key: bytes, secret_key: bytes):
    assert len(encapsulated_key) == TOKEN_LEN
    nonce = encapsulated_key[:12]
    out = ChaCha20Poly1305(secret_key).decrypt(nonce, encapsulated_key[12:], None)
    assert len(out) == KEY_LEN
    return out


if __name__ == '__main__':
    for _ in range(5):
        hk = generate_hash_key()
        assert len(hk) == KEY_LEN
        sk = generate_secret_key()
        ek = encrypt_key(hk, sk)
        assert len(ek) == TOKEN_LEN
        assert decrypt_key(ek, sk) == hk

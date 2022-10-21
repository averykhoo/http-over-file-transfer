import random
import secrets

from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

HASH_KEY_LEN: int = 16
TOKEN_LEN = 44  # len(encrypt_key(generate_key_bytes(), generate_key_fernet()))


def generate_hash_key():
    return secrets.token_bytes(HASH_KEY_LEN)


def generate_secret_key():
    return ChaCha20Poly1305.generate_key() + secrets.token_bytes(random.randint(100, 200))  # fixme: no random in prod


def encrypt_key(hash_key: bytes, secret_key: bytes):
    nonce = secrets.token_bytes(12)
    assert len(hash_key) == HASH_KEY_LEN
    out = ChaCha20Poly1305(secret_key[:32]).encrypt(nonce, hash_key, None)
    assert len(out) == TOKEN_LEN - len(nonce), (len(out), TOKEN_LEN, len(nonce))
    return nonce + out


def decrypt_key(encapsulated_key: bytes, secret_key: bytes):
    assert len(encapsulated_key) == TOKEN_LEN
    nonce = encapsulated_key[:12]
    out = ChaCha20Poly1305(secret_key[:32]).decrypt(nonce, encapsulated_key[12:], None)
    assert len(out) == HASH_KEY_LEN
    return out


if __name__ == '__main__':
    for _ in range(5):
        hk = generate_hash_key()
        assert len(hk) == HASH_KEY_LEN
        sk = generate_secret_key()
        ek = encrypt_key(hk, sk)
        assert len(ek) == TOKEN_LEN
        assert decrypt_key(ek, sk) == hk

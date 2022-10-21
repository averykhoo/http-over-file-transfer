import secrets

KEY_LEN: int = 32


def generate_key():
    return secrets.token_bytes(KEY_LEN)


def encrypt_key(hash_key: bytes, secret_key: bytes):
    return bytes(a ^ b for a, b in zip(hash_key, secret_key))  # todo: very inefficient


def decrypt_key(hash_key: bytes, secret_key: bytes):
    return bytes(a ^ b for a, b in zip(hash_key, secret_key))

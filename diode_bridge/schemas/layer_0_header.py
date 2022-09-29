from dataclasses import dataclass


@dataclass
class Header:
    hash_digest: bytes
    data_size_bytes: int

    @property
    def hex_digest(self):
        return self.hash_digest.hex()

    @classmethod
    def from_bytes(cls, binary_data, hash_digest_len) -> 'Header':
        return Header(hash_digest=binary_data[:hash_digest_len],
                      data_size_bytes=int(binary_data[hash_digest_len:].rstrip(b'\0').decode('ascii')),
                      )

    def __bytes__(self):
        return self.hash_digest + str(self.data_size_bytes).encode('ascii')

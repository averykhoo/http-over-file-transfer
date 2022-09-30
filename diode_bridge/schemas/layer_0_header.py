from dataclasses import dataclass
from pathlib import Path
from uuid import UUID


@dataclass
class PacketSenderRecipient:
    # todo: integrate into new packet header
    sender_uuid: UUID
    recipient_uuid: UUID

    @classmethod
    def from_path(cls, path: Path) -> 'PacketSenderRecipient':
        _path = Path(path)
        if _path.is_dir():
            raise IsADirectoryError(path)
        if not _path.exists():
            raise FileNotFoundError(path)
        if _path.suffix != '.packet':
            raise ValueError(f'{path} is not a `.packet` file')
        if _path.stem.count('--') != 2:
            raise ValueError('file may have been renamed')
        _sender, _recipient, _timestamp = _path.stem.split('--')
        return PacketSenderRecipient(UUID(_sender), UUID(_recipient))


@dataclass
class Header:
    # todo: integrate into new packet header
    hash_digest: bytes
    data_size_bytes: int
    header_size_bytes = 64

    @property
    def hex_digest(self):
        return self.hash_digest.hex()

    @classmethod
    def from_bytes(cls, binary_data, hash_digest_len) -> 'Header':
        return Header(hash_digest=binary_data[:hash_digest_len],
                      data_size_bytes=int(binary_data[hash_digest_len:].rstrip(b'\0').decode('ascii')),
                      )

    def __bytes__(self):
        _len = str(self.data_size_bytes).encode('ascii')
        return self.hash_digest + _len + b'\0' * (self.header_size_bytes - len(self.hash_digest) - len(_len))

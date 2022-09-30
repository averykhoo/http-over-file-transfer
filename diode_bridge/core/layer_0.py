"""
Layer 0: reliably write and read files on some network folder
"""
import datetime
import hashlib
import math
import random
import shutil
import time
from pathlib import Path
from typing import Optional
from typing import Union
from uuid import uuid4

# header will contain a hash and the data length
from diode_bridge.dependencies.jose_wrapper import deserialize
from diode_bridge.dependencies.jose_wrapper import serialize
from diode_bridge.schemas.layer_0_header import Header
from diode_bridge.schemas.layer_1_packet import Packet

HASH_ALGORITHM = hashlib.sha384
HASH_DIGEST_LENGTH = len(HASH_ALGORITHM(b'\0').digest())
MAX_SIZE = 128 * 1024 * 1024  # 128 MiB == 134217728 bytes, 9 ascii characters
assert Header.header_size_bytes >= HASH_DIGEST_LENGTH + math.log10(MAX_SIZE)


class BinaryWriter:
    def __init__(self, path, temp_file_name=None, overwrite=False):
        self._path = Path(path)
        if self._path.is_dir():
            raise IsADirectoryError(path)
        elif self._path.exists() and not overwrite:
            raise FileExistsError(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        self._temp_file_name = temp_file_name or f'.{datetime.date.today().isoformat()}.{uuid4()}.partial'
        self._temp_file_path = self._path.parent / self._temp_file_name
        if self._temp_file_path.exists():
            raise FileExistsError(temp_file_name or self._temp_file_name)
        self._temp_file = None

        self._hash_object = HASH_ALGORITHM()
        self._len = 0

    @property
    def header(self) -> Header:
        return Header(self._hash_object.digest(), self._len)

    def __enter__(self):
        if self._temp_file is None:
            self._temp_file = self._temp_file_path.open('xb')
        self._temp_file.write(b'\0' * Header.header_size_bytes)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._temp_file is None:
            raise RuntimeError('temp file never opened')

        # write header
        if not self._temp_file.closed:
            self._temp_file.seek(0)
            self._temp_file.write(bytes(self.header))
            self._temp_file.close()
            shutil.move(self._temp_file_path, self._path)

    def write(self, binary_data: Union[bytes, bytearray]):
        if self._temp_file is None:
            raise IOError('temp file not yet open for writing, please use `with` syntax')
        if not self._temp_file.writable():
            raise IOError(f'file already written and cannot be edited: {self._path}')
        self._len += len(binary_data)
        if self._len > MAX_SIZE - Header.header_size_bytes:
            raise IOError(f'attempted to write too many bytes, max is {MAX_SIZE - Header.header_size_bytes}')
        self._hash_object.update(binary_data)
        self._temp_file.write(binary_data)
        self._temp_file.flush()


class BinaryReader:
    def __init__(self, path):
        self.path = Path(path)
        if self.path.is_dir():
            raise IsADirectoryError(path)
        elif not self.path.exists():
            raise FileNotFoundError(path)
        self._file = None
        self._data = bytearray()
        self._header = bytearray()
        self._hash_object = HASH_ALGORITHM()

    def __enter__(self):
        self._file = self.path.open('rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file is None:
            raise RuntimeError('file never opened for reading')

        self._file.close()

    @property
    def header(self):
        if len(self._header) < Header.header_size_bytes:
            return None
        return Header.from_bytes(self._header, HASH_DIGEST_LENGTH)

    def is_complete(self):
        return (self.header.hash_digest, self.header.data_size_bytes) == (self._hash_object.digest(), len(self._data))

    def try_read(self) -> Optional[bytearray]:
        if self._file is None:
            raise IOError('file never opened for reading')
        while True:
            if len(self._header) < Header.header_size_bytes:
                # read chunk into header buffer, then copy excess to data buffer
                self._header.extend(self._file.read(4 * 1024 * 1024))
                self._data.extend(self._header[Header.header_size_bytes:])
                self._hash_object.update(self._header[Header.header_size_bytes:])
                # truncate header buffer
                _header = self._header[:Header.header_size_bytes]
                self._header.clear()
                self._header.extend(_header)
            elif len(self._data) < self.header.data_size_bytes:
                _chunk = self._file.read(4 * 1024 * 1024)
                if not _chunk:  # unable to read any data, file still incomplete
                    break  # can retry later to read the rest of the file
                self._data.extend(_chunk)
                self._hash_object.update(_chunk)
            elif len(self._data) > self.header.data_size_bytes:
                raise IOError(f'data len {len(self._data)} greater than expected len {self.header.data_size_bytes}')
            elif self.is_complete():
                break
            else:  # correct data length but wrong hash, either data or header corrupted
                raise IOError('data corrupted and does not match hash')

        if self.is_complete():
            return self._data

    def size_bytes(self) -> int:
        return len(self._data)


def write_packet(packet: Packet, folder, overwrite=False) -> Header:
    timestamp = packet.metadata.sent_timestamp.strftime('%Y-%m-%d-%H-%M-%S-%f')
    file_name = f'{packet.metadata.sender_uuid}--{packet.metadata.recipient_uuid}--{timestamp}.packet'
    file_path = Path(folder) / str(packet.metadata.recipient_uuid) / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with BinaryWriter(file_path, overwrite=overwrite) as f:
        f.write(serialize(packet))
        out = f.header

    # randomly corrupt files
    if random.random() < 0.5:
        print('!!!', file_path)
        with file_path.open(mode='wb') as f:
            f.write(bytes(out))
            f.write(b'\0' * 32)
    return out


def read_packet_blocking(path, timeout_seconds: Optional[int] = None) -> Packet:
    t_start = time.monotonic()
    if timeout_seconds is None:
        timeout_seconds = float('inf')
    if timeout_seconds < 0:
        raise ValueError(timeout_seconds)
    t_max = t_start + timeout_seconds

    with BinaryReader(path) as f:
        while time.monotonic() < t_max:
            data = f.try_read()
            if data is not None:
                return deserialize(data)
            time.sleep(0.5)

        raise TimeoutError(f'timed out (file size is {f.size_bytes()} bytes, expected {f.header.data_size_bytes})')


if __name__ == '__main__':
    with BinaryWriter('test.bin', overwrite=True) as f_test:
        f_test.write(b'hello world')
    with BinaryReader('test.bin') as f_test:
        for _ in range(10):
            print(f_test.try_read())
            print(f_test.size_bytes())
            if f_test.is_complete():
                break
            time.sleep(5)

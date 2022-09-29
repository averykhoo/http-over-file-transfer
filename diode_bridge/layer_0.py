"""
Layer 0: reliably write and read files on some network folder

* some folder
  * recipient
    * {sender}--{recipient}--{id}.json
* write as a hidden file with a . prefix then rename/move once done
* read only when sure the file is fully written - either keep state of timestamp and file size bytes or use mtime/ctime
  * handle weird / negative time differences between reading and writing?
  * needs a timeout after last byte is written before we read the file?
    * or just yolo for reading, ignore/skip errors, and use this timeout only to delete invalid files?
* maybe add error correction?

"""
import datetime
import hashlib
import math
import shutil
import time
import uuid
from pathlib import Path
from typing import Optional
from typing import Union

# header will contain a hash and the data length
HEADER_SIZE = 64
HASH_ALGORITHM = hashlib.sha384
HASH_DIGEST_LENGTH = len(HASH_ALGORITHM(b'\0').digest())
MAX_SIZE = 128 * 1024 * 1024  # 128 MiB == 134217728 bytes, 9 ascii characters
assert HEADER_SIZE >= HASH_DIGEST_LENGTH + math.log10(MAX_SIZE)


class BinaryWriter:
    def __init__(self, path, temp_file_name=None, overwrite=False):
        self._path = Path(path)
        if self._path.is_dir():
            raise IsADirectoryError(path)
        elif self._path.exists() and not overwrite:
            raise FileExistsError(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        self._temp_file_name = temp_file_name or f'.{datetime.date.today().isoformat()}.{uuid.uuid4()}.partial'
        self._temp_file_path = self._path.parent / self._temp_file_name
        if self._temp_file_path.exists():
            raise FileExistsError(temp_file_name or self._temp_file_name)
        self._temp_file = None

        self._hash_object = HASH_ALGORITHM()
        self._len = 0

    def __enter__(self):
        if self._temp_file is None:
            self._temp_file = self._temp_file_path.open('xb')
        self._temp_file.write(b'\0' * HEADER_SIZE)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._temp_file is None:
            raise RuntimeError('temp file never opened')

        # write header
        if not self._temp_file.closed:
            self._temp_file.seek(0)
            self._temp_file.write(self._hash_object.digest())
            self._temp_file.write(str(self._len).encode('ascii'))
            self._temp_file.close()
            shutil.move(self._temp_file_path, self._path)

    def write(self, binary_data: Union[bytes, bytearray]):
        if self._temp_file is None:
            raise IOError('temp file not yet open for writing, please use `with` syntax')
        if not self._temp_file.writable():
            raise IOError(f'file already written and cannot be edited: {self._path}')
        self._len += len(binary_data)
        if self._len > MAX_SIZE - HEADER_SIZE:
            raise IOError(f'attempted to write too many bytes, max is {MAX_SIZE - HEADER_SIZE}')
        self._hash_object.update(binary_data)
        self._temp_file.write(binary_data)
        self._temp_file.flush()


class BinaryReader:
    def __init__(self, path):
        self._path = Path(path)
        if self._path.is_dir():
            raise IsADirectoryError(path)
        elif not self._path.exists():
            raise FileNotFoundError(path)
        self._file = None
        self._data = bytearray()
        self._header = bytearray()
        self._hash_object = HASH_ALGORITHM()

    def __enter__(self):
        self._file = self._path.open('rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file is None:
            raise RuntimeError('file never opened for reading')

        self._file.close()

    @property
    def _len(self):
        if len(self._header) < HEADER_SIZE:
            return None
        return int(self._header[HASH_DIGEST_LENGTH:].rstrip(b'\0').decode('ascii'))

    @property
    def _hexdigest(self):
        if len(self._header) < HEADER_SIZE:
            return None
        return self._header[:HASH_DIGEST_LENGTH]

    def is_complete(self):
        return (self._hexdigest, self._len) == (self._hash_object.digest(), len(self._data))

    def try_read(self) -> Optional[bytearray]:
        if self._file is None:
            raise IOError('file never opened for reading')
        while True:
            if len(self._header) < HEADER_SIZE:
                # read chunk into header buffer, then copy excess to data buffer
                self._header.extend(self._file.read(4 * 1024 * 1024))
                self._data.extend(self._header[HEADER_SIZE:])
                self._hash_object.update(self._header[HEADER_SIZE:])
                # truncate header buffer
                _header = self._header[:HEADER_SIZE]
                self._header.clear()
                self._header.extend(_header)
            elif len(self._data) < self._len:
                _chunk = self._file.read(4 * 1024 * 1024)
                if not _chunk:  # unable to read any data, file still incomplete
                    break  # can retry later to read the rest of the file
                self._data.extend(_chunk)
                self._hash_object.update(_chunk)
            elif len(self._data) > self._len:
                raise IOError(f'data len {len(self._data)} greater than expected len {self._len}')
            elif self.is_complete():
                break
            else:  # correct data length but wrong hash, either data or header corrupted
                raise IOError('data corrupted and does not match hash')

        if self.is_complete():
            return self._data

    def size_bytes(self) -> int:
        return len(self._data)


if __name__ == '__main__':
    with BinaryWriter('test.bin', overwrite=True) as f:
        f.write(b'qwertyuiopasdfghjklzxcvbnm')
    with BinaryReader('test.bin') as f:
        for _ in range(10):
            print(f.try_read())
            print(f.size_bytes())
            if f.is_complete():
                break
            time.sleep(5)

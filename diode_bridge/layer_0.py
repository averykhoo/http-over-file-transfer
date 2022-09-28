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
import shutil
import time
import uuid
from pathlib import Path
from typing import Optional
from typing import Union


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

        self._sha256 = hashlib.sha256()

    def __enter__(self):
        if self._temp_file is None:
            self._temp_file = self._temp_file_path.open('xb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._temp_file is None:
            raise RuntimeError('temp file never opened')
        self.write(self._sha256.digest())
        if not self._temp_file.closed:
            self._temp_file.close()
        shutil.move(self._temp_file_path, self._path)

    def write(self, binary_data: Union[bytes, bytearray]):
        if self._temp_file is None:
            raise IOError('temp file not yet open for writing, please use `with` syntax')
        if not self._temp_file.writable():
            raise IOError(f'file already written and cannot be edited: {self._path}')
        self._sha256.update(binary_data)
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
        self._digest = bytearray()
        self._sha256 = hashlib.sha256()

    def __enter__(self):
        self._file = self._path.open('rb')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file is None:
            raise RuntimeError('file never opened for reading')

        self._file.close()

    def is_complete(self):
        return self._sha256.digest() == bytes(self._digest)

    def try_read(self) -> Optional[bytearray]:
        if self._file is None:
            raise IOError('file never opened for reading')
        while True:
            self._digest.extend(self._file.read(4 * 1024 * 1024))
            print(self._digest)
            if len(self._digest) > 32:
                self._sha256.update(self._digest[:-32])
                self._data.extend(self._digest[:-32])
                self._digest = self._digest[-32:]
            else:
                break
        if self.is_complete():
            return self._data

    def size_bytes(self) -> int:
        return len(self._data)


if __name__ == '__main__':
    # with BinaryWriter('test.bin') as f:
    #     f.write(b'asdf')
    with BinaryReader('test.bin') as f:
        for _ in range(10):
            print(f.try_read())
            print(f.size_bytes())
            if f.is_complete():
                break
            time.sleep((5))

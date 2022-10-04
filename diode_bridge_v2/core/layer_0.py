import gzip
import time
import warnings
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Optional

from diode_bridge_v2.utils import coerce


@dataclass
class BinaryReader:
    path: Path
    _raw_reader: Optional = field(default=None)
    _gzip_reader: Optional[gzip.GzipFile] = field(default=None)
    _prev_time_monotonic: float = field(default_factory=time.monotonic)
    _prev_size_bytes: int = field(default=0)
    _expected_total_size: int = field(default=0)

    def __post_init__(self):
        self.path = Path(self.path)
        self._raw_reader = open(self.path, 'rb')
        self._expected_total_size = coerce.to_unsigned_integer32(self._raw_reader.read(4))
        if self._expected_total_size == 0:
            warnings.warn(f'corrupted packet transmitted: {self.path}')
        self._gzip_reader = gzip.GzipFile(mode='rb', fileobj=self._raw_reader)

    @property
    def is_ready_to_read(self, timeout_seconds=10):
        current_time = time.monotonic()
        current_size = self.path.stat().st_size

        # already correct
        if self._expected_total_size and current_size >= self._expected_total_size:
            return True

        # updated recently
        if current_size > self._prev_size_bytes:
            self._prev_time_monotonic = current_time
            self._prev_size_bytes = current_size
            return False

        # timeout waiting for write, assume ready
        if self._prev_time_monotonic + timeout_seconds < current_time:
            return True

        return False

    @property
    def closed(self):
        if self._raw_reader is not None:
            return self._raw_reader.closed
        return False

    def close(self, *, delete=False):
        if not self._gzip_reader.closed:
            self._gzip_reader.close()
        if not self._raw_reader.closed:
            self._raw_reader.close()
        if delete and self.path.exists():
            self.path.unlink()

    def read(self, size):
        return self._gzip_reader.read(size)


@dataclass
class BinaryWriter:
    path: Path
    _raw_writer: Optional = field(default=None)
    _gzip_writer: Optional[gzip.GzipFile] = field(default=None)

    def __post_init__(self):
        self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._raw_writer = open(self.path, 'wb')
        self._raw_writer.write(b'\0\0\0\0')
        self._gzip_writer = gzip.GzipFile(mode='wb', fileobj=self._raw_writer)

    @property
    def closed(self):
        if self._raw_writer is not None:
            return self._raw_writer.closed
        return False

    def close(self):
        if not self._gzip_writer.closed:
            self._gzip_writer.close()
        if not self._raw_writer.closed:
            size = self._raw_writer.tell()
            self._raw_writer.seek(0)
            self._raw_writer.write(coerce.from_unsigned_integer32(size))
            self._raw_writer.close()

    def write(self, data):
        self._gzip_writer.write(data)

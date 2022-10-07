import base64
import builtins
import io
import warnings
from typing import BinaryIO
from typing import Optional
from typing import Union

_E_SZ = 4  # (encoded size) number of base64 chars per chunk
_D_SZ = 3  # (decoded size) number of raw bytes per chunk


class Base64File(io.BufferedIOBase):
    """
    The GzipFile class simulates most of the methods of a file object.

    This class only supports opening files in binary mode.
    If you need to open a compressed file in text mode, use the gzip.open() function.
    """

    def __init__(self,
                 file_name: Optional[str] = None,
                 mode: Optional[str] = None,
                 file_obj: Optional[Union[io.BytesIO, BinaryIO]] = None,
                 alt_chars: Optional[str] = None,
                 ):
        """
        At least one of file_obj and file_name must be given a
        non-trivial value.

        The new class instance is based on file_obj, which can be a regular
        file, an io.BytesIO object, or any other object which simulates a file.
        It defaults to None, in which case file_name is opened to provide
        a file object.

        When file_obj is not None, the file_name argument is only used to be
        included in the gzip file header, which may include the original
        file_name of the uncompressed file.  It defaults to the file_name of
        file_obj, if discernible; otherwise, it defaults to the empty string,
        and in this case the original file_name is not included in the header.

        The mode argument can be any of 'r', 'rb', 'a', 'ab', 'w', 'wb', 'x', or
        'xb' depending on whether the file will be read or written.  The default
        is the mode of file_obj if discernible; otherwise, the default is 'rb'.
        A mode of 'r' is equivalent to one of 'rb', and similarly for 'w' and
        'wb', 'a' and 'ab', and 'x' and 'xb'.
        """

        # we need at least one of these
        if file_obj is None and file_name is None:
            raise ValueError('either file_name or file_obj must be specified')

        # file object provided
        if file_obj is not None:
            self._opened_file_obj = None
            if file_name is not None and file_name != getattr(file_obj, 'name', None):
                warnings.warn(f'specified file_name "{file_name}" will be ignored, and file_obj will be used as-is')

            # what is the current mode
            self.mode = getattr(file_obj, 'mode', mode)
            if mode is not None and mode != self.mode:
                warnings.warn(f'specified mode "{mode}" will be ignored, and file_obj will be used as-is')

        # file object has to be created from file_name and mode
        else:
            if mode is None:
                mode = 'rb'
            if mode and set(mode).issubset({'r', 'w', 'a', 'x', '+', 'b'}):
                if 'b' not in mode:
                    mode += 'b'
                    warnings.warn(f'base64_file only supports binary, changing mode to {mode}')
            else:
                raise ValueError(f'invalid mode "{mode}"')
            self.mode = mode

            # create file obj
            file_obj = self._opened_file_obj = builtins.open(file_name, mode or 'rb')

        # keep track of file object
        self.file_obj = file_obj

        # allow reading/writing to happen from the middle of a file
        self.file_tell_offset = file_obj.tell()
        self._cursor = 0  # base64 bytes
        self._buffer = bytearray()
        self._buffer_cursor = 0

        # flags
        self._data_not_written_flag = False

        # base64 args
        self.alt_chars = alt_chars

    def __repr__(self):
        s = repr(self.file_obj)
        return '<base64 ' + s[1:-1] + ' ' + hex(id(self)) + '>'

    @property
    def tell(self):
        return self._cursor

    @property
    def closed(self):
        return self.file_obj is None or self.file_obj.closed

    def readable(self):
        return self.file_obj and self.file_obj.readable()

    def writable(self):
        return self.file_obj and self.file_obj.writable()

    def seekable(self):
        return self.file_obj and self.file_obj.seekable()

    def _ensure_not_closed(self):
        if self.file_obj is None:
            raise ValueError("write() on closed GzipFile object")
        if self.file_obj.closed:
            raise ValueError('I/O operation on closed file')

    def _ensure_readable(self):
        if not self.readable():
            raise io.UnsupportedOperation('File not open for reading')

    def _ensure_writable(self):
        if not self.writable():
            raise io.UnsupportedOperation('File not open for writing')

    def _ensure_seekable(self):
        if not self.seekable():
            raise io.UnsupportedOperation('File does not support seeking')

    def write(self, data: Union[bytes, bytearray]) -> int:
        self._ensure_not_closed()
        self._ensure_writable()

        assert 0 <= self._buffer_cursor <= len(self._buffer) <= _D_SZ and self._buffer_cursor < _D_SZ

        # check expected file position
        _expected_tell = self.file_tell_offset + _E_SZ * ((self._cursor - self._buffer_cursor) // _D_SZ)
        if _expected_tell != self.file_obj.tell():  # maybe we read ahead a bit
            self._ensure_seekable()
            self.file_obj.seek(_expected_tell)

        # insert into buffer, possibly only inserting something in the middle
        _tmp, self._buffer = self._buffer, self._buffer[:self._buffer_cursor]
        self._buffer.extend(data)
        if len(_tmp) > len(self._buffer):
            self._buffer.extend(_tmp[len(self._buffer):])
        self._buffer_cursor += len(data)
        self._cursor += len(data)
        _tmp.clear()

        _num_writable_bytes = _D_SZ * (self._buffer_cursor // _D_SZ)
        self.file_obj.write(base64.b64encode(self._buffer[:_num_writable_bytes], altchars=self.alt_chars))

        self._buffer, _tmp = self._buffer[_num_writable_bytes:], self._buffer
        self._buffer_cursor -= _num_writable_bytes
        _tmp.clear()
        self._data_not_written_flag = self._buffer_cursor != 0

        # sanity check
        assert (self._cursor - self._buffer_cursor) % _D_SZ == 0  # we've written a whole number of chunks
        assert self.file_tell_offset + _E_SZ * ((self._cursor - self._buffer_cursor) // _D_SZ) == self.file_obj.tell()
        assert 0 <= self._buffer_cursor <= len(self._buffer) <= _D_SZ and self._buffer_cursor < _D_SZ

        return len(data)

    def read(self, size=-1) -> bytes:
        self._ensure_not_closed()
        self._ensure_readable()  # cannot read if writing

        assert 0 <= self._buffer_cursor <= len(self._buffer) <= _D_SZ and self._buffer_cursor < _D_SZ

        # no-op
        if size == 0:
            return b''

        # check expected file position
        if self._data_not_written_flag:
            self._ensure_seekable()
            _expected_write_tell = self.file_tell_offset + _E_SZ * ((self._cursor - self._buffer_cursor) // _D_SZ)
            assert _expected_write_tell == self.file_obj.tell()
            assert 0 <= self._buffer_cursor < _D_SZ
            self._ensure_seekable()

            # read one chunk of data, appending to existing unwritten bytes
            if len(self._buffer) < _D_SZ:
                _tmp, self._buffer = self._buffer, bytearray()
                self._buffer.extend(base64.b64decode(self.file_obj.read(_E_SZ), altchars=self.alt_chars, validate=True))
                self._buffer[:self._buffer_cursor] = _tmp[:self._buffer_cursor]

            # don't write anything (yet)
            if len(self._buffer) < _D_SZ or (size > 0 and self._buffer_cursor + size < _D_SZ):
                self.file_obj.seek(_expected_write_tell)  # return file cursor to where we should write from
                if size < 0:
                    size = len(self._buffer)
                out = self._buffer[self._buffer_cursor:self._buffer_cursor + size]
                self._buffer_cursor += len(out)
                self._cursor += len(out)
                assert 0 <= self._buffer_cursor <= len(self._buffer) <= _D_SZ and self._buffer_cursor < _D_SZ
                return bytes(out)

            # write one chunk of data
            assert len(self._buffer) == _D_SZ
            self.file_obj.seek(_expected_write_tell)
            self.file_obj.write(base64.b64encode(self._buffer[:_D_SZ], altchars=self.alt_chars))
            self._data_not_written_flag = False

        # prepare to read data
        assert len(self._buffer) in {0, _D_SZ}
        assert 0 <= self._buffer_cursor <= len(self._buffer) <= _D_SZ and self._buffer_cursor < _D_SZ
        assert (self._cursor - self._buffer_cursor) % _D_SZ == 0
        _expected_tell = self.file_tell_offset + _E_SZ * (self._cursor // _D_SZ)
        if len(self._buffer) > 0:
            _expected_tell += _E_SZ
        assert _expected_tell == self.file_obj.tell(), (_expected_tell, self.file_obj.tell())

        # figure out exactly how much to read
        if size > 0:
            _size_to_read = size - (len(self._buffer) - self._buffer_cursor)
            _file_size_to_read = _E_SZ * (_size_to_read // _D_SZ)
            if _size_to_read % _D_SZ > 0:
                _file_size_to_read += _E_SZ
        else:
            _file_size_to_read = -1

        # read data
        _bytes_read = self.file_obj.read(_file_size_to_read)
        assert len(_bytes_read) % _E_SZ == 0, (len(_bytes_read), _bytes_read)
        # if _file_size_to_read < 0 or len(_bytes_read) < _file_size_to_read:
        #     _bytes_read += b'===='
        self._buffer.extend(base64.b64decode(_bytes_read, altchars=self.alt_chars, validate=True))

        # how much to return
        if size > 0:
            out = bytes(self._buffer[self._buffer_cursor:self._buffer_cursor + size])
        else:
            out = bytes(self._buffer[self._buffer_cursor:])
        self._buffer_cursor += len(out)
        self._cursor += len(out)

        # clear read buffer
        _tmp, self._buffer = self._buffer, bytearray()
        _num_bytes_to_clear = _D_SZ * (self._buffer_cursor // _D_SZ)
        self._buffer.extend(_tmp[_num_bytes_to_clear:])
        _tmp.clear()
        self._buffer_cursor -= _num_bytes_to_clear

        assert 0 <= self._buffer_cursor <= len(self._buffer) <= _D_SZ and self._buffer_cursor < _D_SZ
        return out

    def close(self):
        # ensure data is written
        if self._data_not_written_flag:
            self.read(_D_SZ)

        # remove the file obj immediately
        file_obj, self.file_obj = self.file_obj, None
        if file_obj is None:
            return

        # really make sure
        if self._data_not_written_flag:
            file_obj.write(base64.b64encode(self._buffer, altchars=self.alt_chars))

        # close my_file_obj if we opened it via file_name in __init__
        if self._opened_file_obj is not None:
            self._opened_file_obj.close()
            self._opened_file_obj = None

    def flush(self):
        self._ensure_not_closed()
        self.file_obj.flush()

    # noinspection SpellCheckingInspection
    def fileno(self):
        return self.file_obj.fileno()

    def seek(self, offset, whence=io.SEEK_SET):
        self._ensure_not_closed()
        self._ensure_seekable()
        self._ensure_readable()

        # ensure data is written
        if self._data_not_written_flag:
            self.read(_D_SZ)

        # write to end of file
        if self._data_not_written_flag:
            self.file_obj.write(base64.b64encode(self._buffer, altchars=self.alt_chars))

        if whence == io.SEEK_SET:
            if offset < 0:
                raise IOError('unable to seek to negative file size')
            self._cursor = _D_SZ * (offset // _D_SZ)
            _offset = _E_SZ * (offset // _D_SZ)

            self.file_obj.seek(_offset + self.file_tell_offset)
            self._buffer.clear()
            self._buffer_cursor = 0

            if offset % _D_SZ:
                self.read(offset % _D_SZ)

        elif whence == io.SEEK_CUR:
            if self._cursor + offset < 0:
                raise IOError(f'unable to seek back that far, current tell is {self._cursor}')
            self.seek(self._cursor + offset)

        elif whence == io.SEEK_END:
            _tmp = self.file_obj.tell()
            self.file_obj.seek(0, io.SEEK_END)
            _len = (self.file_obj.tell() - self.file_tell_offset)
            self.file_obj.seek(self.file_tell_offset + _E_SZ * (_len // _E_SZ))
            self._cursor = _D_SZ * (_len // _E_SZ)
            self.read()

        return self._cursor

    def readline(self, size=-1):
        raise NotImplementedError


if __name__ == '__main__':
    with open('tmp.txt', 'rb+') as f:
        print(f.write(b'\1\2'))
        print(f.write(b'\3'))
        bf = Base64File(file_obj=f)
        bf.write(b'01234567890123456789')
        bf.seek(1)
        print(bf.read(1))
        bf.write(b'qwert')
        bf.close()
    with open('tmp.txt', 'rb') as f:
        f.seek(3)
        bf = Base64File(file_obj=f)
        print(bf.read())
        bf.close()

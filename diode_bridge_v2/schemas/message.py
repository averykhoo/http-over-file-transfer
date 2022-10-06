import hashlib
import json
from enum import IntEnum
from io import BytesIO
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import conint
from pydantic import constr

from diode_bridge_v2.core.layer_0 import BinaryReader
from diode_bridge_v2.utils import coerce

# set the max based on signed integers since that's probably sufficient and makes life easier
MAX_INT_32 = 2 ** 31 - 1  # max 32-bit signed integer == 2_147_483_647

MESSAGE_DIGEST_SIZE = 16


class ContentType(IntEnum):
    STRING = 1
    BINARY = 2
    JSON_DICT = 3  # not lists or primitives
    MULTIPART_FRAGMENT = 4  # indicates data that requires other data


MESSAGE_HEADER_SIZE = 4 + 4 + 4 + 2 + MESSAGE_DIGEST_SIZE


class MessageHeader(BaseModel):
    message_id: conint(ge=1, le=MAX_INT_32)
    message_prev: conint(ge=0, le=MAX_INT_32)  # support for multipart messages, 0 = null

    content_length: conint(ge=0, le=MAX_INT_32)  # about 2gb max content length
    content_type: ContentType  # 2 bytes
    content_hash: constr(regex=rf'[0-9a-f]{{{MESSAGE_DIGEST_SIZE * 2}}}')  # lowercase hash

    # todo: header hash

    @property
    def size_bytes(self):
        return MESSAGE_HEADER_SIZE

    def __bytes__(self) -> bytes:
        # typecasting for the type checker
        # noinspection PyTypeChecker
        _content_type_int: int = self.content_type.value

        out = bytearray()
        out.extend(coerce.from_unsigned_integer32(self.message_id))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.message_prev))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.content_length))  # 4 bytes
        out.extend(coerce.from_unsigned_integer16(_content_type_int))  # 2 bytes
        out.extend(coerce.from_hex(self.content_hash))  # MESSAGE_DIGEST_SIZE bytes
        assert len(out) == self.size_bytes
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data):
        # validate length
        if len(binary_data) != MESSAGE_HEADER_SIZE:
            raise ValueError('incorrect length of bytes input')

        # create message header
        return MessageHeader(message_id=coerce.to_unsigned_integer32(binary_data[0:4]),
                             message_prev=coerce.to_unsigned_integer32(binary_data[4:8]),
                             content_length=coerce.to_unsigned_integer32(binary_data[8:12]),
                             content_type=ContentType(coerce.to_unsigned_integer16(binary_data[12:14])),
                             content_hash=coerce.to_hex(binary_data[14:14 + MESSAGE_DIGEST_SIZE]))

    @classmethod
    def from_file(cls, file_io: Union[BytesIO, BinaryReader]):
        return MessageHeader.from_bytes(file_io.read(MESSAGE_HEADER_SIZE))


class Message(BaseModel):
    header: MessageHeader
    binary_data: bytes
    previous_message: Optional['Message'] = Field(default=None)

    @property
    def multipart_binary_data(self) -> Optional[bytearray]:
        if self.header.message_prev:
            if self.previous_message is None:
                out = None
            else:
                assert self.previous_message.header.message_id == self.header.message_prev
                out = self.previous_message.multipart_binary_data

        else:
            out = bytearray()

        # if we have all previous data, return it with this message's data
        if out is not None:
            out.extend(self.binary_data)
            return out

    @property
    def size_bytes(self):
        return self.header.size_bytes + len(self.binary_data)

    @property
    def multipart_size_bytes(self):
        if not self.header.message_prev:
            return self.size_bytes

        assert self.previous_message is not None
        assert self.previous_message.header.message_id == self.header.message_prev
        return self.size_bytes + self.previous_message.multipart_size_bytes

    @property
    def content(self):
        _data = self.multipart_binary_data
        if _data is None:
            return None

        if self.header.content_type is ContentType.STRING:
            return coerce.to_string(_data)
        elif self.header.content_type is ContentType.BINARY:
            return _data
        elif self.header.content_type is ContentType.JSON_DICT:
            return json.loads(coerce.to_string(_data))
        elif self.header.content_type is ContentType.MULTIPART_FRAGMENT:
            return ...
        else:
            raise NotImplementedError

    def __bytes__(self):
        out = bytes(self.header) + self.binary_data
        assert len(out) == self.size_bytes
        return out

    @classmethod
    def from_file(cls, file_io: Union[BytesIO, BinaryReader]):
        _header = MessageHeader.from_file(file_io)
        out = Message(header=_header, binary_data=file_io.read(_header.content_length))
        if hashlib.blake2b(out.binary_data, digest_size=MESSAGE_DIGEST_SIZE).hexdigest() != out.header.content_hash:
            raise ValueError('mismatched hash')
        return out

    @classmethod
    def from_bytes(cls, binary_data: bytes):
        file_io = BytesIO(binary_data)
        out = Message.from_file(file_io)
        if file_io.read(1):
            raise ValueError('extra unexpected data')
        return out

    @classmethod
    def from_content(cls, message_id: int, content: Union[str, bytes], previous_message_id: int = 0):
        if isinstance(content, str):
            data = coerce.from_string(content)
            content_type = ContentType.STRING
        elif isinstance(content, bytes):
            data = content
            content_type = ContentType.BINARY
        elif isinstance(content, dict):
            data = coerce.from_string(json.dumps(content))
            content_type = ContentType.JSON_DICT
        elif isinstance(content, BaseModel):
            data = coerce.from_string(content.json())
            content_type = ContentType.JSON_DICT
        else:
            raise TypeError

        content_hash = hashlib.blake2b(data, digest_size=MESSAGE_DIGEST_SIZE).hexdigest()

        return Message(header=MessageHeader(message_id=message_id,
                                            message_prev=previous_message_id,
                                            content_length=len(data),
                                            content_type=content_type,
                                            content_hash=content_hash),
                       binary_data=data)

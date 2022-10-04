import hashlib
import json
from enum import IntEnum
from io import BytesIO
from typing import Dict
from typing import Union

from pydantic import BaseModel
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


MESSAGE_HEADER_SIZE = 4 + 4 + 2 + MESSAGE_DIGEST_SIZE


class MessageHeader(BaseModel):
    message_id: conint(ge=1, le=MAX_INT_32)
    message_next: conint(ge=0, le=MAX_INT_32)  # todo: support for multipart messages, 0 = null
    message_prev: conint(ge=0, le=MAX_INT_32)  # todo: support for multipart messages, 0 = null

    content_length: conint(ge=0, le=MAX_INT_32)  # about 2gb max content length
    content_type: ContentType  # 2 bytes
    content_hash: constr(regex=rf'[0-9a-f]{{{MESSAGE_DIGEST_SIZE * 2}}}')  # lowercase hash

    @property
    def size_bytes(self):
        return MESSAGE_HEADER_SIZE

    def __bytes__(self) -> bytes:
        # typecasting for the type checker
        # noinspection PyTypeChecker
        _content_type_int: int = self.content_type.value

        out = bytearray()
        out.extend(coerce.from_unsigned_integer32(self.message_id))  # 4 bytes
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
                             content_length=coerce.to_unsigned_integer32(binary_data[4:8]),
                             content_type=ContentType(coerce.to_unsigned_integer16(binary_data[8:10])),
                             content_hash=coerce.to_hex(binary_data[10:10 + MESSAGE_DIGEST_SIZE]))

    @classmethod
    def from_file(cls, file_io: Union[BytesIO, BinaryReader]):
        return MessageHeader.from_bytes(file_io.read(MESSAGE_HEADER_SIZE))


class Message(BaseModel):
    header: MessageHeader
    binary_data: bytes

    @property
    def size_bytes(self):
        return self.header.size_bytes + len(self.binary_data)

    @property
    def content(self):
        if self.header.content_type is ContentType.STRING:
            return coerce.to_string(self.binary_data)
        elif self.header.content_type is ContentType.BINARY:
            return self.binary_data
        elif self.header.content_type is ContentType.JSON_DICT:
            return json.loads(coerce.to_string(self.binary_data))
        else:
            raise NotImplementedError

    def multipart_content(self, *messages: 'Message'):
        _messages: Dict[int, Message] = {message.header.message_id: message for message in messages}

        # append previous messages in reverse order
        _all_messages = []
        _prev_message_id = self.header.message_prev
        while _prev_message_id:
            if _prev_message_id not in _messages:
                raise KeyError(f'message {_prev_message_id} not provided')
            _all_messages.append(_messages[_prev_message_id])
            _prev_message_id = _messages[_prev_message_id].header.message_prev

        # reverse the list (so it's now in the right order)  and append this message
        _all_messages.reverse()
        _all_messages.append(self)

        # get the remaining messages
        _next_message_id = self.header.message_next
        while _next_message_id:
            if _next_message_id not in _messages:
                raise KeyError(f'message {_next_message_id} not provided')
            _all_messages.append(_messages[_next_message_id])
            _next_message_id = _messages[_next_message_id].header.message_next

        # concatenate the message data
        data = bytearray()
        for message in _all_messages:
            data.extend(message.binary_data)

        # reformat and return
        if self.header.content_type is ContentType.STRING:
            return coerce.to_string(data)
        elif self.header.content_type is ContentType.BINARY:
            return data
        elif self.header.content_type is ContentType.JSON_DICT:
            return json.loads(coerce.to_string(data))
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
    def from_content(cls, message_id: int, content: Union[str, bytes]):
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
                                            content_length=len(data),
                                            content_type=content_type,
                                            content_hash=content_hash),
                       binary_data=data)

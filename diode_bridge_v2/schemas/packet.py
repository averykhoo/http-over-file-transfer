import datetime
import hashlib
import random
from enum import IntEnum
from typing import List
from typing import Optional
from typing import Union
from uuid import UUID
from uuid import uuid4

from pydantic import BaseModel
from pydantic import Field
from pydantic import conint
from pydantic import constr

from diode_bridge_v2.utils import coerce

# set the max based on signed integers since that's probably sufficient and makes life easier
MAX_INT_32 = 2 ** 31 - 1  # max 32-bit signed integer == 2_147_483_647

HEADER_DIGEST_SIZE = 8
MESSAGE_DIGEST_SIZE = 16


def get_utc_timestamp():
    return datetime.datetime.utcnow().replace(microsecond=0, tzinfo=datetime.timezone.utc)


class PacketHeader(BaseModel):
    sender_uuid: UUID
    recipient_uuid: UUID
    packet_id: conint(ge=1, le=MAX_INT_32)
    num_messages: conint(ge=0, le=MAX_INT_32)
    packet_timestamp: datetime.datetime = Field(default_factory=get_utc_timestamp)
    protocol_version: conint(ge=1, le=MAX_INT_32) = 2

    def __bytes__(self) -> bytes:
        out = bytearray()
        out.extend(coerce.from_uuid(self.sender_uuid))  # 16 bytes
        out.extend(coerce.from_uuid(self.recipient_uuid))  # 16 bytes
        out.extend(coerce.from_unsigned_integer32(self.packet_id))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.num_messages))  # 4 bytes
        out.extend(coerce.from_datetime32(self.packet_timestamp))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.protocol_version))  # 4 bytes
        out.extend(hashlib.blake2b(out, digest_size=HEADER_DIGEST_SIZE).digest())
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data):
        # validate length
        if len(binary_data) != 16 + 16 + 4 + 4 + 4 + 4 + HEADER_DIGEST_SIZE:
            raise ValueError('incorrect length of bytes input')

        # validate hash
        _expected_hash = binary_data[-HEADER_DIGEST_SIZE:]
        _actual_hash = hashlib.blake2b(binary_data[:-HEADER_DIGEST_SIZE], digest_size=HEADER_DIGEST_SIZE).digest()
        if _actual_hash != _expected_hash:
            raise ValueError('incorrect hash')

        # create packet header
        return PacketHeader(sender_uuid=coerce.to_uuid(binary_data[0:16]),
                            recipient_uuid=coerce.to_uuid(binary_data[16:32]),
                            packet_id=coerce.to_unsigned_integer32(binary_data[32:36]),
                            num_messages=coerce.to_unsigned_integer32(binary_data[36:40]),
                            packet_timestamp=coerce.to_datetime32(binary_data[40:44]),
                            protocol_version=coerce.to_unsigned_integer32(binary_data[44:48]))


class ContentType(IntEnum):
    STRING = 1
    BINARY = 2


class MessageHeader(BaseModel):
    message_id: conint(ge=1, le=MAX_INT_32)
    content_length: conint(ge=0, le=MAX_INT_32)  # about 2gb max content length
    content_type: ContentType  # 2 bytes
    content_hash: constr(regex=rf'[0-9a-f]{{{MESSAGE_DIGEST_SIZE * 2}}}')  # lowercase hash

    def __bytes__(self) -> bytes:
        # typecasting for the type checker
        # noinspection PyTypeChecker
        _content_type_int: int = self.content_type.value

        out = bytearray()
        out.extend(coerce.from_unsigned_integer32(self.message_id))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.content_length))  # 4 bytes
        out.extend(coerce.from_unsigned_integer16(_content_type_int))  # 2 bytes
        out.extend(coerce.from_hex(self.content_hash))  # MESSAGE_DIGEST_SIZE bytes
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data):
        # validate length
        if len(binary_data) != 4 + 4 + 2 + MESSAGE_DIGEST_SIZE:
            raise ValueError('incorrect length of bytes input')

        # create message header
        return MessageHeader(message_id=coerce.to_unsigned_integer32(binary_data[0:4]),
                             content_length=coerce.to_unsigned_integer32(binary_data[4:8]),
                             content_type=ContentType(coerce.to_unsigned_integer16(binary_data[8:10])),
                             content_hash=coerce.to_hex(binary_data[10:10 + MESSAGE_DIGEST_SIZE]))

    @classmethod
    def read_bytes(cls, binary_data, cursor):
        out = MessageHeader.from_bytes(binary_data[cursor:cursor + 4 + 4 + 2 + MESSAGE_DIGEST_SIZE])
        cursor += 4 + 4 + 2 + MESSAGE_DIGEST_SIZE
        return out, cursor


class Message(BaseModel):
    header: MessageHeader
    binary_data: bytes

    @property
    def content(self):
        if self.header.content_type is ContentType.STRING:
            return coerce.to_string(self.binary_data)
        elif self.header.content_type is ContentType.BINARY:
            return self.binary_data
        else:
            raise NotImplementedError

    def __bytes__(self):
        return bytes(self.header) + self.binary_data

    @classmethod
    def from_bytes(cls, binary_data):
        _header_size = 4 + 4 + 2 + MESSAGE_DIGEST_SIZE
        _header = MessageHeader.from_bytes(binary_data[:_header_size])
        assert len(binary_data) == _header_size + _header.content_length

        out = Message(header=_header, binary_data=binary_data[_header_size:])
        if hashlib.blake2b(out.binary_data, digest_size=MESSAGE_DIGEST_SIZE).hexdigest() != out.header.content_hash:
            raise ValueError('mismatched hash')

        return out

    @classmethod
    def from_content(cls, message_id: int, content: Union[str, bytes]):
        if isinstance(content, str):
            data = coerce.from_string(content)
            content_type = ContentType.STRING
        elif isinstance(content, bytes):
            data = content
            content_type = ContentType.BINARY
        else:
            raise TypeError

        content_hash = hashlib.blake2b(data, digest_size=MESSAGE_DIGEST_SIZE).hexdigest()

        return Message(header=MessageHeader(message_id=message_id,
                                            content_length=len(data),
                                            content_type=content_type,
                                            content_hash=content_hash),
                       binary_data=data)


class Control(BaseModel):
    sender_clock_sender: conint(ge=0, le=MAX_INT_32)
    sender_clock_recipient: conint(ge=0, le=MAX_INT_32)
    sender_clock_out_of_order: List[conint(ge=1, le=MAX_INT_32)]  # list of message ids
    nack_ids: List[conint(ge=1, le=MAX_INT_32)]  # list of packet ids
    recipient_clock_sender: conint(ge=0, le=MAX_INT_32)

    def __bytes__(self):
        out = bytearray()
        out.extend(coerce.from_unsigned_integer32(self.sender_clock_sender))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.sender_clock_recipient))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(len(self.sender_clock_out_of_order)))  # 4 bytes
        for _sack in self.sender_clock_out_of_order:
            out.extend(coerce.from_unsigned_integer32(_sack))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(len(self.nack_ids)))  # 4 bytes
        for _nack in self.nack_ids:
            out.extend(coerce.from_unsigned_integer32(_nack))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.recipient_clock_sender))  # 4 bytes
        out.extend(hashlib.blake2b(out, digest_size=HEADER_DIGEST_SIZE).digest())
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data: bytes):

        # validate hash
        _expected_hash = binary_data[-HEADER_DIGEST_SIZE:]
        _actual_hash = hashlib.blake2b(binary_data[:-HEADER_DIGEST_SIZE], digest_size=HEADER_DIGEST_SIZE).digest()
        if _actual_hash != _expected_hash:
            raise ValueError('incorrect hash')

        # parse uint32
        cursor = 0
        _sender_clock_sender = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # parse uint32
        _sender_clock_recipient = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # parse uint32
        _n_sacks = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # parse list of uint32
        _sender_clock_out_of_order = []
        for _ in range(_n_sacks):
            _sender_clock_out_of_order.append(coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4]))
            cursor += 4

        # parse uint32
        _n_nacks = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # parse list of packet id
        _nack_ids = []
        for _ in range(_n_nacks):
            _nack_ids.append(coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4]))
            cursor += 4

        # parse uint32
        _recipient_clock_sender = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # validate header length
        assert cursor + HEADER_DIGEST_SIZE == len(binary_data)

        # create object
        return Control(sender_clock_sender=_sender_clock_sender,
                       sender_clock_recipient=_sender_clock_recipient,
                       sender_clock_out_of_order=_sender_clock_out_of_order,
                       nack_ids=_nack_ids,
                       recipient_clock_sender=_recipient_clock_sender)


class Packet(BaseModel):
    header: PacketHeader
    control: Optional[Control]
    messages: List[Message]

    def __bytes__(self):
        assert len(self.messages) == self.header.num_messages
        assert self.control is not None

        out = bytearray()
        out.extend(bytes(self.header))
        out.extend(bytes(self.control))
        for _msg in self.messages:
            out.extend(bytes(_msg))
        return bytes(out)


if __name__ == '__main__':
    _ph = PacketHeader(sender_uuid=uuid4(),
                       recipient_uuid=uuid4(),
                       packet_id=random.randint(1, MAX_INT_32),
                       num_messages=random.randint(0, MAX_INT_32))
    print(_ph)
    assert PacketHeader.from_bytes(bytes(_ph)) == _ph

    _mh = MessageHeader(message_id=random.randint(1, MAX_INT_32),
                        content_length=random.randint(1, MAX_INT_32),
                        content_type=ContentType.STRING,
                        content_hash='0123456789abcdef' * 2)
    print(_mh)
    assert MessageHeader.from_bytes(bytes(_mh)) == _mh

    _m = Message.from_content(random.randint(1, MAX_INT_32),
                              content=str(random.randint(10000000000000000000000000, 100000000000000000000000000 - 1)))
    print(_m)
    assert Message.from_bytes(bytes(_m)) == _m

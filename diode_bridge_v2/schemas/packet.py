import hashlib
from enum import IntEnum
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import conint
from pydantic import constr

from diode_bridge_v2.utils import coerce

# set the max based on signed integers since that's probably sufficient and makes life easier
MAX_INT_64 = 2 ** 63 - 1  # max 64-bit signed integer == 9_223_372_036_854_775_807
MAX_INT_32 = 2 ** 31 - 1  # max 32-bit signed integer == 2_147_483_647

HEADER_DIGEST_SIZE = 8


class PacketHeader(BaseModel):
    sender_uuid: UUID
    recipient_uuid: UUID
    packet_uuid: UUID
    num_messages: conint(ge=0, le=MAX_INT_32)
    protocol_version: conint(ge=1, le=MAX_INT_32) = 2

    def __bytes__(self) -> bytes:
        out = bytearray()
        out.extend(coerce.from_uuid(self.sender_uuid))  # 16 bytes
        out.extend(coerce.from_uuid(self.recipient_uuid))  # 16 bytes
        out.extend(coerce.from_uuid(self.packet_uuid))  # 16 bytes
        out.extend(coerce.from_unsigned_integer32(self.num_messages))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.protocol_version))  # 4 bytes
        out.extend(hashlib.blake2b(out, digest_size=HEADER_DIGEST_SIZE).digest())
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data):
        # validate length
        if len(binary_data) != 16 + 16 + 16 + 4 + 4 + HEADER_DIGEST_SIZE:
            raise ValueError('incorrect length of bytes input')

        # validate hash
        _expected_hash = binary_data[-HEADER_DIGEST_SIZE:]
        _actual_hash = hashlib.blake2b(binary_data[:-HEADER_DIGEST_SIZE], digest_size=HEADER_DIGEST_SIZE).digest()
        if _actual_hash != _expected_hash:
            raise ValueError('incorrect hash')

        # create packet header
        return PacketHeader(sender_uuid=coerce.to_uuid(binary_data[0:16]),
                            recipient_uuid=coerce.to_uuid(binary_data[16:32]),
                            packet_uuid=coerce.to_uuid(binary_data[32:48]),
                            num_messages=coerce.to_unsigned_integer32(binary_data[48:52]),
                            protocol_version=coerce.to_unsigned_integer32(binary_data[52:56]))


class ContentType(IntEnum):
    STRING = 1
    BINARY = 2


class MessageHeader(BaseModel):
    message_id: conint(ge=1, le=MAX_INT_64)
    content_length: conint(ge=0, le=MAX_INT_32)  # about 2gb max content length
    content_hash: constr(regex=r'[0-9a-f]{64}')  # lowercase hash for sha256
    content_type: ContentType  # 2 bytes

    def __bytes__(self) -> bytes:
        out = bytearray()
        out.extend(coerce.from_unsigned_integer64(self.message_id))  # 8 bytes
        out.extend(coerce.from_unsigned_integer32(self.content_length))  # 4 bytes
        out.extend(coerce.from_hex(self.content_hash))  # 32 bytes
        out.extend(coerce.from_unsigned_integer16(self.content_type.value()))  # 2 bytes
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data):
        # validate length
        if len(binary_data) != 8 + 4 + 32 + 2:
            raise ValueError('incorrect length of bytes input')

        # create message header
        return MessageHeader(message_id=coerce.to_unsigned_integer64(binary_data[0:8]),
                             content_length=coerce.to_unsigned_integer32(binary_data[8:12]),
                             content_hash=coerce.to_hex(binary_data[12:44]),
                             content_type=ContentType(coerce.to_unsigned_integer16(binary_data[48:50])))


class Message(BaseModel):
    header: MessageHeader
    data: bytes

    def __bytes__(self):
        return bytes(self.header) + self.data

    @classmethod
    def from_bytes(cls, binary_data):
        _header_size = 8 + 4 + 32 + 2 + HEADER_DIGEST_SIZE
        return Message(header=MessageHeader.from_bytes(binary_data[:_header_size]),
                       data=binary_data[_header_size:])


class Control(BaseModel):
    sender_clock_sender: conint(ge=0, le=MAX_INT_64)
    sender_clock_recipient: conint(ge=0, le=MAX_INT_64)
    sender_clock_out_of_order: List[conint(ge=1, le=MAX_INT_64)]
    recipient_clock_sender: conint(ge=0, le=MAX_INT_64)
    nack_uuids: List[UUID]

    def __bytes__(self):
        out = bytearray()
        out.extend(coerce.from_unsigned_integer64(self.sender_clock_sender))  # 8 bytes
        out.extend(coerce.from_unsigned_integer64(self.sender_clock_recipient))  # 8 bytes
        out.extend(coerce.from_unsigned_integer32(len(self.sender_clock_out_of_order)))  # 4 bytes
        for _sack in self.sender_clock_out_of_order:
            out.extend(coerce.from_unsigned_integer64(_sack))  # 8 bytes
        out.extend(coerce.from_unsigned_integer64(self.recipient_clock_sender))  # 8 bytes
        out.extend(coerce.from_unsigned_integer32(len(self.nack_uuids)))  # 4 bytes
        for _nack in self.nack_uuids:
            out.extend(coerce.from_uuid(_nack))  # 16 bytes
        out.extend(hashlib.blake2b(out, digest_size=HEADER_DIGEST_SIZE).digest())
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data: bytes):

        # validate hash
        _expected_hash = binary_data[-HEADER_DIGEST_SIZE:]
        _actual_hash = hashlib.blake2b(binary_data[:-HEADER_DIGEST_SIZE], digest_size=HEADER_DIGEST_SIZE).digest()
        if _actual_hash != _expected_hash:
            raise ValueError('incorrect hash')

        # parse uint64
        cursor = 0
        _sender_clock_sender = coerce.to_unsigned_integer64(binary_data[cursor:cursor + 8])
        cursor += 8

        # parse uint64
        _sender_clock_recipient = coerce.to_unsigned_integer64(binary_data[cursor:cursor + 8])
        cursor += 8

        # parse uint32
        _n_sacks = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # parse list of uint64
        _sender_clock_out_of_order = []
        for _ in range(_n_sacks):
            _sender_clock_out_of_order.append(coerce.to_unsigned_integer64(binary_data[cursor:cursor + 8]))
            cursor += 8

        # parse uint64
        _recipient_clock_sender = coerce.to_unsigned_integer64(binary_data[cursor:cursor + 8])
        cursor += 8

        # parse uint32
        _n_nacks = coerce.to_unsigned_integer32(binary_data[cursor:cursor + 4])
        cursor += 4

        # parse list of uuid
        _nack_uuids = []
        for _ in range(_n_nacks):
            _nack_uuids.append(coerce.to_uuid(binary_data[cursor:cursor + 16]))
            cursor += 16

        # validate header length
        assert cursor + HEADER_DIGEST_SIZE == len(binary_data)

        # create object
        return Control(sender_clock_sender=_sender_clock_sender,
                       sender_clock_recipient=_sender_clock_recipient,
                       sender_clock_out_of_order=_sender_clock_out_of_order,
                       recipient_clock_sender=_recipient_clock_sender,
                       nack_uuids=_nack_uuids)


class Packet(BaseModel):
    header: PacketHeader
    messages: List[Message]
    control: Optional[Control]

    def __bytes__(self):
        assert len(self.messages) == self.header.num_messages
        assert self.control is not None

        out = bytearray()
        out.extend(bytes(self.header))
        for _msg in self.messages:
            out.extend(bytes(_msg))
        out.extend(bytes(self.control))
        return bytes(out)

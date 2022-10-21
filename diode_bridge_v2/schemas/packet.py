import datetime
import hashlib
import random
import warnings
from io import BytesIO
from typing import List
from typing import Optional
from typing import Union
from uuid import UUID
from uuid import uuid4

from pydantic import BaseModel
from pydantic import Field
from pydantic import conbytes
from pydantic import conint

from diode_bridge_v2.core.layer_0 import BinaryReader
from diode_bridge_v2.core.layer_0 import BinaryWriter
from diode_bridge_v2.schemas.message import Message
from diode_bridge_v2.utils import coerce
from diode_bridge_v2.utils.key_encapsulation import HASH_KEY_LEN
from diode_bridge_v2.utils.key_encapsulation import TOKEN_LEN
from diode_bridge_v2.utils.key_encapsulation import decrypt_key
from diode_bridge_v2.utils.key_encapsulation import encrypt_key
from diode_bridge_v2.utils.key_encapsulation import generate_hash_key
from diode_bridge_v2.utils.key_encapsulation import generate_secret_key

# set the max based on signed integers since that's probably sufficient and makes life easier
MAX_INT_32 = 2 ** 31 - 1  # max 32-bit signed integer == 2_147_483_647

HEADER_DIGEST_SIZE = 8


def get_utc_timestamp() -> datetime.datetime:
    return datetime.datetime.utcnow().replace(microsecond=0, tzinfo=datetime.timezone.utc)


PACKET_HEADER_SIZE = 16 + 16 + 4 + 4 + 4 + 4 + TOKEN_LEN + HEADER_DIGEST_SIZE


class PacketHeader(BaseModel):
    sender_uuid: UUID
    recipient_uuid: UUID
    packet_id: conint(ge=1, le=MAX_INT_32)
    num_messages: conint(ge=0, le=MAX_INT_32)
    packet_timestamp: datetime.datetime = Field(default_factory=get_utc_timestamp)
    protocol_version: conint(ge=1, le=MAX_INT_32) = 2
    hash_key: conbytes(min_length=HASH_KEY_LEN, max_length=HASH_KEY_LEN) = Field(default_factory=generate_hash_key)

    @property
    def size_bytes(self):
        return PACKET_HEADER_SIZE

    @property
    def filename(self):
        return f'{self.recipient_uuid}/{self.sender_uuid}--{self.recipient_uuid}--{self.packet_id}.packet'

    def to_bytes(self, secret_key: bytes) -> bytes:
        encapsulated_key = encrypt_key(self.hash_key, secret_key)

        out = bytearray()
        out.extend(coerce.from_uuid(self.sender_uuid))  # 16 bytes
        out.extend(coerce.from_uuid(self.recipient_uuid))  # 16 bytes
        out.extend(coerce.from_unsigned_integer32(self.packet_id))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.num_messages))  # 4 bytes
        out.extend(coerce.from_datetime32(self.packet_timestamp))  # 4 bytes
        out.extend(coerce.from_unsigned_integer32(self.protocol_version))  # 4 bytes
        out.extend(encapsulated_key)  # 140 bytes
        out.extend(hashlib.blake2b(out, key=self.hash_key, digest_size=HEADER_DIGEST_SIZE).digest())
        assert len(out) == self.size_bytes, (len(out), self.size_bytes)
        return bytes(out)

    @classmethod
    def from_bytes(cls, binary_data: bytes, secret_key: bytes):
        # validate length
        if len(binary_data) != PACKET_HEADER_SIZE:
            raise ValueError('incorrect length of bytes input')

        # get hash key
        encapsulated_key = binary_data[48:48 + TOKEN_LEN]
        hash_key = decrypt_key(encapsulated_key, secret_key)

        # validate hash
        _expected_hash = binary_data[-HEADER_DIGEST_SIZE:]
        _actual_hash = hashlib.blake2b(binary_data[:-HEADER_DIGEST_SIZE],
                                       key=hash_key,
                                       digest_size=HEADER_DIGEST_SIZE).digest()
        if _actual_hash != _expected_hash:
            raise ValueError('incorrect hash')

        # create packet header
        return PacketHeader(sender_uuid=coerce.to_uuid(binary_data[0:16]),
                            recipient_uuid=coerce.to_uuid(binary_data[16:32]),
                            packet_id=coerce.to_unsigned_integer32(binary_data[32:36]),
                            num_messages=coerce.to_unsigned_integer32(binary_data[36:40]),
                            packet_timestamp=coerce.to_datetime32(binary_data[40:44]),
                            protocol_version=coerce.to_unsigned_integer32(binary_data[44:48]),
                            hash_key=hash_key)

    @classmethod
    def from_file(cls, file_io: Union[BytesIO, BinaryReader], secret_key:bytes):
        return PacketHeader.from_bytes(file_io.read(PACKET_HEADER_SIZE), secret_key)


class Control(BaseModel):
    sender_clock_sender: conint(ge=0, le=MAX_INT_32)
    sender_clock_recipient: conint(ge=0, le=MAX_INT_32)
    sender_clock_out_of_order: List[conint(ge=1, le=MAX_INT_32)]  # list of message ids
    nack_ids: List[conint(ge=1, le=MAX_INT_32)]  # list of packet ids
    recipient_clock_sender: conint(ge=0, le=MAX_INT_32)

    @property
    def size_bytes(self):
        return 4 * (len(self.sender_clock_out_of_order) + len(self.nack_ids) + 5) + HEADER_DIGEST_SIZE

    def to_bytes(self):
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
        assert len(out) == self.size_bytes, (len(out), self.size_bytes)
        return bytes(out)

    @classmethod
    def from_file(cls, file_io: Union[BytesIO, BinaryReader]):
        _hash_object = hashlib.blake2b(digest_size=HEADER_DIGEST_SIZE)

        def read_word():
            nonlocal _hash_object
            data = file_io.read(4)
            _hash_object.update(data)
            return data

        _sender_clock_sender = coerce.to_unsigned_integer32(read_word())
        _sender_clock_recipient = coerce.to_unsigned_integer32(read_word())

        _n_sacks = coerce.to_unsigned_integer32(read_word())
        _sender_clock_out_of_order = []
        for _ in range(_n_sacks):
            _sender_clock_out_of_order.append(coerce.to_unsigned_integer32(read_word()))

        _n_nacks = coerce.to_unsigned_integer32(read_word())
        _nack_ids = []
        for _ in range(_n_nacks):
            _nack_ids.append(coerce.to_unsigned_integer32(read_word()))

        _recipient_clock_sender = coerce.to_unsigned_integer32(read_word())

        _actual_hash = _hash_object.digest()
        _expected_hash = file_io.read(HEADER_DIGEST_SIZE)
        assert _actual_hash == _expected_hash

        return Control(sender_clock_sender=_sender_clock_sender,
                       sender_clock_recipient=_sender_clock_recipient,
                       sender_clock_out_of_order=_sender_clock_out_of_order,
                       nack_ids=_nack_ids,
                       recipient_clock_sender=_recipient_clock_sender)

    @classmethod
    def from_bytes(cls, binary_data: bytes):
        file_io = BytesIO(binary_data)
        out = Control.from_file(file_io)
        if file_io.read(1):
            raise ValueError('extra unexpected data')
        return out


class Packet(BaseModel):
    header: PacketHeader
    control: Optional[Control]
    messages: List[Message]

    @property
    def size_bytes(self):
        return self.header.size_bytes + self.control.size_bytes + sum(msg.size_bytes for msg in self.messages)

    def to_bytes(self, secret_key:bytes):
        assert len(self.messages) == self.header.num_messages
        assert self.control is not None

        out = bytearray()
        out.extend(self.header.to_bytes(secret_key))
        out.extend(self.control.to_bytes())
        for _msg in self.messages:
            out.extend(_msg.to_bytes(self.header.hash_key))
        assert len(out) == self.size_bytes
        return bytes(out)

    @classmethod
    def from_file(cls, file_io: Union[BytesIO, BinaryReader], secret_key:bytes):

        out = Packet(header=PacketHeader.from_file(file_io, secret_key),
                     control=None,
                     messages=[])

        # noinspection PyBroadException
        try:
            out.control = Control.from_file(file_io)
        except Exception:
            warnings.warn('unable to parse control structure or messages')
            return out

        # noinspection PyBroadException
        try:
            for _i in range(out.header.num_messages):
                out.messages.append(Message.from_file(file_io, hash_key=out.header.hash_key))
        except Exception:
            warnings.warn(f'unable to parse message, got {len(out.messages)} of {out.header.num_messages}')
            return out

        assert out.header.num_messages == len(out.messages)
        return out

    @classmethod
    def from_bytes(cls, binary_data: bytes, secret_key: bytes):
        file_io = BytesIO(binary_data)
        out = Packet.from_file(file_io, secret_key)
        if file_io.read(1):
            raise ValueError('extra unexpected data')
        return out

    def to_file(self, file_io: Union[BytesIO, BinaryWriter], secret_key:bytes):
        assert len(self.messages) == self.header.num_messages
        assert self.control is not None

        file_io.write(self.header.to_bytes(secret_key))
        file_io.write(self.control.to_bytes())
        for _msg in self.messages:
            file_io.write(_msg.to_bytes(self.header.hash_key))


if __name__ == '__main__':
    from diode_bridge_v2.schemas.message import ContentType
    from diode_bridge_v2.schemas.message import MessageHeader

    hash_key = generate_hash_key()
    secret_key = generate_secret_key()

    _ph = PacketHeader(sender_uuid=uuid4(),
                       recipient_uuid=uuid4(),
                       packet_id=random.randint(1, MAX_INT_32),
                       num_messages=random.randint(0, MAX_INT_32))
    print(_ph)
    assert PacketHeader.from_bytes(_ph.to_bytes(secret_key), secret_key) == _ph

    _mh = MessageHeader(message_id=random.randint(1, MAX_INT_32),
                        message_prev=0,
                        content_length=random.randint(1, MAX_INT_32),
                        content_type=ContentType.STRING,
                        content_hash='0123456789abcdef' * 2)
    print(_mh)
    assert MessageHeader.from_bytes(_mh.to_bytes(hash_key), hash_key) == _mh

    _m = Message.from_content(random.randint(1, MAX_INT_32),
                              content=str(random.randint(10000000000000000000000000, 100000000000000000000000000 - 1)))
    print(_m)
    assert Message.from_bytes(_m.to_bytes(hash_key), hash_key) == _m

    _p = Packet(header=PacketHeader(sender_uuid=uuid4(),
                                    recipient_uuid=uuid4(),
                                    packet_id=123,
                                    num_messages=2),
                control=Control(sender_clock_sender=123,
                                sender_clock_recipient=432,
                                sender_clock_out_of_order=[201, 202, 203],
                                nack_ids=[123],
                                recipient_clock_sender=1234,
                                ),
                messages=[_m] * 2)
    print(_p)
    assert Packet.from_bytes(_p.to_bytes(secret_key), secret_key) == _p

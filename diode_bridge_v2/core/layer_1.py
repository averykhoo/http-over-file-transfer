"""
Layer 1: reliably synchronize a set of messages

todo: load/dump messenger class for persistence, or use an sqlite with 3 tables: metadata, inbox, outbox
"""
import datetime
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from dataclasses import field
from threading import Lock
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union
from uuid import UUID

from pydantic import BaseModel

from diode_bridge_v2.schemas.message import ContentType
from diode_bridge_v2.schemas.packet import Control
from diode_bridge_v2.schemas.packet import Message
from diode_bridge_v2.schemas.packet import PACKET_HEADER_SIZE
from diode_bridge_v2.schemas.packet import Packet
from diode_bridge_v2.schemas.packet import PacketHeader
from diode_bridge_v2.schemas.packet import get_utc_timestamp

MULTIPART_LIMIT_SIZE_BYTES = 20  # * 1024 * 1024  # 20 MiB, based on email attachment size because why not
PACKET_LIMIT_SIZE_BYTES = 200  # * 1024 * 1024  # 200 MiB, below recommended limit of 500MB to avoid truncation
NACK_TRANSMIT_COUNT = 5  # transmit nack at least this many times
RETRANSMISSION_TIMEOUT = datetime.timedelta(seconds=5)


@dataclass
class OutboxItem:
    message: Message
    packet_timestamp: Optional[datetime.datetime] = None
    packet_id: Optional[int] = None
    acked: Optional[datetime.datetime] = None  # this could be a bool


@dataclass
class InboxItem:
    message: Optional[Message] = None
    packet_timestamp: Optional[datetime.datetime] = None
    acked: Optional[datetime.datetime] = None  # this could be a bool
    ack_acked: Optional[datetime.datetime] = None  # this could be a bool


@dataclass
class Messenger:
    self_uuid: UUID
    other_uuid: UUID

    outbox: List[OutboxItem] = field(default_factory=list)  # outbox[i].message.header.message_id == i + 1
    _outbox_lock = Lock()

    inbox: List[InboxItem] = field(default_factory=list)  # inbox[i].message.header.message_id == i + 1
    _inbox_unlinked_previous_messages: Dict[int, int] = field(default_factory=dict)

    _cached_clock_self: int = field(default=0)
    _cached_clock_other: int = field(default=0)
    _cached_other_clock_self: int = field(default=0)

    nack_ids: Set[int] = field(default_factory=set)
    _sent_nack_ids: Counter = field(default_factory=Counter)

    num_sent_packets: int = field(default=0)

    @property
    def clock_self(self) -> int:
        for i in range(self._cached_clock_self, len(self.outbox)):
            assert self.outbox[i].message.header.message_id == i + 1, (i, self.outbox[i])
        self._cached_clock_self = len(self.outbox)
        return self._cached_clock_self

    @property
    def clock_other(self) -> int:
        for i in range(self._cached_clock_other, len(self.inbox)):
            if self.inbox[i].message:
                assert self.inbox[i].message.header.message_id == i + 1, (i, self.inbox[i])
                continue
            else:
                self._cached_clock_other = i
                break
        else:
            self._cached_clock_other = len(self.inbox)
        return self._cached_clock_other

    @property
    def clock_out_of_order(self) -> List[int]:
        message_ids = []
        for inbox_item in self.inbox[self.clock_other:]:
            if inbox_item.message:
                message_ids.append(inbox_item.message.header.message_id)
        return message_ids

    @property
    def other_clock_self(self):
        for i in range(self._cached_other_clock_self, len(self.outbox)):
            if not self.outbox[i].acked:
                self._cached_other_clock_self = i
                break
        else:
            self._cached_other_clock_self = len(self.outbox)
        return self._cached_other_clock_self

    @property
    def other_clock_other(self):
        return len(self.inbox)

    @property
    def other_clock_out_of_order(self) -> List[int]:
        message_ids = []
        for outbox_item in self.outbox[self.other_clock_self:]:
            if outbox_item.acked:
                message_ids.append(outbox_item.message.header.message_id)
        return message_ids

    @property
    def debug_clocks(self):
        return {
            'self_clock_self':          self.clock_self,
            'self_clock_other':         self.clock_other,
            'self_clock_out_of_order':  self.clock_out_of_order,
            'other_clock_self':         self.other_clock_self,
            'other_clock_other':        self.other_clock_other,
            'other_clock_out_of_order': self.other_clock_out_of_order,
        }

    @property
    def is_synchronized(self):
        if self.clock_self != self.other_clock_self:
            return False
        if self.clock_other != self.other_clock_other:
            return False
        assert len(self.clock_out_of_order) == 0
        assert len(self.other_clock_out_of_order) == 0
        return True

    def append_outbox_data(self, data: Union[str, bytes, dict, BaseModel]):
        message = Message.from_content(message_id=len(self.outbox) + 1, content=data)

        # single part if size < 10M
        if len(message.binary_data) <= MULTIPART_LIMIT_SIZE_BYTES:
            with self._outbox_lock:
                message.header.message_id = len(self.outbox) + 1  # just to be safe, set this again
                self.outbox.append(OutboxItem(message))
                return

        _cursor = 0
        _previous_message_id = 0
        while _cursor < len(message.binary_data):
            # read data fragment from previously-created message
            _data = message.binary_data[_cursor:_cursor + MULTIPART_LIMIT_SIZE_BYTES]
            _cursor += MULTIPART_LIMIT_SIZE_BYTES

            # create message fragment
            _message_part = Message.from_content(message_id=len(self.outbox) + 1, content=_data)
            _message_part.header.message_prev = _previous_message_id

            # set content type to multipart, or to the actual content type for the final fragment
            if _cursor < len(message.binary_data):
                _message_part.header.content_type = ContentType.MULTIPART_FRAGMENT
            else:
                _message_part.header.content_type = message.header.content_type

            # add the message
            with self._outbox_lock:
                _message_part.header.message_id = _previous_message_id = len(self.outbox) + 1
                self.outbox.append(OutboxItem(_message_part))

        # monotonicity sanity check
        _ = self.clock_self

    def create_packet(self, retransmission_timeout: Optional[datetime.timedelta] = None) -> Packet:
        # allow overwriting the retransmission timeout to immediately resend
        if retransmission_timeout is None:
            retransmission_timeout = RETRANSMISSION_TIMEOUT

        # control data
        control = Control(sender_clock_sender=self.clock_self,
                          sender_clock_recipient=self.clock_other,
                          sender_clock_out_of_order=self.clock_out_of_order,
                          recipient_clock_sender=self.other_clock_self,
                          nack_ids=sorted(self.nack_ids))

        # tracking
        current_timestamp = get_utc_timestamp()
        total_size_bytes = PACKET_HEADER_SIZE + control.size_bytes
        assert total_size_bytes < PACKET_LIMIT_SIZE_BYTES

        # get messages to send
        messages = []
        for outbox_item in self.outbox[self.other_clock_self:]:
            # skip if already acked
            if outbox_item.acked:
                continue
            # skip if not yet time to retransmit
            if outbox_item.packet_timestamp is not None:
                if outbox_item.packet_timestamp + retransmission_timeout > current_timestamp:
                    continue
            # skip if too big to append
            if total_size_bytes + outbox_item.message.size_bytes > PACKET_LIMIT_SIZE_BYTES:
                continue
            messages.append(outbox_item.message)

        # housekeeping of nacks
        for nack_id in self.nack_ids:
            self._sent_nack_ids[nack_id] += 1
        self.nack_ids.clear()
        for nack_id, times_sent in list(self._sent_nack_ids.items()):
            if times_sent > NACK_TRANSMIT_COUNT:
                del self._sent_nack_ids[nack_id]
            else:
                self.nack_ids.add(nack_id)

        # create packet
        self.num_sent_packets += 1
        return Packet(header=PacketHeader(sender_uuid=self.self_uuid,
                                          recipient_uuid=self.other_uuid,
                                          packet_id=self.num_sent_packets,
                                          num_messages=len(messages)),
                      messages=messages,
                      control=control)

    def packet_send(self, packet: Packet):
        # double-check the uuids
        if packet.header.sender_uuid != self.self_uuid:
            raise KeyError('mismatched sender uuid')
        if packet.header.recipient_uuid != self.other_uuid:
            raise KeyError('mismatched recipient uuid')

        # just update clocks
        # noinspection DuplicatedCode
        for i in range(self.clock_other, packet.control.sender_clock_recipient):
            if not self.inbox[i].acked:
                self.inbox[i].acked = packet.header.packet_timestamp
        self._cached_clock_other = packet.control.sender_clock_recipient
        for i in packet.control.sender_clock_out_of_order:
            assert self.inbox[i - 1].message.header.message_id == i
            if not self.inbox[i - 1].acked:
                self.inbox[i - 1].acked = packet.header.packet_timestamp
        for message in packet.messages:
            if self.outbox[message.header.message_id - 1].acked:
                continue
            self.outbox[message.header.message_id - 1].packet_timestamp = packet.header.packet_timestamp
            self.outbox[message.header.message_id - 1].packet_id = packet.header.packet_id

    def packet_receive(self, packet: Packet):
        # double-check the uuids
        if packet.header.sender_uuid != self.other_uuid:
            raise KeyError('mismatched sender uuid')
        if packet.header.recipient_uuid != self.self_uuid:
            raise KeyError('mismatched recipient uuid')

        # update clocks
        while self.other_clock_other < packet.control.sender_clock_sender:
            self.inbox.append(InboxItem())
        # noinspection DuplicatedCode
        for i in range(self.other_clock_self, packet.control.sender_clock_recipient):
            if not self.outbox[i].acked:
                self.outbox[i].acked = packet.header.packet_timestamp
        self._cached_other_clock_self = packet.control.sender_clock_recipient
        for i in packet.control.sender_clock_out_of_order:
            assert self.outbox[i - 1].message.header.message_id == i
            if not self.outbox[i - 1].acked:
                self.outbox[i - 1].acked = packet.header.packet_timestamp
        for i in range(packet.control.recipient_clock_sender):
            if not self.inbox[i].ack_acked:
                self.inbox[i].ack_acked = packet.header.packet_timestamp

        # update nack to pretend the messages are unsent
        # this could be optimized by tracking un-acked hashes in a dict, but it also means additional complexity
        nack_ids = set(packet.control.nack_ids)
        if nack_ids:
            for outbox_item in self.outbox[self.clock_other:]:
                if outbox_item.acked:
                    continue
                if outbox_item.packet_id in nack_ids:
                    outbox_item.packet_id = None
                    outbox_item.packet_timestamp = None

        # add received messages to inbox
        for message in packet.messages:
            # already received
            if self.inbox[message.header.message_id - 1].message:
                continue

            # link previous message
            if message.header.message_prev > 0:
                self._inbox_unlinked_previous_messages[message.header.message_id] = message.header.message_prev

            self.inbox[message.header.message_id - 1].message = message
            self.inbox[message.header.message_id - 1].packet_timestamp = packet.header.packet_timestamp

        # link previous messages
        for message_id, previous_message_id in list(self._inbox_unlinked_previous_messages.items()):
            if self.inbox[previous_message_id - 1].message is not None:
                self.inbox[message_id - 1].message.previous_message = self.inbox[previous_message_id - 1].message
                del self._inbox_unlinked_previous_messages[message_id]


if __name__ == '__main__':
    s1 = Messenger(self_uuid=uuid.uuid4(), other_uuid=uuid.uuid4())
    s2 = Messenger(self_uuid=s1.other_uuid, other_uuid=s1.self_uuid)

    p1_0 = s1.create_packet()
    print(p1_0)

    print(s1.debug_clocks)

    s1.append_outbox_data('test')
    s1.append_outbox_data(b'some binary \0\0')

    print(s1.debug_clocks)

    p1_1 = s1.create_packet()
    print(p1_1)
    s1.packet_send(p1_1)

    p1_2 = s1.create_packet()
    print(p1_2)

    s2.nack_ids.update([p1_1.header.packet_id, 999])
    p2_0 = s2.create_packet()
    print(p2_0)
    s1.packet_receive(p2_0)

    p1_3 = s1.create_packet()
    print(p1_3)

    s2.packet_receive(p1_1)

    for _item in s2.inbox:
        print(_item.message)
    p2_1 = s2.create_packet()
    print(p2_1)
    s2.packet_send(p2_1)
    time.sleep(0.1)

    p2_2 = s2.create_packet()
    print(p2_2)
    s2.packet_send(p2_2)
    s1.packet_receive(p2_2)

    print(s1.debug_clocks)

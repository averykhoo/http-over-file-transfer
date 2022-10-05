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
from typing import List
from typing import Optional
from typing import Union
from uuid import UUID

from diode_bridge_v2.schemas.packet import Control
from diode_bridge_v2.schemas.packet import Message
from diode_bridge_v2.schemas.packet import Packet
from diode_bridge_v2.schemas.packet import PacketHeader
from diode_bridge_v2.schemas.packet import get_utc_timestamp


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
    retransmission_timeout: datetime.timedelta = field(default=datetime.timedelta(seconds=5))
    max_size_bytes: int = field(default=100 * 1024 * 124)  # of data, not the binary thing, so leave some overhead
    transmit_nack_how_many_times: int = field(default=5)

    outbox: List[OutboxItem] = field(default_factory=list)  # outbox[i].message.header.message_id == i + 1
    inbox: List[InboxItem] = field(default_factory=list)  # inbox[i].message.header.message_id == i + 1

    _cached_clock_other: int = field(default=0)
    _cached_other_clock_self: int = field(default=0)
    # todo: cache the other two and enforce monotonicity

    nack_ids: List[int] = field(default_factory=list)  # todo: make this a set
    _sent_nack_ids: Counter = field(default_factory=Counter)
    # todo: collect nacks in a set to calculate stats to optimize nack retransmits and pessimistic retransmits

    _num_sent_packets: int = field(default=0)

    @property
    def clock_self(self) -> int:
        return len(self.outbox)

    @property
    def clock_other(self) -> int:
        for i in range(self._cached_clock_other, len(self.inbox)):
            if not self.inbox[i].message:
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

    def append_outbox_data(self, data: Union[str, bytes, dict]):
        self.outbox.append(OutboxItem(Message.from_content(message_id=len(self.outbox) + 1, content=data)))

        for i, outbox_item in enumerate(self.outbox):
            assert outbox_item.message.header.message_id == i + 1, (i, outbox_item)

    def create_packet(self, retransmission_timeout: Optional[datetime.timedelta] = None) -> Packet:
        # allow overwriting the retransmission timeout to immediately resend
        if retransmission_timeout is None:
            retransmission_timeout = self.retransmission_timeout

        # get messages to send
        current_timestamp = get_utc_timestamp()
        messages = []
        total_size_bytes = 0
        for outbox_item in self.outbox[self.other_clock_self:]:
            # skip if already acked
            if outbox_item.acked:
                continue
            # skip if not yet time to retransmit
            if outbox_item.packet_timestamp is not None:
                if outbox_item.packet_timestamp + retransmission_timeout > current_timestamp:
                    continue
            # skip if too big to append
            if total_size_bytes + len(outbox_item.message.content) > self.max_size_bytes:
                continue
            messages.append(outbox_item.message)

        # create packet
        self._num_sent_packets += 1
        out = Packet(header=PacketHeader(sender_uuid=self.self_uuid,
                                         recipient_uuid=self.other_uuid,
                                         packet_id=self._num_sent_packets,
                                         num_messages=len(messages)),
                     messages=messages,
                     control=Control(sender_clock_sender=self.clock_self,
                                     sender_clock_recipient=self.clock_other,
                                     sender_clock_out_of_order=self.clock_out_of_order,
                                     recipient_clock_sender=self.other_clock_self,
                                     nack_ids=sorted(set(self.nack_ids))))

        # housekeeping of nacks
        for nack_id in self.nack_ids:
            self._sent_nack_ids[nack_id] += 1
        self.nack_ids.clear()
        to_remove = []
        for nack_id, times_sent in self._sent_nack_ids.items():
            if times_sent > self.transmit_nack_how_many_times:
                to_remove.append(nack_id)
            else:
                self.nack_ids.append(nack_id)
        for nack_id in to_remove:
            del self._sent_nack_ids[nack_id]

        # return the packet
        return out

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

        # update received data
        for message in packet.messages:
            if self.inbox[message.header.message_id - 1].message:
                continue
            self.inbox[message.header.message_id - 1].message = message
            self.inbox[message.header.message_id - 1].packet_timestamp = packet.header.packet_timestamp

        for i, inbox_item in enumerate(self.inbox):
            if inbox_item.message:
                assert inbox_item.message.header.message_id == i + 1, (i, inbox_item)


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

    s2.nack_ids.extend([p1_1.header.packet_id, 999])
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

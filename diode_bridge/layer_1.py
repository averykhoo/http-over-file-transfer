import datetime
import time
import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union
from uuid import UUID

from diode_bridge.schemas.layer_1_packet import ContentType
from diode_bridge.schemas.layer_1_packet import Control
from diode_bridge.schemas.layer_1_packet import Message
from diode_bridge.schemas.layer_1_packet import Metadata
from diode_bridge.schemas.layer_1_packet import Packet


@dataclass
class OutboxItem:
    message: Message
    packet_timestamp: Optional[datetime.datetime] = None
    packet_hash: Optional[str] = None
    acked: Optional[datetime.datetime] = None  # this could be a bool


@dataclass
class InboxItem:
    message: Optional[Message] = None
    packet_timestamp: Optional[datetime.datetime] = None
    acked: Optional[datetime.datetime] = None  # this could be a bool
    ack_acked: Optional[datetime.datetime] = None  # this could be a bool


@dataclass
class Server:
    self_uuid: UUID
    other_uuid: UUID
    retransmission_timeout: datetime.timedelta = datetime.timedelta(seconds=60)
    max_size_bytes: int = 100 * 1024 * 124  # of data, not the binary thing, so leave some overhead

    outbox: List[OutboxItem] = field(default_factory=list)  # outbox[i].message.message_id == i + 1
    inbox: List[InboxItem] = field(default_factory=list)  # inbox[i].message.message_id == i + 1

    _cached_clock_other: int = 0
    _cached_other_clock_self: int = 0

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
                message_ids.append(inbox_item.message.message_id)
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
                message_ids.append(outbox_item.message.message_id)
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

    def append_outbox_data(self, data: Union[str, bytes]):
        if isinstance(data, str):
            _data = data.encode('utf8')
            if len(_data) > self.max_size_bytes:
                raise ValueError('too big')
            self.outbox.append(OutboxItem(Message(message_id=len(self.outbox),
                                                  content_type=ContentType.STRING,
                                                  content=_data)))
        elif isinstance(data, bytes):
            if len(data) > self.max_size_bytes:
                raise ValueError('too big')
            self.outbox.append(OutboxItem(Message(message_id=len(self.outbox),
                                                  content_type=ContentType.BINARY,
                                                  content=data)))
        else:
            raise TypeError(data)

    def create_packet(self,
                      retransmission_timeout: Optional[datetime.timedelta] = None,
                      nack_hashes: Iterable[str] = (),
                      ) -> Packet:
        # allow overwriting the retransmission timeout to immediately resend
        if retransmission_timeout is None:
            retransmission_timeout = self.retransmission_timeout

        # get messages to send
        current_timestamp = datetime.datetime.now()
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
        return Packet(metadata=Metadata(sender_uuid=self.self_uuid,
                                        recipient_uuid=self.other_uuid,
                                        sent_timestamp=current_timestamp),
                      messages=messages,
                      control=Control(sender_clock_sender=self.clock_self,
                                      sender_clock_recipient=self.clock_other,
                                      sender_clock_out_of_order=self.clock_out_of_order,
                                      recipient_clock_sender=self.other_clock_self,
                                      nack_hashes=sorted(set(nack_hashes))))

    def packet_send(self, packet: Packet, packet_hash: str):
        # double-check the uuids
        if packet.metadata.sender_uuid != self.self_uuid:
            raise KeyError('mismatched sender uuid')
        if packet.metadata.recipient_uuid != self.other_uuid:
            raise KeyError('mismatched recipient uuid')

        # just update clocks
        # noinspection DuplicatedCode
        for i in range(self.clock_other, packet.control.sender_clock_recipient):
            if not self.inbox[i].acked:
                self.inbox[i].acked = packet.metadata.sent_timestamp
        self._cached_clock_other = packet.control.sender_clock_recipient
        for i in packet.control.sender_clock_out_of_order:
            if not self.inbox[i].acked:
                self.inbox[i].acked = packet.metadata.sent_timestamp
        for message in packet.messages:
            if self.outbox[message.message_id - 1].acked:
                continue
            self.outbox[message.message_id - 1].packet_timestamp = packet.metadata.sent_timestamp
            self.outbox[message.message_id - 1].packet_hash = packet_hash

    def packet_receive(self, packet: Packet):
        # double-check the uuids
        if packet.metadata.sender_uuid != self.other_uuid:
            raise KeyError('mismatched sender uuid')
        if packet.metadata.recipient_uuid != self.self_uuid:
            raise KeyError('mismatched recipient uuid')

        # update clocks
        while self.other_clock_other < packet.control.sender_clock_sender:
            self.inbox.append(InboxItem())
        # noinspection DuplicatedCode
        for i in range(self.other_clock_self, packet.control.sender_clock_recipient):
            if not self.outbox[i].acked:
                self.outbox[i].acked = packet.metadata.sent_timestamp
        self._cached_other_clock_self = packet.control.sender_clock_recipient
        for i in packet.control.sender_clock_out_of_order:
            if not self.outbox[i].acked:
                self.outbox[i].acked = packet.metadata.sent_timestamp
        for i in range(packet.control.recipient_clock_sender):
            if not self.inbox[i].ack_acked:
                self.inbox[i].ack_acked = packet.metadata.sent_timestamp

        # update nack to pretend the messages are unsent
        # this could be optimized by tracking un-acked hashes in a dict, but it also means additional complexity
        nack_hashes = set(packet.control.nack_hashes)
        if nack_hashes:
            for outbox_item in self.outbox[self.clock_other:]:
                if outbox_item.acked:
                    continue
                if outbox_item.packet_hash in nack_hashes:
                    outbox_item.packet_hash = None
                    outbox_item.packet_timestamp = None

        # update received data
        for message in packet.messages:
            if self.inbox[message.message_id - 1].message:
                continue
            self.inbox[message.message_id - 1].message = message
            self.inbox[message.message_id - 1].packet_timestamp = packet.metadata.sent_timestamp


if __name__ == '__main__':
    s1 = Server(self_uuid=uuid.uuid4(), other_uuid=uuid.uuid4())
    s2 = Server(self_uuid=s1.other_uuid, other_uuid=s1.self_uuid)

    p1_0 = s1.create_packet()
    print(p1_0)

    print(s1.debug_clocks)

    s1.append_outbox_data('test')
    s1.append_outbox_data(b'some binary \0\0')

    print(s1.debug_clocks)

    p1_1 = s1.create_packet()
    print(p1_1)
    s1.packet_send(p1_1, 'some-hash-hex-digest-1-1')

    p1_2 = s1.create_packet()
    print(p1_2)

    p2_0 = s2.create_packet(nack_hashes=['some-hash-hex-digest-1-1'])
    print(p2_0)
    s1.packet_receive(p2_0)

    p1_3 = s1.create_packet()
    print(p1_3)

    s2.packet_receive(p1_1)

    for _item in s2.inbox:
        print(_item.message)
    p2_1 = s2.create_packet()
    print(p2_1)
    s2.packet_send(p2_1, 'some-hash-hex-digest-2')
    time.sleep(0.1)

    p2_2 = s2.create_packet()
    print(p2_2)
    s2.packet_send(p2_2, 'some-hash-hex-digest-3')
    s1.packet_receive(p2_2)

    print(s1.debug_clocks)

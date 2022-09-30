import datetime
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Dict
from uuid import UUID

from diode_bridge.core.layer_0 import BinaryReader
from diode_bridge.core.layer_0 import write_packet
from diode_bridge.core.layer_1 import Messenger
from diode_bridge.dependencies.jose_wrapper import deserialize
from diode_bridge.schemas.layer_0_header import PacketSenderRecipient

DELAY_ASSUME_ERROR = datetime.timedelta(seconds=1)


@dataclass
class OpenFile:
    reader: BinaryReader
    prev_time_monotonic: float = field(default_factory=time.monotonic)
    prev_size_bytes: int = 0
    closed: bool = False

    def __post_init__(self):
        self.reader.__enter__()

    def close(self, *, delete=False):
        self.reader.__exit__(None, None, None)  # todo: this is unsafe as it might not close
        self.closed = True
        if delete:
            self.reader.path.unlink()


@dataclass
class Server:
    uuid: UUID
    input_folder: Path
    output_folder: Path
    messengers: Dict[UUID, Messenger] = field(default_factory=dict)

    _current_files: Dict[Path, OpenFile] = field(default_factory=dict)

    def _find_input_files(self):
        for path in self.input_folder.glob(f'{self.uuid}/*.packet'):
            packet_sender_recipient = PacketSenderRecipient.from_path(path)
            assert packet_sender_recipient.recipient_uuid == self.uuid  # todo: do something if not
            if path not in self._current_files:
                self._current_files[path] = OpenFile(BinaryReader(path))
        # print(self.uuid, self._current_files)

    def _try_read_input_files(self,
                              delete_successful=True,
                              delete_error_files=True,
                              ):
        # read files
        for path, open_file in self._current_files.items():

            # try to read a chunk of data
            try:
                data = open_file.reader.try_read()
            except IOError:
                if delete_error_files and open_file.reader.header is not None:
                    messenger = self.messengers[PacketSenderRecipient.from_path(path).sender_uuid]
                    messenger.nack_hashes.append(open_file.reader.header.hex_digest)
                open_file.close(delete=delete_error_files)
                continue

            # try to ingest packet
            if data is not None:
                packet_sender_recipient = PacketSenderRecipient.from_path(path)
                assert packet_sender_recipient.recipient_uuid == self.uuid  # todo: do something if not
                packet = deserialize(data)
                self.messengers[packet_sender_recipient.sender_uuid].packet_receive(packet)
                open_file.close(delete=delete_successful)
                continue

            # check if we haven't read new bytes for a while
            if open_file.prev_size_bytes == open_file.reader.size_bytes():
                if open_file.prev_time_monotonic + DELAY_ASSUME_ERROR.total_seconds() < time.monotonic():
                    if delete_error_files and open_file.reader.header is not None:
                        messenger = self.messengers[PacketSenderRecipient.from_path(path).sender_uuid]
                        messenger.nack_hashes.append(open_file.reader.header.hex_digest)
                    open_file.close(delete=delete_error_files)

            else:
                open_file.prev_size_bytes = open_file.reader.size_bytes()
                open_file.prev_time_monotonic = time.monotonic()

        # prune files
        closed = [path for path, open_file in self._current_files.items() if open_file.closed]
        for path in closed:
            del self._current_files[path]

    def _write_output_files(self):
        for messenger in self.messengers.values():
            packet = messenger.create_packet()
            # print(packet)
            file_header = write_packet(packet, self.output_folder)
            messenger.packet_send(packet, file_header.hex_digest)

    def run_once(self):
        self._find_input_files()
        self._try_read_input_files()
        self._write_output_files()


if __name__ == '__main__':
    from uuid import uuid4

    # create servers
    s1 = Server(uuid=uuid4(), input_folder=Path('test/s1'), output_folder=Path('test/s2'))
    s2 = Server(uuid=uuid4(), input_folder=Path('test/s2'), output_folder=Path('test/s1'))

    # create messengers
    m1 = Messenger(self_uuid=s1.uuid, other_uuid=s2.uuid)
    s1.messengers[s2.uuid] = m1
    m2 = Messenger(self_uuid=s2.uuid, other_uuid=s1.uuid)
    s2.messengers[s1.uuid] = m2

    m1.append_outbox_data('test test m1')
    m2.append_outbox_data('hello from m2')
    i = 0


    def sync():
        global i
        for _ in range(10):
            m1.append_outbox_data(f'm1 {i}')
            s1.run_once()
            # print('m1', m1.debug_clocks)
            m2.append_outbox_data(f'm2 {i}')
            s2.run_once()
            # print('m2', m2.debug_clocks)
            i += 1
            time.sleep(0.5)


    # sync()

    m1.append_outbox_data('test test 2 m1')
    m2.append_outbox_data('hello from m2 2')

    sync()

    print([i.message.content if i.message else None for i in m1.inbox])
    print([i.message.content if i.message else None for i in m2.inbox])
    sync()

    print([i.message.content if i.message else None for i in m1.inbox])
    print([i.message.content if i.message else None for i in m2.inbox])

    sync()

    print([i.message.content if i.message else None for i in m1.inbox])
    print([i.message.content if i.message else None for i in m2.inbox])

    sync()

    print([i.message.content if i.message else None for i in m1.inbox])
    print([i.message.content if i.message else None for i in m2.inbox])

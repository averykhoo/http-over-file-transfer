import datetime
import time
import warnings
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Dict
from uuid import UUID

from diode_bridge_v2.core.layer_0 import BinaryReader
from diode_bridge_v2.core.layer_0 import BinaryWriter
from diode_bridge_v2.core.layer_1 import Messenger
from diode_bridge_v2.schemas.packet import Packet

DELAY_ASSUME_ERROR = datetime.timedelta(seconds=3)


@dataclass
class Server:
    uuid: UUID
    input_folder: Path
    output_folder: Path
    messengers: Dict[UUID, Messenger] = field(default_factory=dict)

    _current_files: Dict[Path, BinaryReader] = field(default_factory=dict)

    def _find_input_files(self):
        for path in self.input_folder.glob(f'{self.uuid}/*.packet'):
            # ignore seen files
            if path in self._current_files:
                continue

            # validate filename
            if path.stem.startswith('.'):
                continue
            if path.stem.count('--') != 2:
                continue

            # check recipient uuid
            _sender, _recipient, _packet_id = path.stem.split('--')
            if str(self.uuid) != _recipient:
                warnings.warn(f'incorrect recipient for {path}, expected {self.uuid}, got {_recipient}')
                continue

            # assume corrupted if less than 4 bytes
            if path.stat().st_size < 4:
                messenger = self.messengers[UUID(_sender)]
                messenger.nack_ids.add(_packet_id)
                continue

            self._current_files[path] = BinaryReader(path)

    def _try_read_input_files(self,
                              delete_successful=True,
                              delete_error_files=True,
                              ):
        # read files
        for path, open_file in self._current_files.items():

            if not open_file.is_ready_to_read:
                continue

            _sender, _recipient, _packet_id = path.stem.split('--')
            messenger = self.messengers[UUID(_sender)]
            # noinspection PyBroadException
            try:
                packet = Packet.from_file(open_file)
                open_file.close(delete=delete_successful)

                if packet.header.recipient_uuid != self.uuid:
                    warnings.warn('incorrect recipient uuid')
                    open_file.close(delete=delete_error_files)
                    continue

                # receive the packet data
                messenger.packet_receive(packet)

                # nack incomplete packets
                if packet.control is None or packet.header.num_messages < len(packet.messages):
                    messenger.nack_ids.add(packet.header.packet_id)

            except Exception:
                open_file.close(delete=delete_error_files)
                messenger.nack_ids.add(_packet_id)

        # prune files
        closed = [path for path, open_file in self._current_files.items() if open_file.closed]
        for path in closed:
            del self._current_files[path]

    def _write_output_files(self):
        for messenger in self.messengers.values():
            packet = messenger.create_packet()
            f = BinaryWriter(self.output_folder / packet.header.filename)
            packet.to_file(f)
            f.close()
            messenger.packet_send(packet)

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

    m1.append_outbox_data('test test m1' + '*' * 100)
    m2.append_outbox_data('hello from m2' + '*' * 200)
    i = 0


    def sync():
        global i
        for _ in range(5):
            m1.append_outbox_data({'m1': i})
            m2.append_outbox_data(f'm2 {i}')
            i += 1
            for _ in range(4):
                s1.run_once()
                # print('m1', m1.debug_clocks)
                s2.run_once()
                # print('m2', m2.debug_clocks)
                time.sleep(0.5)


    sync()

    m1.append_outbox_data('test test 2 m1' + '!' * 200)
    m2.append_outbox_data('hello from m2 2' + '!' * 200)

    sync()

    print([i.message.content if i.message else None for i in m1.inbox])
    print([i.message.content if i.message else None for i in m2.inbox])
    sync()

    print([i.message.content if i.message else None for i in m1.inbox])
    print([i.message.content if i.message else None for i in m2.inbox])
    #
    # sync()
    #
    # print([i.message.content if i.message else None for i in m1.inbox])
    # print([i.message.content if i.message else None for i in m2.inbox])
    #
    # sync()
    #
    # print([i.message.content if i.message else None for i in m1.inbox])
    # print([i.message.content if i.message else None for i in m2.inbox])

    print(m1.debug_clocks)
    print(m2.debug_clocks)

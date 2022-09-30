"""
acts as a wrapper for some jwt/jwe library
undecided which library to use, but it shouldn't matter
for now, just passthrough
"""
import json
from typing import Union

from diode_bridge.schemas.layer_1_packet import Packet


def serialize(packet: Packet) -> bytes:
    # todo: actually use some jwt and jwe
    # todo: accept symmetric secrets or public/private keys loaded from somewhere
    return json.dumps({'pretend-this-is-a-jwt-jwe': json.loads(packet.json())}).encode('ascii')


def deserialize(binary_data: Union[bytes, bytearray]) -> Packet:
    # todo: reverse of serialize
    return Packet.parse_obj(json.loads(binary_data.decode('ascii'))['pretend-this-is-a-jwt-jwe'])


if __name__ == '__main__':
    import datetime
    import random
    import uuid

    from diode_bridge.schemas.layer_1_packet import Control
    from diode_bridge.schemas.layer_1_packet import Metadata

    p = Packet(metadata=Metadata(sender_uuid=uuid.uuid4(),
                                 recipient_uuid=uuid.uuid4(),
                                 sent_timestamp=datetime.datetime.now(),
                                 protocol_version=random.randint(0, 99999)),
               messages=[],
               control=Control(sender_clock_sender=random.randint(0, 99999),
                               sender_clock_recipient=random.randint(0, 99999),
                               sender_clock_out_of_order=[random.randint(0, 9999) for _ in
                                                          range(random.randint(0, 999))],
                               recipient_clock_sender=random.randint(0, 99999),
                               nack_hashes=[str(random.randint(0, 99999)) for _ in range(random.randint(0, 99))]))
    print(p)
    print(serialize(p))
    print(deserialize(serialize(p)))
    print(deserialize(serialize(p)) == p)

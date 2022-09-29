"""
acts as a wrapper for some jwt/jwe library
undecided which library to use, but it shouldn't matter
for now, just passthrough
"""
import json
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union


def serialize(json_data: Dict[str, Any]) -> bytes:
    # todo: actually use some jwt and jwe
    # todo: accept symmetric secrets or public/private keys loaded from somewhere
    return json.dumps(json_data).encode('ascii')


def deserialize(binary_data: Union[bytes, bytearray]) -> Optional[Dict[str, Any]]:
    # todo: reverse of serialize
    return json.loads(binary_data.decode('ascii'))

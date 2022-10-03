"""
https://github.com/averykhoo/msgparse/blob/main/msgparse/coerce.py
"""
import datetime
import struct
from functools import lru_cache
from ipaddress import IPv4Address
from ipaddress import IPv6Address
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Union
from uuid import UUID

STRING_ENCODING: str = 'utf8'
BIG_ENDIAN: bool = True


@lru_cache(maxsize=None)
def hex_to_integer16(hex_number: str) -> int:
    # e.g. "0xD0CF" -> 53455
    if not isinstance(hex_number, str):
        raise TypeError(hex_number)

    # remove leading '0x' (e.g. "0xD0CF" -> "D0CF")
    if hex_number[:2].lower() != '0x':
        raise ValueError(hex_number)
    _number = hex_number[2:]

    # check length is 4 chars (e.g. '0x0001' not '0x1')
    if len(_number) != 4:
        raise ValueError(hex_number)

    # don't allow mixed-case (e.g. "d0cF")
    if not (_number.islower() or _number.isupper() or _number.isdigit()):
        raise ValueError(hex_number)

    return int(_number, 16)


def to_bytes(binary_data: bytes) -> Optional[bytes]:
    if binary_data == b'':
        return None
    else:
        return binary_data


def from_bytes(binary_data: Optional[bytes]) -> bytes:
    if binary_data is None:
        return b''
    else:
        return binary_data


def to_string(binary_data: bytes) -> Optional[str]:
    if binary_data == b'':
        return None
    else:
        return binary_data.decode(encoding=STRING_ENCODING)


def from_string(text: Optional[str]) -> bytes:
    if text is None:
        return b''
    else:
        return text.encode(encoding=STRING_ENCODING)


_int8_unpack = struct.Struct('>b').unpack


@lru_cache(maxsize=None)
def to_integer8(binary_data: bytes) -> Optional[int]:  # signed char
    if binary_data == b'':
        return None
    else:
        return _int8_unpack(binary_data)[0]


_int8_pack = struct.Struct('>b').pack


@lru_cache(maxsize=None)
def from_integer8(number: Optional[int]) -> bytes:  # signed char
    if number is None:
        return b''
    else:
        return _int8_pack(number)


@lru_cache(maxsize=None)
def to_unsigned_integer8(binary_data: bytes) -> Optional[int]:  # unsigned char / byte
    if binary_data == b'':
        return None
    elif len(binary_data) != 1:
        raise ValueError(binary_data)
    else:
        return binary_data[0]


_uint8_pack = struct.Struct('>B').pack


@lru_cache(maxsize=None)
def from_unsigned_integer8(number: Optional[int]) -> bytes:  # unsigned char / byte
    if number is None:
        return b''
    else:
        return _uint8_pack(number)


_int16_unpack_be = struct.Struct('>h').unpack
_int16_unpack_le = struct.Struct('<h').unpack


@lru_cache(maxsize=None)
def to_integer16(binary_data: bytes) -> Optional[int]:  # short
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _int16_unpack_be(binary_data)[0]
    else:
        return _int16_unpack_le(binary_data)[0]


_int16_pack_be = struct.Struct('>h').pack
_int16_pack_le = struct.Struct('<h').pack


@lru_cache(maxsize=None)
def from_integer16(number: Optional[int]) -> bytes:  # short
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _int16_pack_be(number)
    else:
        return _int16_pack_le(number)


_uint16_unpack_be = struct.Struct('>H').unpack
_uint16_unpack_le = struct.Struct('<H').unpack


@lru_cache(maxsize=None)
def to_unsigned_integer16(binary_data: bytes) -> Optional[int]:  # unsigned short
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _uint16_unpack_be(binary_data)[0]
    else:
        return _uint16_unpack_le(binary_data)[0]


_uint16_pack_be = struct.Struct('>H').pack
_uint16_pack_le = struct.Struct('<H').pack


@lru_cache(maxsize=None)
def from_unsigned_integer16(number: Optional[int]) -> bytes:  # unsigned short
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _uint16_pack_be(number)
    else:
        return _uint16_pack_le(number)


_int32_unpack_be = struct.Struct('>i').unpack
_int32_unpack_le = struct.Struct('<i').unpack


def to_integer32(binary_data: bytes) -> Optional[int]:  # int / long
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _int32_unpack_be(binary_data)[0]
    else:
        return _int32_unpack_le(binary_data)[0]


_int32_pack_be = struct.Struct('>i').pack
_int32_pack_le = struct.Struct('<i').pack


def from_integer32(number: Optional[int]) -> bytes:  # int / long
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _int32_pack_be(number)
    else:
        return _int32_pack_le(number)


_uint32_unpack_be = struct.Struct('>I').unpack
_uint32_unpack_le = struct.Struct('<I').unpack


def to_unsigned_integer32(binary_data: bytes) -> Optional[int]:  # unsigned int / long
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _uint32_unpack_be(binary_data)[0]
    else:
        return _uint32_unpack_le(binary_data)[0]


_uint32_pack_be = struct.Struct('>I').pack
_uint32_pack_le = struct.Struct('<I').pack


def from_unsigned_integer32(number: Optional[int]) -> bytes:  # unsigned int / long
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _uint32_pack_be(number)
    else:
        return _uint32_pack_le(number)


_int64_unpack_be = struct.Struct('>q').unpack
_int64_unpack_le = struct.Struct('<q').unpack


def to_integer64(binary_data: bytes) -> Optional[int]:  # long long
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _int64_unpack_be(binary_data)[0]
    else:
        return _int64_unpack_le(binary_data)[0]


_int64_pack_be = struct.Struct('>q').pack
_int64_pack_le = struct.Struct('<q').pack


def from_integer64(number: Optional[int]) -> bytes:  # long long
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _int64_pack_be(number)
    else:
        return _int64_pack_le(number)


_uint64_unpack_be = struct.Struct('>Q').unpack
_uint64_unpack_le = struct.Struct('<Q').unpack


def to_unsigned_integer64(binary_data: bytes) -> Optional[int]:  # unsigned long long
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _uint64_unpack_be(binary_data)[0]
    else:
        return _uint64_unpack_le(binary_data)[0]


_uint64_pack_be = struct.Struct('>Q').pack
_uint64_pack_le = struct.Struct('<Q').pack


def from_unsigned_integer64(number: Optional[int]) -> bytes:  # unsigned long long
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _uint64_pack_be(number)
    else:
        return _uint64_pack_le(number)


int_from_bytes = int.from_bytes  # this dereference costs 0.2 microseconds


def to_bigint(binary_data: bytes) -> Optional[int]:
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return int_from_bytes(binary_data, byteorder='big', signed=True)
    else:
        return int_from_bytes(binary_data, byteorder='little', signed=True)


def from_bigint(number: Optional[int]) -> bytes:
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return number.to_bytes(length=(8 + (number + (number < 0)).bit_length()) // 8, byteorder='big', signed=True)
    else:
        return number.to_bytes(length=(8 + (number + (number < 0)).bit_length()) // 8, byteorder='little', signed=True)


def to_unsigned_bigint(binary_data: bytes) -> Optional[int]:
    if binary_data == b'':
        return None  # this COULD be zero, but it's more likely to be a null value
    elif BIG_ENDIAN:
        return int_from_bytes(binary_data, byteorder='big', signed=False)
    else:
        return int_from_bytes(binary_data, byteorder='little', signed=False)


def from_unsigned_bigint(number: Optional[int]) -> bytes:
    if number is None:
        return b''
    elif number == 0:
        return b'\x00'  # differentiate from None
    elif number < 0:
        raise ValueError(number)
    elif BIG_ENDIAN:
        return number.to_bytes(length=(7 + number.bit_length()) // 8, byteorder='big', signed=False)
    else:
        return number.to_bytes(length=(7 + number.bit_length()) // 8, byteorder='little', signed=False)


_float16_unpack_be = struct.Struct('>e').unpack
_float16_unpack_le = struct.Struct('<e').unpack


@lru_cache(maxsize=None)
def to_float16(binary_data: bytes) -> Optional[float]:  # half
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _float16_unpack_be(binary_data)[0]
    else:
        return _float16_unpack_le(binary_data)[0]


_float16_pack_be = struct.Struct('>e').pack
_float16_pack_le = struct.Struct('<e').pack


@lru_cache(maxsize=None)
def from_float16(number: Optional[float]) -> bytes:  # half
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _float16_pack_be(number)
    else:
        return _float16_pack_le(number)


_float32_unpack_be = struct.Struct('>f').unpack
_float32_unpack_le = struct.Struct('<f').unpack


def to_float32(binary_data: bytes) -> Optional[float]:  # float / single
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _float32_unpack_be(binary_data)[0]
    else:
        return _float32_unpack_le(binary_data)[0]


_float32_pack_be = struct.Struct('>f').pack
_float32_pack_le = struct.Struct('<f').pack


def from_float32(number: Optional[float]) -> bytes:  # float / single
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _float32_pack_be(number)
    else:
        return _float32_pack_le(number)


_float64_unpack_be = struct.Struct('>d').unpack
_float64_unpack_le = struct.Struct('<d').unpack


def to_float64(binary_data: bytes) -> Optional[float]:  # double
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        return _float64_unpack_be(binary_data)[0]
    else:
        return _float64_unpack_le(binary_data)[0]


_float64_pack_be = struct.Struct('>d').pack
_float64_pack_le = struct.Struct('<d').pack


def from_float64(number: Optional[float]) -> bytes:  # double
    if number is None:
        return b''
    elif BIG_ENDIAN:
        return _float64_pack_be(number)
    else:
        return _float64_pack_le(number)


@lru_cache(maxsize=None)
def to_bool(binary_data: bytes) -> Optional[bool]:  # _Bool
    # if `False` is much more common than `None`
    # then swapping the checks will be faster by 0.02 microseconds
    if binary_data == b'':
        return None
    elif binary_data == b'\x00':
        return False
    elif len(binary_data) == 1:
        return True
    else:
        raise ValueError(binary_data)


@lru_cache(maxsize=None)
def from_bool(true_or_false: Optional[bool]) -> bytes:  # _Bool
    # WARNING: accepts non-bool truthy/falsy values, but faster than checking
    # if `Truthy` is much more common than `None`
    # then swapping the checks will be faster by 0.02 microseconds
    if true_or_false is None:
        return b''
    elif true_or_false:
        return b'\x01'
    else:
        return b'\x00'


@lru_cache(maxsize=None)
def from_bool_strict(true_or_false: Optional[bool]) -> bytes:  # _Bool
    # WARNING: about 0.1 microseconds slower than the non-strict version
    # if `True` or `False` are much more common than `None`
    # then swapping the 3 checks around will be faster by 0.02 - 0.1 microseconds
    if true_or_false is None:
        return b''
    elif true_or_false is True:
        return b'\x01'
    elif true_or_false is False:
        return b'\x00'
    else:
        raise ValueError(true_or_false)


def to_datetime32(binary_data: bytes) -> Optional[datetime.datetime]:
    # note: the spec is unclear about the signedness of the int32
    # I'm assuming it's signed because that's the more common standard worldwide
    # and because the spec does specify a few other types as being unsigned
    # so the lack of specification implies that this is not unsigned
    # the java parser we have also treats it as signed
    # tldr: we'll find out in year 2038
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        timestamp = _int32_unpack_be(binary_data)[0]
    else:
        timestamp = _int32_unpack_le(binary_data)[0]

    # we only need to worry about (signed) 32-bit timestamps
    # much faster but only handles 1969-12-31 12:00:00 onwards
    if timestamp >= -43200:
        return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)

    # slower, handles all 32-bit cases
    else:
        return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + \
               datetime.timedelta(seconds=timestamp)


def from_datetime32(timestamp: Optional[datetime.datetime]) -> bytes:
    # spec is unclear about the signedness of the int32
    if timestamp is None:
        return b''
    elif BIG_ENDIAN:
        if timestamp.tzinfo is None:
            return _int32_pack_be(round(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()))
        elif timestamp.tzinfo == datetime.timezone.utc:
            return _int32_pack_be(round(timestamp.timestamp()))
        else:
            return _int32_pack_be(round(timestamp.astimezone(datetime.timezone.utc).timestamp()))
    else:
        if timestamp.tzinfo is None:
            return _int32_pack_le(round(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()))
        elif timestamp.tzinfo == datetime.timezone.utc:
            return _int32_pack_le(round(timestamp.timestamp()))
        else:
            return _int32_pack_le(round(timestamp.astimezone(datetime.timezone.utc).timestamp()))


def to_datetime64(binary_data: bytes) -> Optional[datetime.datetime]:
    # assuming it's stored in a java-like way as milliseconds in a signed long long
    if binary_data == b'':
        return None
    elif BIG_ENDIAN:
        timestamp_millis = _int64_unpack_be(binary_data)[0]
    else:
        timestamp_millis = _int64_unpack_le(binary_data)[0]

    # much faster but only handles 1969-12-31 12:00:00 to 3001-01-19 22:00:00
    if -43200000 <= timestamp_millis <= 32536850399999.99804:  # the mysteries of floating point
        return datetime.datetime.fromtimestamp(timestamp_millis / 1000, tz=datetime.timezone.utc)

    # slower, handles 0001-01-01 00:00:00 to 9999-12-31 23:59:59
    elif -62135596800000 <= timestamp_millis <= 253402300799999.98437:  # more floating point weirdness
        return datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + \
               datetime.timedelta(milliseconds=timestamp_millis)

    # year <= 0 and year >= 10000 can't be handled by python datetime
    else:
        raise ValueError(timestamp_millis)


def from_datetime64(timestamp: Optional[datetime.datetime]) -> bytes:
    # assuming it's stored in a java-like way as milliseconds in a signed long long
    if timestamp is None:
        return b''
    elif BIG_ENDIAN:
        if timestamp.tzinfo is None:
            return _int64_pack_be(round(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000))
        elif timestamp.tzinfo == datetime.timezone.utc:
            return _int64_pack_be(round(timestamp.timestamp() * 1000))
        else:
            return _int64_pack_be(round(timestamp.astimezone(datetime.timezone.utc).timestamp() * 1000))
    else:
        if timestamp.tzinfo is None:
            return _int64_pack_le(round(timestamp.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000))
        elif timestamp.tzinfo == datetime.timezone.utc:
            return _int64_pack_le(round(timestamp.timestamp() * 1000))
        else:
            return _int64_pack_le(round(timestamp.astimezone(datetime.timezone.utc).timestamp() * 1000))


def to_uuid(binary_data: bytes) -> Optional[UUID]:
    if binary_data == b'':
        return None
    else:
        return UUID(bytes=binary_data)


def from_uuid(uuid: Optional[UUID]) -> bytes:
    if uuid is None:
        return b''
    else:
        return uuid.bytes  # 16 bytes


def to_ipv4(binary_data: bytes) -> Optional[IPv4Address]:
    if binary_data == b'':
        return None
    else:
        return IPv4Address(binary_data)


def from_ipv4(ipv4: Optional[IPv4Address]) -> bytes:
    if ipv4 is None:
        return b''
    else:
        return ipv4.packed  # 4 bytes


def to_ipv6(binary_data: bytes) -> Optional[IPv6Address]:
    if binary_data == b'':
        return None
    else:
        return IPv6Address(binary_data)


def from_ipv6(ipv6: Optional[IPv6Address]) -> bytes:
    if ipv6 is None:
        return b''
    else:
        return ipv6.packed  # 16 bytes


def to_ip(binary_data: bytes) -> Optional[Union[IPv4Address, IPv6Address]]:
    if binary_data == b'':
        return None
    elif len(binary_data) == 4:
        return IPv4Address(binary_data)
    else:
        return IPv6Address(binary_data)


def from_ip(ip: Optional[Union[IPv4Address, IPv6Address]]) -> bytes:
    if ip is None:
        return b''
    else:
        return ip.packed  # 4 or 16 bytes


def to_hex(binary_data: bytes) -> Optional[str]:
    if binary_data == b'':
        return None
    else:
        return binary_data.hex()


def from_hex(hex_str: Optional[str]) -> bytes:
    if hex_str is None:
        return b''
    else:
        return bytes.fromhex(hex_str)


CONVERT_FROM_BYTES: Dict[str, Callable] = {
    'hex':    to_bytes,  # to_hex
    'uuid':   to_uuid,
    'int':    to_integer32,
    'short':  to_unsigned_integer16,
    'byte':   to_unsigned_integer8,
    'time':   to_datetime32,
    'ip':     to_ip,
    'string': to_string,
}

CONVERT_TO_BYTES: Dict[str, Callable] = {
    'hex':    from_bytes,  # from_hex
    'uuid':   from_uuid,
    'int':    from_integer32,
    'short':  from_unsigned_integer16,
    'byte':   from_unsigned_integer8,
    'time':   from_datetime32,
    'ip':     from_ip,
    'string': from_string,
}

# sanity check
assert set(CONVERT_FROM_BYTES.keys()) == set(CONVERT_TO_BYTES.keys())
import datetime
from enum import Enum
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel
from pydantic import conint


class ContentType(Enum):
    STRING = 1
    BINARY = 2


class Metadata(BaseModel):
    sender_uuid: UUID
    recipient_uuid: UUID
    sent_timestamp: datetime.datetime  # todo: this has to be unique per-packet
    protocol_version: conint(ge=1) = 1


class Message(BaseModel):
    message_id: conint(ge=1)
    content_type: ContentType
    content: bytes


class Control(BaseModel):
    sender_clock_sender: conint(ge=0)
    sender_clock_recipient: conint(ge=0)
    sender_clock_out_of_order: List[conint(ge=1)]
    recipient_clock_sender: conint(ge=0)
    nack_hashes: List[str]


class Packet(BaseModel):
    metadata: Metadata
    messages: List[Message]
    control: Optional[Control] = None

import datetime
from enum import Enum
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class ContentType(Enum):
    STRING = 1
    BINARY = 2


class Metadata(BaseModel):
    sender_uuid: UUID
    recipient_uuid: UUID
    sent_timestamp: datetime.datetime
    protocol_version: int = 1


class Message(BaseModel):
    message_id: int  # more than zero
    content_type: ContentType
    content: bytes


class Control(BaseModel):
    sender_clock_sender: int
    sender_clock_recipient: int
    sender_clock_out_of_order: List[int]
    recipient_clock_sender: int
    nack_hashes: List[str]


class Packet(BaseModel):
    metadata: Metadata
    messages: List[Message]
    control: Optional[Control] = None

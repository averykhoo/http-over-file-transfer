from dataclasses import dataclass
from dataclasses import field
from typing import Optional
from typing import Union


@dataclass
class DataCursor:
    data: Union[bytes, bytearray]
    __cursor: int = field(default=0)

    def read(self, length: int, hash_object: Optional = None) -> bytes:
        if length < 0:
            raise ValueError
        if length > len(self.data) - self.__cursor:
            raise IndexError

        self.__cursor += length
        out = bytes(self.data[self.__cursor - length: self.__cursor])

        if hash_object is not None:
            hash_object.update(out)
        return out

    # def seek(self, position: int) -> None:
    #     assert 0 <= position <= len(self.data)
    #     self.__cursor = position
    #
    # def tell(self) -> int:
    #     return self.__cursor
    #
    # def __len__(self):
    #     return len(self.data)

    def at_end(self):
        return self.__cursor == len(self.data)

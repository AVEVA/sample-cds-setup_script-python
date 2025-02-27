import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Callable

from omf_sample_library_preview.Models import OMFContainer, OMFData, OMFType


class StreamReader(ABC):
    @abstractmethod
    def __init__(self, id: str, name: str):
        self.__id = id
        self.__name = name
        self.__observers = set()

    @property
    def id(self) -> str:
        return self.__id

    @id.setter
    def id(self, value: str):
        self.__id = value

    @property
    def name(self) -> str:
        return self.__name

    @name.setter
    def name(self, value: str):
        self.__name = value

    @property
    def observers(self):
        return self.__observers

    def bind_observer(self, callback: Callable):
        self.__observers.add(callback)

    @abstractmethod
    def get_stream(self) -> OMFContainer:
        pass

    @abstractmethod
    def get_type(self) -> OMFType:
        pass

    @abstractmethod
    def read_streaming_data(self, now: datetime) -> Iterator[OMFData]:
        pass
    
    @staticmethod
    @abstractmethod
    def fromJson(content: dict[str, Any]) -> 'StreamReader':
        pass

    def toJson(self):
        return json.dumps(self.toDictionary(), indent=2)

    @abstractmethod
    def toDictionary(self) -> dict[str, Any]:
        pass

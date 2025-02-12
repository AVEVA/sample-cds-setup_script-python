import json
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from adh_sample_library_preview import (
    AuthorizationTag,
    BaseEvent,
    Enumeration,
    EventType,
)

from ..GraphData import GraphData


class EventReader(ABC):
    @abstractmethod
    def __init__(
        self,
        id: str,
        name: str,
        reference_asset: str = None,
        observing: list[str] = None,
    ):
        self.__id = id
        self.__name = name
        self.__reference_asset = reference_asset
        self.__observing = observing

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
    def reference_asset(self) -> str:
        return self.__reference_asset

    @reference_asset.setter
    def reference_asset(self, value: str):
        self.__reference_asset = value

    @property
    def observing(self) -> list[str]:
        return self.__observing

    @abstractmethod
    def get_authorization_tags(self) -> list[AuthorizationTag]:
        pass

    @abstractmethod
    def get_enumerations(self) -> list[Enumeration]:
        pass

    @abstractmethod
    def get_type(self) -> EventType:
        pass

    @abstractmethod
    def read_streaming_events(self, now: datetime) -> Iterator[GraphData[BaseEvent]]:
        pass

    @staticmethod
    @abstractmethod
    def fromJson(content: dict[str, Any]) -> 'EventReader':
        pass

    def toJson(self):
        return json.dumps(self.toDictionary(), indent=2)

    @abstractmethod
    def toDictionary(self) -> dict[str, Any]:
        pass

import uuid
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Generic, TypeVar

from adh_sample_library_preview import (
    AuthorizationTag,
    BaseEvent,
    Enumeration,
    EventType,
    SdsUom,
)

from data_types import EventTypeEnum, AuthorizationTagEnum, EnumEnum

from ..CSVTransformer import CSVTransformer
from ..CSVIterator import CSVIterator
from ..GraphData import GraphData
from .BackfillableEventReader import BackfillableEventReader

T = TypeVar('T')


class CSVEventReader(BackfillableEventReader, Generic[T]):
    def __init__(
        self,
        id: str,
        name: str,
        file_path: str,
        event_class: type,
        event_type: EventType,
        authorization_tags: list[AuthorizationTag] = None,
        enumerations: list[Enumeration] = None,
        units_of_measure: dict[str, SdsUom] = None,
        reference_asset: str = None,
    ):
        super().__init__(id, name, reference_asset)
        self.__event_class = event_class
        self.__event_type = event_type
        self.__authorization_tags = authorization_tags if authorization_tags else []
        self.__enumerations = enumerations if enumerations else []
        self.__file_path = file_path
        self.__units_of_measure = units_of_measure
        self.__transformer = CSVTransformer(
            self.__event_class,
            units_of_measure=units_of_measure,
        )
        self.__streaming_event_iterator = CSVIterator(
            data_file_path = file_path,
            transformer = self.__transformer,
            index_field = 'StartTime',
        )
        self.__backfill_event_iterator = CSVIterator(
            data_file_path = file_path,
            transformer = self.__transformer,
            index_field = 'StartTime',
            file_data_number_of_rows = self.__streaming_event_iterator.get_file_data_number_of_rows(),
            file_data_end = self.__streaming_event_iterator.get_file_data_end()
        )

        self.__open_events = []
        self.__current_event = None

    def get_authorization_tags(self) -> list[AuthorizationTag]:
        return self.__authorization_tags

    def get_enumerations(self) -> list[Enumeration]:
        return self.__enumerations

    def get_type(self) -> EventType:
        return self.__event_type

    def __get_events(
        self, event_iterator: CSVIterator, start_time: datetime, end_time: datetime
    ) -> Iterator[T]:
        last_time = start_time
        while end_time > last_time:
            next_event = next(event_iterator, None)
            next_event: BaseEvent = self.__event_class(**next_event)
            last_time = next_event.StartTime

            if self.__current_event:
                self.__open_events.append(self.__current_event)

            self.__current_event = next_event

        event: BaseEvent
        for event in self.__open_events:
            # assign a random id and name if the event does not have one
            if not event.Id:
                event.Id = str(uuid.uuid4())
            if not event.Name:
                event.Name = str(uuid.uuid4())

            if event.EndTime < end_time:
                yield event
                self.__open_events.remove(event)
            else:
                yield BaseEvent(
                        Id=event.Id,
                        Name=event.Name,
                        StartTime=event.StartTime)

    def read_streaming_events(self, now: datetime) -> Iterator[GraphData[T]]:
        # trim subseconds off of input datetimes for clarity
        now = now.replace(microsecond=0)
        if not self.__streaming_event_iterator.current_value_time:
            current_value_time = self.__streaming_event_iterator.seek_to_first_event_prior_to(now)
        else:
            current_value_time = self.__streaming_event_iterator.current_value_time

        for value in self.__get_events(self.__streaming_event_iterator, current_value_time, now):
            yield GraphData([value], self.__event_type.Id)

    def read_backfill_events(
        self, start_time: datetime, end_time: datetime
    ) -> Iterator[GraphData[T]]:
        # trim subseconds off of input datetimes for clarity
        start_time = start_time.replace(microsecond=0)
        end_time = end_time.replace(microsecond=0)

        current_value_time = self.__backfill_event_iterator.seek_to_first_event_prior_to(start_time)
        for value in self.__get_events(self.__backfill_event_iterator, current_value_time, end_time):
            yield GraphData([value], self.__event_type.Id)

    @staticmethod
    def fromJson(content: dict[str, Any]) -> 'CSVEventReader':
        event_class = EventTypeEnum[content['EventClass']].value[0]
        event_type = EventTypeEnum[content['EventClass']].value[1]

        authorization_tags = None
        if 'AuthorizationTags' in content and content['AuthorizationTags']:
            authorization_tags = [
                AuthorizationTagEnum[authorization_tag].value
                for authorization_tag in content['AuthorizationTags']
            ]

        enumerations = None
        if 'Enumerations' in content and content['Enumerations']:
            enumerations = [
                EnumEnum[enumeration].value for enumeration in content['Enumerations']
            ]

        units_of_measure = None
        if 'UnitsOfMeasure' in content and content['UnitsOfMeasure']:
            units_of_measure = {
                k: SdsUom.fromJson(uom) for k, uom in content['UnitsOfMeasure'].items()
            }

        result = CSVEventReader(
            content['Id'],
            content['Name'],
            content['FilePath'],
            event_class,
            event_type,
            authorization_tags,
            enumerations,
            units_of_measure,
            content['ReferenceAsset'],
        )

        return result

    def toDictionary(self) -> dict[str, Any]:
        authorization_tags = None
        if self.__authorization_tags:
            authorization_tags = [
                authorization_tag.toDictionary()
                for authorization_tag in self.__authorization_tags
            ]

        enumerations = None
        if self.__enumerations:
            enumerations = [
                enumeration.toDictionary() for enumeration in self.__enumerations
            ]

        units_of_measure = None
        if self.__units_of_measure:
            units_of_measure = {
                k: uom.toDictionary() for k, uom in self.__units_of_measure.items()
            }

        result = {
            'Reader': 'CSVEventReader',
            'Id': self.id,
            'Name': self.name,
            'FilePath': self.__file_path,
            'EventClass': EventTypeEnum((self.__event_class, self.__event_type)).name,
            'AuthorizationTags': authorization_tags,
            'Enumerations': enumerations,
            'UnitsOfMeature': units_of_measure,
            'ReferenceAsset': self.reference_asset,
        }

        return result

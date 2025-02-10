from collections.abc import Iterator
from datetime import datetime, timedelta
from random import random
from typing import Any

from omf_sample_library_preview.Converters import convert
from omf_sample_library_preview.Models import (
    OMFContainer,
    OMFData,
    OMFFormatCode,
    OMFType,
)

from data_types import StreamTypeEnum

from .BackfillableStreamReader import BackfillableStreamReader


class RandomStreamReader(BackfillableStreamReader):
    def __init__(
        self,
        id: str,
        name: str,
        data_class: type,
        omf_type: OMFType,
        interval: timedelta,
    ):
        super().__init__(id, name)
        self.__data_class = data_class
        self.__omf_type = omf_type
        self.__interval = interval
        self.__last_data_time = None

    def get_stream(self) -> OMFContainer:
        return OMFContainer(self.id, self.__omf_type.Id, self.name)

    def get_type(self) -> OMFType:
        return self.__omf_type

    def __get_next_value(self, timestamp: datetime) -> OMFData:
        value = {'Timestamp': timestamp, 'Value': random()}
        return OMFData[self.__data_class](
            [self.__data_class(**value)], ContainerId=self.id
        )

    def __get_values(
        self, start_time: datetime, end_time: datetime, interval: timedelta
    ) -> Iterator[(datetime, OMFData)]:
        last_time = start_time
        while end_time > last_time + interval:
            last_time += interval
            yield last_time, self.__get_next_value(last_time)

    def read_streaming_data(self, now: datetime) -> Iterator[OMFData]:
        if not self.__last_data_time:
            self.__last_data_time = now

        for last_time, value in self.__get_values(
            self.__last_data_time, now, self.__interval
        ):
            self.__last_data_time = last_time
            yield value

    def read_backfill_data(
        self, start_time: datetime, end_time: datetime
    ) -> Iterator[OMFData]:
        for _, value in self.__get_values(start_time, end_time, self.__interval):
            yield value

    @staticmethod
    def fromJson(content: dict[str, Any]) -> 'RandomStreamReader':
        data_class = StreamTypeEnum[content['DataClass']].value
        result = RandomStreamReader(
            content['Id'],
            content['Name'],
            data_class,
            convert(data_class),
            timedelta(seconds=content['Interval']),
        )

        return result

    def toDictionary(self) -> dict[str, Any]:
        result = {
            'Reader': 'RandomStreamReader',
            'Id': self.id,
            'Name': self.name,
            'DataClass': StreamTypeEnum(self.__data_class).name,
            'Interval': self.__interval.total_seconds(),
        }

        return result

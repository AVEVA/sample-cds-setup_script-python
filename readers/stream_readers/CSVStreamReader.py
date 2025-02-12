from collections.abc import Iterator
from datetime import datetime
from typing import Any

from omf_sample_library_preview.Converters import convert
from omf_sample_library_preview.Models import (
    OMFContainer,
    OMFData,
    OMFFormatCode,
    OMFType,
)

from data_types import StreamTypeEnum

from ..CSVTransformer import CSVTransformer
from .BackfillableStreamReader import BackfillableStreamReader
from ..CSVIterator import CSVIterator


class CSVStreamReader(BackfillableStreamReader):
    def __init__(
        self, id: str, name: str, file_path: str, data_class: type, omf_type: OMFType
    ):
        super().__init__(id, name)
        self.__file_path = file_path
        self.__data_class = data_class
        self.__omf_type = omf_type

        self.__index_property = next(
            iter(
                [
                    id
                    for id, prop in self.__omf_type.Properties.items()
                    if prop.IsIndex and prop.Format is OMFFormatCode.DateTime
                ]
            )
        )

        self.__csv_transformer = CSVTransformer(self.__data_class)

        # TODO: For now use a single streaming and single backfill iterator.  However
        # in the future can implement these as an array of iterators where [0] is streaming 
        # and the rest can be pooled for parallel backfill tasks
        self.__streaming_data_iterator = CSVIterator(
            self.__file_path, self.__csv_transformer, self.__index_property
        )
        self.__backfill_data_iterator = CSVIterator(
            self.__file_path, self.__csv_transformer, self.__index_property
        )

    def get_stream(self) -> OMFContainer:
        return OMFContainer(self.id, self.__omf_type.Id, self.name)

    def get_type(self) -> OMFType:
        return self.__omf_type

    def __get_values(
        self, stream_data_iterator: CSVIterator, start_time: datetime, end_time: datetime
    ) -> Iterator[OMFData]:
        last_time = start_time
        while end_time > last_time:
            next_row = next(stream_data_iterator, None)
            current_value = self.__data_class(**next_row)
            last_time = getattr(current_value, self.__index_property)

            yield OMFData[self.__data_class]([current_value], ContainerId=self.id)

    def read_streaming_data(self, now: datetime) -> Iterator[OMFData]:
        # trim subseconds off of input datetimes for clarity
        now = now.replace(microsecond=0)
        
        # if no current value time then the iterator has never been iterated, so we need to seek to appropriate starting point
        if not self.__streaming_data_iterator.current_value_time:
            current_value_time = self.__streaming_data_iterator.seek_to_first_event_prior_to(now)
        else:
            current_value_time = self.__streaming_data_iterator.current_value_time

        for value in self.__get_values(self.__streaming_data_iterator, current_value_time, now):
            for observer in self.observers:
                observer(value)
            yield value

    def read_backfill_data(self, start_time: datetime, end_time: datetime) -> Iterator[OMFData]:
        # trim subseconds off of input datetimes for clarity
        start_time = start_time.replace(microsecond=0)
        end_time = end_time.replace(microsecond=0)
        current_value_time = self.__backfill_data_iterator.seek_to_first_event_prior_to(start_time)

        for value in self.__get_values(self.__backfill_data_iterator, current_value_time, end_time):
            for observer in self.observers:
                observer(value)
            yield value

    @staticmethod
    def fromJson(content: dict[str, Any]) -> 'CSVStreamReader':
        data_class = StreamTypeEnum[content['DataClass']].value
        result = CSVStreamReader(
            content['Id'],
            content['Name'],
            content['FilePath'],
            data_class,
            convert(data_class),
        )

        return result

    def toDictionary(self) -> dict[str, Any]:
        result = {
            'Reader': 'CSVStreamReader',
            'Id': self.id,
            'Name': self.name,
            'FilePath': self.__file_path,
            'DataClass': StreamTypeEnum(self.__data_class).name,
        }

        return result

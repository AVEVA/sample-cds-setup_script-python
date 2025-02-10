import copy
import csv
import itertools
from datetime import timedelta

from ..CSVTransformer import CSVTransformer
import global_settings

class CSVStreamReaderCache:
    def __init__(
        self,
        data_file_path: str,
        transformer: CSVTransformer,
        index_field: str = None,
        max_cache_size: int = 1000,
        should_loop=True,
    ):
        self.__data_file_path = data_file_path
        self.__transformer = transformer
        self.__max_cache_length = max_cache_size
        self.__file_data_index = 0
        self.__cache = []
        self.__cache_index = 0
        self.__datetime_fields = set()
        self.__should_loop = should_loop
        self.__index_field = index_field
        self.__file_data_start = None
        self.__cache_data_end = None
        self.__time_offset_to_now = None
        self.__populate_cache()
        self.__file_data_start = (
            self.__cache[0].get(self.__index_field) if index_field and not (global_settings.application_mode is global_settings.ApplicationMode.Setup
        or global_settings.application_mode is global_settings.ApplicationMode.Cleanup) else None
        )

    def __iter__(self):
        return self

    def get_data_start(self):
        return self.__file_data_start

    @property
    def offset(self):
        return self.__time_offset_to_now

    @offset.setter
    def offset(self, value: timedelta):
        self.__time_offset_to_now = value

    def __populate_cache(self):
        if (global_settings.application_mode is global_settings.ApplicationMode.Setup
        or global_settings.application_mode is global_settings.ApplicationMode.Cleanup):
            return

        self.__cache.clear()

        with open(self.__data_file_path, newline='') as file:
            reader = csv.DictReader(file)
            reader = itertools.islice(reader, self.__file_data_index, self.__file_data_index + self.__max_cache_length)
            for row in reader:
                transformed_row, self.__datetime_fields = self.__transformer.transform_row(row)
                self.__cache.append(transformed_row)
                self.__cache_data_end = self.__cache[-1].get(self.__index_field)
                self.__file_data_index += 1

    def __next__(self):
        if self.__cache_index >= len(self.__cache):
            # We reached the end of our cache.  Reset the cache index to 0 and load next file chunk into cache
            self.__cache_index = 0
            self.__populate_cache()

            if len(self.__cache) == 0 and self.__should_loop:
                # We reached the end of the file and are configured to loop.  Reset the file index to 0, update the time offset 
                # and start reading again from the beginning of the file
                self.__file_data_index = 0
                self.__time_offset_to_now += self.__cache_data_end - self.__file_data_start
                self.__populate_cache()

        if self.__cache_index < len(self.__cache):
            row = self.__cache[self.__cache_index]

            # Add time offset to datetime fields
            row_copy = copy.deepcopy(row)
            if self.__time_offset_to_now:
                for datetime_field in self.__datetime_fields:
                    row_copy[datetime_field] += self.__time_offset_to_now

            self.__cache_index += 1
            return row_copy
        else:
            raise StopIteration

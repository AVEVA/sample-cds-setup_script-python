import copy
import csv
import itertools
from datetime import timedelta, datetime, timezone

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
        self.__file_data_end = None
        self.__time_offset_to_now = None
        self.__populate_cache()
        self.__file_data_start = (
            self.__cache[0].get(self.__index_field) if index_field and not (global_settings.application_mode is global_settings.ApplicationMode.Setup
        or global_settings.application_mode is global_settings.ApplicationMode.Cleanup) else None
        )
        self.__file_data_end = self.__get_file_data_end()

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

    def __populate_cache(self, target_index = None):
        if (global_settings.application_mode is global_settings.ApplicationMode.Setup
        or global_settings.application_mode is global_settings.ApplicationMode.Cleanup):
            return

        self.__cache.clear()

        with open(self.__data_file_path, newline='') as file:
            reader = csv.DictReader(file)
            target_cache_index = self.__file_data_index if target_index is None else target_index
            reader = itertools.islice(reader, target_cache_index, target_cache_index + self.__max_cache_length)
            for row in reader:
                transformed_row, self.__datetime_fields = self.__transformer.transform_row(row)
                self.__cache.append(transformed_row)
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
                self.__time_offset_to_now += self.__calculate_data_set_duration()
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
        
    def __get_file_data_end(self):
        if self.__file_data_end is None:
            with open(self.__data_file_path, newline='') as file:
                reader = csv.DictReader(file)
                for row in reader: pass

            last_line, _ = self.__transformer.transform_row(row)
            self.__file_data_end = last_line.get(self.__index_field)

            return self.__file_data_end

    # Datasets assume that the first value in the data file is at time epoch 0 UTC
    # Find where the starttime would land in the dataset had it continued forever
    # And use this as starting point for reading data
    def seek_to_first_event_prior_to(self, start_time: datetime):
        # calculate expected loops and offset
        expected_time_offset = self.__calculate_expected_dataset_time_offset(start_time)

        target_row_index = 0
        file_time_offset_from_target = timedelta(seconds=0)
        file_first_event_timestamp = self.get_data_start()
        current_timestamp = file_first_event_timestamp

        # iterate until we go over the expected offset
        while (current_timestamp - file_first_event_timestamp) < expected_time_offset:
            current_timestamp = self.__next__().get(self.__index_field)
            if (current_timestamp - file_first_event_timestamp) < expected_time_offset:
                file_time_offset_from_target = current_timestamp - file_first_event_timestamp
                target_row_index += 1

        # calculate timestamp of event just prior to stepping over the expected offset
        dataset_duration = self.__calculate_data_set_duration()
        dataset_loops = self.__calculate_expected_dataset_loops(start_time)
        previous_value_timestamp = self.calculate_previous_value_timestamp(dataset_duration, dataset_loops, file_time_offset_from_target)

        # reset the reader state to the correct point
        self.__reset_reader_cache(target_row_index)
        self.__time_offset_to_now = start_time - self.__file_data_start

        return previous_value_timestamp
    
    def calculate_previous_value_timestamp(self, dataset_duration: timedelta, dataset_loops: int, file_time_offset: timedelta) -> datetime:
        return self.epoch_zero_utc() + (dataset_duration * dataset_loops) + file_time_offset
    
    def __calculate_expected_dataset_time_offset(self, start_time: datetime) -> timedelta:
        return (start_time - self.epoch_zero_utc()) % (self.__calculate_data_set_duration())
    
    def __calculate_expected_dataset_loops(self, start_time: datetime) -> int:
        return (start_time - self.epoch_zero_utc()) // (self.__calculate_data_set_duration())

    def __calculate_data_set_duration(self) -> timedelta:
        return self.__file_data_end - self.__file_data_start
    
    def epoch_zero_utc(self) -> datetime:
        return datetime.fromtimestamp(0, timezone.utc)

    def __reset_reader_cache(self, target_row_index):
        self.__file_data_index = target_row_index
        self.__populate_cache(target_row_index)
        self.__cache_index = 0

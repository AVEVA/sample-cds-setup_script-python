import copy
import csv
import itertools
from datetime import timedelta, datetime, timezone
from itertools import islice

from .CSVTransformer import CSVTransformer
import global_settings

class CSVIterator:
    def __init__(
        self,
        data_file_path: str,
        transformer: CSVTransformer,
        index_field: str = None,
        max_cache_size: int = 1000,
        should_loop=True,
        file_data_number_of_rows=None,
        file_data_end=None
    ):
        # data file settings and state
        self.__data_file_path = data_file_path
        self.__transformer = transformer
        self.__index_field = index_field
        self.__datetime_fields = set()
        self.__should_loop = should_loop
        self.__file_data_index = 0
        self.__file_data_first_timestamp = None
        self.__file_data_end = file_data_end
        self.__file_data_number_of_rows = file_data_number_of_rows
        self.__current_row = None
        # cache settings and state
        self.__cache = []
        self.__cache_index = 0
        self.__max_cache_length = max_cache_size
        self.__time_offset_to_now = None
        self.__populate_cache()
        self.__file_data_first_timestamp = (
            self.__cache[0].get(self.__index_field) if index_field and not (global_settings.application_mode is global_settings.ApplicationMode.Setup
        or global_settings.application_mode is global_settings.ApplicationMode.Cleanup) else None
        )
        if self.__should_loop and (self.__file_data_end == None and self.__file_data_number_of_rows == None):
            self.__file_data_end, self.__file_data_number_of_rows = self.__get_file_data_details()

    def __iter__(self):
        return self
    
    def get_data_start(self):
        return self.__file_data_first_timestamp
    
    def get_file_data_end(self):
        return self.__file_data_end
    
    def get_file_data_number_of_rows(self):
        return self.__file_data_number_of_rows
          
    @property
    def current_value_time(self):
        return self.__current_row.get(self.__index_field) if self.__current_row is not None else None

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
                self.__time_offset_to_now += self.__calculate_data_file_duration()
                self.__populate_cache()

        if self.__cache_index < len(self.__cache):
            row = self.__cache[self.__cache_index]

            # Add time offset to datetime fields
            row_copy = self.__apply_time_offset_to_datetime_fields(row)

            self.__cache_index += 1
            self.__current_row = row_copy
            return row_copy
        else:
            raise StopIteration
        
    # in order to properly calculate where in the data file we need to start iterating, as well as edge cases 
    # such as needing to start at the first event in the file, we need to get the last event timestamp in the file, 
    # as well as the total number of data rows in the file. This must be done for each dataset .csv file and can take
    # some time for large datasets (1000+ files)
    #   1. The timestamp of the last event in the file
    #   2. The total number of rows in the data file
    def __get_file_data_details(self):
        if self.__file_data_end is None:
            self.__file_data_number_of_rows = 0
            
            # iterate through file quickly to find number of rows and the last row
            with open(self.__data_file_path, newline='') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    self.__file_data_number_of_rows += 1
                
                last_row, _ = self.__transformer.transform_row(row)
                self.__file_data_end = last_row.get(self.__index_field)

            return self.__file_data_end, self.__file_data_number_of_rows
               
    def __apply_time_offset_to_datetime_fields(self, row):
        if not row:
            return None
        row_copy = copy.deepcopy(row)
        if self.__time_offset_to_now:
            for datetime_field in self.__datetime_fields:
                row_copy[datetime_field] += self.__time_offset_to_now
        return row_copy

    # Datasets assume that the first value in the data file is at time epoch 0 UTC
    # and all remaining data points are offset from there.  Using this fixed reference
    # we can calculate where in the dataset we should start reading from for any given start time
    def seek_to_first_event_prior_to(self, start_time: datetime):
        # calculate expected time offset in the dataset
        target_time_offset_in_data_file = self.__calculate_target_time_offset_in_dataset(start_time)

        data_file_target_index = 0
        data_file_time_offset_to_target = timedelta(seconds=0)
        data_file_first_event_timestamp = self.__file_data_first_timestamp
        current_timestamp = data_file_first_event_timestamp

        # edge case, but it is possible that we line up with the very first event in the file, in which case
        # we need to reset back to the last event in the file on the previous loop.  In addition, we need to 
        # adjust the offset which means we need to know the time difference between the last two rows in the file
        if target_time_offset_in_data_file == timedelta():
            last_two_rows_time_difference = self.__get_last_two_rows_time_difference()
            data_file_target_index = self.__file_data_number_of_rows - 1
            start_time = start_time - last_two_rows_time_difference
            data_file_time_offset_to_target = data_file_time_offset_to_target - last_two_rows_time_difference
            
        else:
            # iterate until we pass the target time offset, and remember the index immediately before
            while (current_timestamp - data_file_first_event_timestamp) < target_time_offset_in_data_file:
                current_timestamp = self.__next__().get(self.__index_field)
                if (current_timestamp - data_file_first_event_timestamp) < target_time_offset_in_data_file:
                    data_file_time_offset_to_target = current_timestamp - data_file_first_event_timestamp
                    data_file_target_index += 1

        # set the iterator state to the index we found in the dataset
        self.__set_iterator_cache(data_file_target_index)

        # the time offsets are updated on each cache update, so the offset must be set to the beginning of the
        # cache that we are now in the middle of
        self.__time_offset_to_now = self.__calculate_cache_time_offset_for_timestamp(
            start_time = start_time,
            data_file_first_event_timestamp = data_file_first_event_timestamp
        )

        # apply time offset to the current_row since we didn't know the proper offset on first iteration
        self.__current_row = self.__apply_time_offset_to_datetime_fields(self.__current_row)

        # calculate and return the full timestamp of event just prior to stepping over the target offset and return
        return self.calculate_previous_value_timestamp_before_start_time(
            start_time = start_time,
            data_file_time_offset = data_file_time_offset_to_target
        )
        
    def calculate_previous_value_timestamp_before_start_time(self, start_time: datetime, data_file_time_offset: timedelta) -> datetime:
        return self.epoch_zero_utc() + (self.__calculate_data_file_duration() * self.__calculate_expected_data_file_loops(start_time)) + data_file_time_offset
    
    def __calculate_cache_time_offset_for_timestamp(self, start_time: datetime, data_file_first_event_timestamp: datetime) -> timedelta:
        return (self.__calculate_data_file_duration() * self.__calculate_expected_data_file_loops(start_time)) - (data_file_first_event_timestamp - self.epoch_zero_utc())

    # Use modulo to calculate the target offset in dataset we would be at had we started
    # writing data at epoch zero
    def __calculate_target_time_offset_in_dataset(self, start_time: datetime) -> timedelta:
        return (start_time - self.epoch_zero_utc()) % (self.__calculate_data_file_duration())
    
    # Use "integer" division to calculate the total number of times the dataset would have 
    # looped between epoch zero and starttime
    def __calculate_expected_data_file_loops(self, start_time: datetime) -> int:
        return (start_time - self.epoch_zero_utc()) // (self.__calculate_data_file_duration())

    def __calculate_data_file_duration(self) -> timedelta:
        return self.__file_data_end - self.__file_data_first_timestamp
    
    def epoch_zero_utc(self) -> datetime:
        return datetime.fromtimestamp(0, timezone.utc)

    def __set_iterator_cache(self, target_row_index):
        self.__file_data_index = target_row_index
        self.__populate_cache(target_row_index)
        self.__cache_index = 0

    def __get_last_two_rows_time_difference(self):
        with open(self.__data_file_path, newline='') as file:
            reader = csv.DictReader(file)
            last_two_rows_iterator = islice(reader, self.__file_data_number_of_rows - 2, None, None)
            second_to_last_row, _ = self.__transformer.transform_row(next(last_two_rows_iterator))
            return self.__file_data_end - second_to_last_row.get(self.__index_field)

from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime

from omf_sample_library_preview.Models import OMFData

from .StreamReader import StreamReader


class BackfillableStreamReader(StreamReader, ABC):
    @abstractmethod
    def read_backfill_data(
        self, start_time: datetime, end_time: datetime
    ) -> Iterator[OMFData]:
        pass

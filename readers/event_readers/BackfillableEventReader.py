from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime

from adh_sample_library_preview import BaseEvent

from ..GraphData import GraphData
from .EventReader import EventReader


class BackfillableEventReader(EventReader, ABC):
    @abstractmethod
    def read_backfill_events(
        self, start_time: datetime, end_time: datetime
    ) -> Iterator[GraphData[BaseEvent]]:
        pass

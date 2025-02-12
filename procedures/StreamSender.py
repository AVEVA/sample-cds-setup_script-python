import time
from collections import deque
from datetime import datetime, timedelta, timezone

from omf_sample_library_preview.Client import OMFClient
from omf_sample_library_preview.Models import OMFData
from omf_sample_library_preview.Services import DataService

from readers.stream_readers import BackfillableStreamReader, StreamReader

from .EventRateCounter import EventRateCounter
from .helpers import *


def _send(
    clients : ADHOMFClients,
    queue: deque,
    events_to_send: int,
    event_rate_counter: EventRateCounter = None,
):
    payload = {}
    for _ in range(events_to_send):
        if len(queue) == 0:
            continue

        event: OMFData = queue.pop()
        if event.ContainerId not in payload:
            payload[event.ContainerId] = event
        else:
            payload[event.ContainerId].Values += event.Values

    if event_rate_counter:
        for _, values in payload.items():
            event_rate_counter.add_events_sent(len(values.Values))

    payload = [values for _, values in payload.items()]

    for client in clients.Clients:
        data_service = DataService(client)
        data_service.updateData(payload)


def start(
    omf_clients: ADHOMFClients,
    readers: StreamReader,
    read_interval: timedelta,
    backfill_start: datetime,
    backfill_end: datetime,
    event_rate_counter: EventRateCounter = None,
    send_period: int = 30,
    max_events: int = 10000,
    max_queue_length: int = 10000,
):
    """
    Starts stream sender procedure.
    Backfills data and continuously reads from readers in hierarchy and sends data through the provided OMF client.
    :param omf_client: omf client used to send OMF messages
    :param readers: data readers to read from
    :param read_interval: interval to wait between reading data points
    :param backfill_start: start time for backfill
    :param backfill_end: end time for backfill
    :param send_period: maximum time to wait before sending the next OMF data message
    :param max_events: maximum number of events to send per OMF data message
    :param max_queue_length: maximum queue length
    """   

    queue = deque(maxlen=max_queue_length)
    reader: StreamReader
    if False:
        for reader in readers:
            if isinstance(reader, BackfillableStreamReader):
                for data in reader.read_backfill_data(backfill_start, backfill_end):
                    queue.appendleft(data)

                    while len(queue) >= max_events:
                        _send(omf_clients, queue, max_events, event_rate_counter)

        _send(omf_clients, queue, max_events, event_rate_counter)

    timer = time.time()
    while True:
        for reader in readers:
            #now = datetime.fromisoformat('2025-02-12T12:00:00Z')
            now = datetime.now(timezone.utc)
            for data in reader.read_streaming_data(now):
                queue.appendleft(data)

                while len(queue) >= max_events:
                    _send(omf_clients, queue, max_events, event_rate_counter)
                    timer = time.time()

        if time.time() - timer > send_period:
            try:
                _send(omf_clients, queue, max_events, event_rate_counter)
            except Exception as error:
                print(error)            
                
            timer = time.time()

        time.sleep(read_interval.total_seconds())

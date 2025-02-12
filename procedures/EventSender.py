import time
from collections import deque
from datetime import datetime, timedelta, timezone

from adh_sample_library_preview import ADHClient

from readers.event_readers import BackfillableEventReader, EventReader

from readers.GraphData import GraphData
from .EventRateCounter import EventRateCounter


def _send(
    adh_clients: list[ADHClient],
    namespace_ids: list[str],
    queue: deque,
    events_to_send: int,
    event_rate_counter: EventRateCounter = None,
):
    payload = {}
    for _ in range(events_to_send):
        if len(queue) == 0:
            continue

        data: GraphData = queue.pop()
        if data.TypeId not in payload:
            payload[data.TypeId] = data.Data
        else:
            payload[data.TypeId] += data.Data

    if event_rate_counter:
        for _, values in payload.items():
            event_rate_counter.add_events_sent(len(values))

    for type_id, events in payload.items():
        for index,adh_client in enumerate(adh_clients):
            adh_client.Events.getOrCreateEvents(namespace_ids[index], type_id, events)


def start(
    adh_clients: list[ADHClient],
    namespace_ids: list[str],
    readers: EventReader,
    read_interval: timedelta,
    backfill_start: datetime,
    backfill_end: datetime,
    event_rate_counter: EventRateCounter = None,
    send_period: int = 30,
    max_events: int = 1000,
    max_queue_length: int = 10000,
):
    """
    Starts event sender procedure.
    Backfills data and continuously reads from readers in hierarchy and sends data through the provided ADH client.
    :param adh_client: adh client used to send events
    :param namespace_id: namespace to send events to
    :param readers: data readers to read from
    :param read_interval: interval to wait between reading data points
    :param backfill_start: start time for backfill
    :param backfill_end: end time for backfill
    :param send_period: maximum time to wait before sending the next data message
    :param max_events: maximum number of events to send per data message
    :param max_queue_length: maximum queue length
    """
    queue = deque(maxlen=max_queue_length)
    reader: EventReader
    for reader in readers:
        if isinstance(reader, BackfillableEventReader):
            for data in reader.read_backfill_events(backfill_start, backfill_end):
                queue.appendleft(data)

                while len(queue) >= max_events:
                    _send(
                        adh_clients, namespace_ids, queue, max_events, event_rate_counter
                    )

    timer = time.time()
    while True:
        for reader in readers:
            for data in reader.read_streaming_events(datetime.now(timezone.utc)):
                queue.appendleft(data)

                while len(queue) >= max_events:
                    _send(
                        adh_clients, namespace_ids, queue, max_events, event_rate_counter
                    )
                    timer = time.time()

        if time.time() - timer > send_period:
            _send(adh_clients, namespace_ids, queue, max_events, event_rate_counter)
            timer = time.time()

        time.sleep(read_interval.total_seconds())

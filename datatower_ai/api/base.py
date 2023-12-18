from datatower_ai.src.bean.event import Event
from datatower_ai.src.consumer.abstract_consumer import _AbstractConsumer
from datatower_ai.src.process.event_processor import _EventProcessor


class _DTApi(object):
    def __init__(self, consumer: _AbstractConsumer, debug: bool):
        self.__consumer = consumer
        self.__ep = _EventProcessor(consumer, debug)

    def _add(self, dt_id, acid, send_type, event_name=None, properties_add=None, meta=None):
        self.__ep.add(dt_id, acid, send_type, event_name, properties_add, meta)

    def _add_batch(self, send_type: str, *events: Event):
        self.__ep.add_batch(send_type, *events)

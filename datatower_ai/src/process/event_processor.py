# -*- coding: utf-8 -*-
import copy
import datetime
import json
import random
import re
import time

from datatower_ai.src.bean.event import Event
from datatower_ai.src.consumer.abstract_consumer import _AbstractConsumer
from datatower_ai.src.util.exception import DTIllegalDataException, DTMetaDataException
from datatower_ai.src.util.logger import Logger
from datatower_ai.src.util.type_check import is_str, is_number, is_int


class _EventProcessor:
    def __init__(self, consumer, debug):
        self.__consumer = consumer
        self.__debug = debug

    def add(self, dt_id, acid, send_type, event_name=None, properties_add=None, meta=None):
        self.add_batch(
            send_type,
            Event(dt_id=dt_id, acid=acid, event_name=event_name, properties=properties_add, meta=meta)
        )

    def add_batch(self, send_type, *events):
        batch = [self.__build_data_from_event(send_type, event) for event in events]

        try:
            Logger.log('collect data=(len: {}){}'.format(len(batch), batch))
            self.__consumer.add(lambda: [
                json.dumps(data, separators=(',', ':'), cls=DTDateTimeSerializer, allow_nan=False) for data in batch
            ])
        except TypeError as e:
            raise DTIllegalDataException(e)
        except ValueError:
            raise DTIllegalDataException("Nan or Inf data are not allowed")

    def __build_data_from_event(self, send_type, event):
        if event.dt_id is None and event.acid is None:
            raise DTMetaDataException("dt_id and acid must be set at least one")
        if (event.dt_id is not None and not is_str(event.dt_id)) or (
                event.acid is not None and not is_str(event.acid)):
            raise DTMetaDataException("dt_id and acid must be string type")

        assert_properties(event.event_name, event.properties)

        data = {'#event_type': send_type}
        if event.properties:
            properties = copy.deepcopy(event.properties)
        else:
            properties = {}

        import datatower_ai.src.process.extra_verify as extra_verify
        extra_verify.move_meta(properties, data)
        extra_verify.move_meta(event.meta, data, delete=False)

        if '#event_time' not in data:
            self.__build_data(data, '#event_time', int(time.time() * 1000))
        if not is_int(data.get('#event_time')) or len(str(data.get('#event_time'))) != 13:
            raise DTMetaDataException("event_time must be timestamp (ms)")

        if '#event_syn' not in data:
            self.__build_data(data, '#event_syn', random_str(16))

        if event.dt_id is None:
            self.__build_data(data, '#dt_id', '0000000000000000000000000000000000000000')
        else:
            self.__build_data(data, '#dt_id', event.dt_id)

        if self.__debug:
            self.__build_data(data, '#debug', 'true')

        self.__build_data(data, '#app_id', self.__consumer.get_app_id())
        self.__build_data(data, '#event_name', event.event_name)
        self.__build_data(data, '#acid', event.acid)
        data['properties'] = properties

        extra_verify.extra_verify(data)
        return data

    @staticmethod
    def __build_data(data, key, value):
        if value is not None:
            data[key] = value


__NAME_PATTERN = re.compile(r"^[#$a-zA-Z][a-zA-Z0-9_]{0,63}$", re.I)
_STR_LD = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'


def assert_properties(event_name, properties):
    if not __NAME_PATTERN.match(event_name):
        raise DTIllegalDataException(
            "Event_name must be a valid variable name.")
    if properties is not None:
        for key, value in properties.items():
            if not is_str(key):
                raise DTIllegalDataException("Property key must be a str. [key=%s]" % str(key))

            if value is None:
                continue

            if not __NAME_PATTERN.match(key):
                raise DTIllegalDataException(
                    "Event_name=[%s] property key must be a valid variable name. [key=%s]" % (event_name, str(key)))

            if '#user_add' == event_name.lower() and not is_number(value):
                raise DTIllegalDataException('User_add properties must be number type')


def random_str(byte=32):
    return ''.join(random.choice(_STR_LD) for _ in range(byte))


class DTDateTimeSerializer(json.JSONEncoder):
    """
    实现 date 和 datetime 类型的自动转化
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            # head_fmt = "%Y-%m-%d %H:%M:%S"
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        elif isinstance(obj, datetime.date):
            fmt = '%Y-%m-%d'
            return obj.strftime(fmt)
        return json.JSONEncoder.default(self, obj)

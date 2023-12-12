# encoding:utf-8

from __future__ import unicode_literals
import copy
import datetime
import gzip
import json
import os
import re
import threading
import time
import random
from typing import Callable, List

from datatower_ai.src.service.http_service import _HttpService
from datatower_ai.src.util.exception import DTIllegalDataException, DTMetaDataException, DTException, DTNetworkException

from datatower_ai.src.util.logger import Logger
import logging

default_server_url = "https://s2s.roiquery.com/sync"

__NAME_PATTERN = re.compile(r"^[#$a-zA-Z][a-zA-Z0-9_]{0,63}$", re.I)
_STR_LD = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'

try:
    import queue
    from urllib.parse import urlparse
except ImportError:
    import Queue as queue
    from urlparse import urlparse
try:
    isinstance("", basestring)


    def is_str(s):
        return isinstance(s, basestring)
except NameError:
    def is_str(s):
        return isinstance(s, str)
try:
    isinstance(1, long)


    def is_int(n):
        return isinstance(n, int) or isinstance(n, long)
except NameError:
    def is_int(n):
        return isinstance(n, int)


def isNumber(s):
    if is_int(s):
        return True
    if isinstance(s, float):
        return True
    return False


def random_str(byte=32):
    return ''.join(random.choice(_STR_LD) for i in range(byte))


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

            if '#user_add' == event_name.lower() and not isNumber(value):
                raise DTIllegalDataException('User_add properties must be number type')


class DynamicSuperPropertiesTracker():
    def get_dynamic_super_properties(self):
        raise NotImplementedError


class DTDateTimeSerializer(json.JSONEncoder):
    """
        实现 date 和 datetime 类型的自动转化
        """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            head_fmt = "%Y-%m-%d %H:%M:%S"
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        elif isinstance(obj, datetime.date):
            fmt = '%Y-%m-%d'
            return obj.strftime(fmt)
        return json.JSONEncoder.default(self, obj)


class DTAnalytics(object):
    """
    DTAnalytics 上报数据关键实例
    """

    def __init__(self, consumer, debug=False, log_level=logging.INFO):
        """
        创建一个 DTAnalytics 实例
        DTAanlytics 需要与指定的 Consumer 一起使用，可以使用以下任何一种:
        - BatchConsumer: 批量实时地向DT服务器传输数据（同步阻塞），不需要搭配传输工具
        - AsyncBatchConsumer: 批量实时地向DT服务器传输数据（异步非阻塞），不需要搭配传输工具
        - DebugConsumer: 逐条发送数据，并对数据格式做严格校验,用于调试

        Args:
            consumer: 指定的 Consumer
        """

        self.__consumer = consumer
        self.__super_properties = {}
        self.__dynamic_super_properties_tracker = None
        self.__app_id = consumer.get_app_id()
        from datatower_ai.__version__ import __version__
        self.__preset_properties = {
            '#app_id': self.__app_id,
            '#sdk_type': 'dt_python_sdk',
            '#sdk_version_name': __version__,
        }
        self.debug = debug
        Logger.set(self.debug, log_level)

    def set_dynamic_super_properties_tracker(self, dynamic_super_properties_tracker):
        self.__dynamic_super_properties_tracker = dynamic_super_properties_tracker

    def user_set(self, dt_id=None, acid=None, properties=None, meta=None):
        """
        设置用户属性

        对于一般的用户属性，您可以调用 user_set 来进行设置。使用该接口上传的属性将会覆盖原有的属性值，如果之前不存在该用户属性，
        则会新建该用户属性，类型与传入属性的类型一致.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_set', send_type='user',
                   properties_add=properties, meta=meta)

    def user_unset(self, dt_id=None, acid=None, properties=None, meta=None):
        """
        删除某个用户的用户属性

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        if isinstance(properties, list):
            properties = dict((key, 0) for key in properties)
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_unset', send_type='user',
                   properties_add=properties, meta=meta)

    def user_set_once(self, dt_id=None, acid=None, properties=None, meta=None):
        """
        设置用户属性, 不覆盖已存在的用户属性

        如果您要上传的用户属性只要设置一次，则可以调用 user_set_once 来进行设置，当该属性之前已经有值的时候，将会忽略这条信息.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_set_once', send_type='user',
                   properties_add=properties, meta=meta)

    def user_add(self, dt_id=None, acid=None, properties=None, meta=None):
        """
        对指定的数值类型的用户属性进行累加操作

        当您要上传数值型的属性时，您可以调用 user_add 来对该属性进行累加操作. 如果该属性还未被设置，则会赋值0后再进行计算.
        可传入负值，等同于相减操作.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: Dict[str, int|float|double]
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_add', send_type='user',
                   properties_add=properties, meta=meta)

    def user_append(self, dt_id=None, acid=None, properties=None, meta=None):
        """
        对指定的**列表**类型的用户属性进行追加操作，列表内的元素都会转成字符串类型。

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties:  Dict[str, list]
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        for key, value in properties.items():
            if not isinstance(value, list):
                raise DTIllegalDataException('#user_append properties must be list type')
            properties[key] = [str(i) for i in value]

        self.__add(dt_id=dt_id, acid=acid, event_name='#user_append', send_type='user',
                   properties_add=properties, meta=meta)

    def user_uniq_append(self, dt_id=None, acid=None, properties=None, meta=None):
        """
        对指定的**列表**类型的用户属性进行追加操作，列表内的元素都会转成字符串类型，并对该属性的数组进行去重

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: Dict[str, list]
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        for key, value in properties.items():
            if not isinstance(value, list):
                raise DTIllegalDataException('#user_uniq_append properties must be list type')
            properties[key] = [str(i) for i in value]

        self.__add(dt_id=dt_id, acid=acid, event_name='#user_uniq_append', send_type='user',
                   properties_add=properties, meta=meta)

    def track(self, dt_id=None, acid=None, event_name=None, properties=None, meta=None):
        """
        发送事件数据

        您可以调用 track 来上传事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的名称只能以字母开头，可包含数字，字母和下划线“_”，
        长度最大为 50 个字符，对字母大小写不敏感. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            event_name: 事件名称
            properties: 事件属性
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性

        Raises:
            DTIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name, properties)
        self.__add(dt_id=dt_id, acid=acid, send_type='track', event_name=event_name, properties_add=all_properties,
                   meta=meta)

    def flush(self):
        """
        立即提交数据到相应的接收端
        """
        self.__consumer.flush()

    def close(self):
        """
        关闭并退出 sdk

        请在退出前调用本接口，以避免缓存内的数据丢失
        """
        self.__consumer.close()

    def _public_track_add(self, event_name, properties):
        if not is_str(event_name):
            raise DTMetaDataException('a string type event_name is required for track')

        all_properties = self.__preset_properties.copy()
        all_properties.update(self.__super_properties)
        if self.__dynamic_super_properties_tracker:
            all_properties.update(self.__dynamic_super_properties_tracker.get_dynamic_super_properties())
        if properties:
            all_properties.update(properties)
        return all_properties

    def __add(self, dt_id, acid, send_type, event_name=None, properties_add=None, meta=None):
        if dt_id is None and acid is None:
            raise DTMetaDataException("dt_id and acid must be set at least one")
        if (dt_id is not None and not is_str(dt_id)) or (acid is not None and not is_str(acid)):
            raise DTMetaDataException("dt_id and acid must be string type")

        assert_properties(event_name, properties_add)

        data = {'#event_type': send_type}
        if properties_add:
            properties = copy.deepcopy(properties_add)
        else:
            properties = {}

        import datatower_ai.src.extra_verify as extra_verify
        self.__movePresetProperties(extra_verify.meta, data, properties)
        self.__movePresetProperties(extra_verify.meta, data, copy.deepcopy(meta))

        if '#event_time' not in data:
            self.__buildData(data, '#event_time', int(time.time() * 1000))
        if not is_int(data.get('#event_time')) or len(str(data.get('#event_time'))) != 13:
            raise DTMetaDataException("event_time must be timestamp (ms)")

        if '#event_syn' not in data:
            self.__buildData(data, '#event_syn', random_str(16))

        if dt_id is None:
            self.__buildData(data, '#dt_id', '0000000000000000000000000000000000000000')
        else:
            self.__buildData(data, '#dt_id', dt_id)

        if self.debug:
            self.__buildData(data, '#debug', 'true')

        self.__buildData(data, '#app_id', self.__app_id)
        self.__buildData(data, '#event_name', event_name)
        self.__buildData(data, '#acid', acid)
        data['properties'] = properties

        extra_verify.extra_verify(data)

        try:
            Logger.log('collect data={}'.format(data))
            self.__consumer.add(lambda: [json.dumps(data, separators=(',', ':'), cls=DTDateTimeSerializer, allow_nan=False)])
        except TypeError as e:
            raise DTIllegalDataException(e)
        except ValueError:
            raise DTIllegalDataException("Nan or Inf data are not allowed")

    def __buildData(self, data, key, value):
        if value is not None:
            data[key] = value

    def __movePresetProperties(self, keys, data, properties):
        for key in keys:
            if key in properties.keys():
                data[key] = properties.get(key)
                del (properties[key])

    @staticmethod
    def enable_log(isPrint=False):
        Logger.is_print = isPrint

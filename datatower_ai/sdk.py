# encoding:utf-8

from __future__ import unicode_literals

from datatower_ai.api.base import _DTApi
from datatower_ai.api import DTAdReport
from datatower_ai.api import DTAnalyticsUtils
from datatower_ai.src.util.decoration import deprecated
from datatower_ai.src.util.exception import DTIllegalDataException, DTMetaDataException

from datatower_ai.src.util.logger import Logger
import logging

from datatower_ai.src.util.type_check import is_str


class DynamicSuperPropertiesTracker():
    def get_dynamic_super_properties(self):
        raise NotImplementedError


class DTAnalytics(_DTApi):
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
        super(DTAnalytics, self).__init__(consumer, debug)
        self.__consumer = consumer
        self.__super_properties = {}
        self.__dynamic_super_properties_tracker = None
        self.__app_id = consumer.get_app_id()
        from datatower_ai.__version__ import __version__
        self.__preset_properties = {
            str("#app_id"): self.__app_id,
            str("#sdk_type"): str("dt_python_sdk"),
            str("#sdk_version_name"): __version__,
        }
        Logger.set(debug, log_level)

        from datatower_ai.src.util._holder import _Holder
        _Holder.debug = debug

        self.__ad = DTAdReport(consumer, debug)
        self.__analytics_utils = DTAnalyticsUtils(consumer, debug)

    @property
    def ad(self):
        return self.__ad

    @property
    def utils(self):
        return self.__analytics_utils

    def set_dynamic_super_properties_tracker(self, dynamic_super_properties_tracker):
        self.__dynamic_super_properties_tracker = dynamic_super_properties_tracker

    def user_set(self, dt_id, acid=None, properties=None, meta=None):
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
        self._add(dt_id=dt_id, acid=acid, event_name='#user_set', send_type='user',
                  properties_add=properties, meta=meta)

    def user_unset(self, dt_id, acid=None, properties=None, meta=None):
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
        self._add(dt_id=dt_id, acid=acid, event_name='#user_unset', send_type='user',
                  properties_add=properties, meta=meta)

    def user_set_once(self, dt_id, acid=None, properties=None, meta=None):
        """
        设置用户属性, 不覆盖已存在的用户属性

        如果您要上传的用户属性只要设置一次，则可以调用 user_set_once 来进行设置，当该属性之前已经有值的时候，将会忽略这条信息.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
            meta: Dict[str, Any], properties 之外以带 #、$ 开头的属性
        """
        self._add(dt_id=dt_id, acid=acid, event_name='#user_set_once', send_type='user',
                  properties_add=properties, meta=meta)

    def user_add(self, dt_id, acid=None, properties=None, meta=None):
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
        self._add(dt_id=dt_id, acid=acid, event_name='#user_add', send_type='user',
                  properties_add=properties, meta=meta)

    def user_append(self, dt_id, acid=None, properties=None, meta=None):
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

        self._add(dt_id=dt_id, acid=acid, event_name='#user_append', send_type='user',
                  properties_add=properties, meta=meta)

    def user_uniq_append(self, dt_id, acid=None, properties=None, meta=None):
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

        self._add(dt_id=dt_id, acid=acid, event_name='#user_uniq_append', send_type='user',
                  properties_add=properties, meta=meta)

    def track(self, dt_id, acid=None, event_name=None, properties=None, meta=None):
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
        self._add(dt_id=dt_id, acid=acid, send_type='track', event_name=event_name, properties_add=all_properties,
                  meta=meta)

    @deprecated("This function is deprecated, please use track() instead.")
    def track_first(self, dt_id, acid=None, event_name='#app_install', properties=None):
        """
        发送安装事件数据

        您可以调用 track_first 来上传首次事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.
        首次事件是指针对某个设备或者其他维度的 ID，只会记录一次的事件. 例如在一些场景下，您可能希望记录在某个设备上第一次发生的事件，则可以用首次事件来上报数据.
        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            event_name: 事件名称
            properties: 事件属性

        Raises:
            DTIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name, properties)
        self._add(dt_id=dt_id, acid=acid, send_type='track', event_name=event_name,
                  properties_add=all_properties)

    def track_batch(self, *events):
        """ Track a batch of events

        :param events: A list of events to track.
        """
        for event in events:
            self._public_track_add(event.event_name, event.properties)
        self._add_batch("track", *events)

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

    def clear_super_properties(self):
        """
        删除所有已设置的事件公共属性
        """
        self.__super_properties = self.__preset_properties.copy()

    def set_super_properties(self, super_properties):
        """
        设置公共事件属性

        公共事件属性是所有事件中的属性属性，建议您在发送事件前，先设置公共事件属性. 当 track 的 properties 和
        super properties 有相同的 key 时，track 的 properties 会覆盖公共事件属性的值.

        Args:
            super_properties 公共属性
        """
        self.__super_properties.update(super_properties)

    @staticmethod
    def enable_log(isPrint=False):
        Logger.is_print = isPrint

    def register_pager(self, pager):
        """
        Register a pager to listen errors or warnings from SDK.

        :param pager: A function with signature: (code: int, message: str) -> None.
        """
        self.__consumer.register_pager(pager)

    def unregister_pager(self, pager):
        """
        Unregister this pager.
        """
        self.__consumer.unregister_pager(pager)

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

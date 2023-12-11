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
import requests
from requests import ConnectionError
import logging

logger = logging.getLogger(__name__)

default_server_url = "https://s2s.roiquery.com/sync"
__version__ = '2.0.0'
is_print = False

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


def log(msg=None, level=logging.INFO):
    if msg is not None and is_print:
        prefix = '[DataTower.ai-Python SDK V%s]' % __version__
        if level <= logging.INFO:
            logger.info("{}-{}".format(prefix, msg))
        elif level <= logging.WARNING:
            logger.warning("{}-{}".format(prefix, msg))
        else:
            logger.error("{}-{}".format(prefix, msg))


class DTException(Exception):
    pass


class DTIllegalDataException(DTException):
    """
    数据格式异常
    在发送的数据格式有误时，SDK 会抛出此异常，用户应当捕获并处理.
    """
    pass


class DTMetaDataException(DTException):
    """
    dt_id, acid, event_name, event_time 等元数据出错异常
    """


class DTNetworkException(DTException):
    """
    网络异常
    在因为网络或者不可预知的问题导致数据无法发送时，SDK会抛出此异常，用户应当捕获并处理.
    """
    pass


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

    def __init__(self, consumer, debug=False):
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
        self.__preset_properties = {
            '#app_id': self.__app_id,
            '#sdk_type': 'dt_python_sdk',
            '#sdk_version_name': __version__,
        }
        self.debug = debug
        self.clear_super_properties()

    def set_dynamic_super_properties_tracker(self, dynamic_super_properties_tracker):
        self.__dynamic_super_properties_tracker = dynamic_super_properties_tracker

    def user_set(self, dt_id=None, acid=None, properties=None):
        """
        设置用户属性

        对于一般的用户属性，您可以调用 user_set 来进行设置。使用该接口上传的属性将会覆盖原有的属性值，如果之前不存在该用户属性，
        则会新建该用户属性，类型与传入属性的类型一致.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
        """
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_set', send_type='user',
                   properties_add=properties)

    def user_unset(self, dt_id=None, acid=None, properties=None):
        """
        删除某个用户的用户属性

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
        """
        if isinstance(properties, list):
            properties = dict((key, 0) for key in properties)
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_unset', send_type='user',
                   properties_add=properties)

    def user_set_once(self, dt_id=None, acid=None, properties=None):
        """
        设置用户属性, 不覆盖已存在的用户属性

        如果您要上传的用户属性只要设置一次，则可以调用 user_set_once 来进行设置，当该属性之前已经有值的时候，将会忽略这条信息.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: dict 类型的用户属性
        """
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_set_once', send_type='user',
                   properties_add=properties)

    def user_add(self, dt_id=None, acid=None, properties=None):
        """
        对指定的数值类型的用户属性进行累加操作

        当您要上传数值型的属性时，您可以调用 user_add 来对该属性进行累加操作. 如果该属性还未被设置，则会赋值0后再进行计算.
        可传入负值，等同于相减操作.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: Dict[str, int|float|double]
        """
        self.__add(dt_id=dt_id, acid=acid, event_name='#user_add', send_type='user',
                   properties_add=properties)

    def user_append(self, dt_id=None, acid=None, properties=None):
        """
        对指定的**列表**类型的用户属性进行追加操作，列表内的元素都会转成字符串类型。

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties:  Dict[str, list]
        """
        for key, value in properties.items():
            if not isinstance(value, list):
                raise DTIllegalDataException('#user_append properties must be list type')
            properties[key] = [str(i) for i in value]

        self.__add(dt_id=dt_id, acid=acid, event_name='#user_append', send_type='user',
                   properties_add=properties)

    def user_uniq_append(self, dt_id=None, acid=None, properties=None):
        """
        对指定的**列表**类型的用户属性进行追加操作，列表内的元素都会转成字符串类型，并对该属性的数组进行去重

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            properties: Dict[str, list]
        """
        for key, value in properties.items():
            if not isinstance(value, list):
                raise DTIllegalDataException('#user_uniq_append properties must be list type')
            properties[key] = [str(i) for i in value]

        self.__add(dt_id=dt_id, acid=acid, event_name='#user_uniq_append', send_type='user',
                   properties_add=properties)

    def track(self, dt_id=None, acid=None, event_name=None, properties=None):
        """
        发送事件数据

        您可以调用 track 来上传事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的名称只能以字母开头，可包含数字，字母和下划线“_”，
        长度最大为 50 个字符，对字母大小写不敏感. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.

        Args:
            dt_id: 访客 ID
            acid: 账户 ID
            event_name: 事件名称
            properties: 事件属性

        Raises:
            DTIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name, properties)
        self.__add(dt_id=dt_id, acid=acid, send_type='track', event_name=event_name, properties_add=all_properties)

    def track_first(self, dt_id=None, acid=None, event_name='#app_install', properties=None):
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
        self.__add(dt_id=dt_id, acid=acid, send_type='track', event_name=event_name,
                   properties_add=all_properties)

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

    def __add(self, dt_id, acid, send_type, event_name=None, properties_add=None):
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

        self.__movePresetProperties(['#app_id', '#debug', '#event_time', '#event_syn'], data, properties)

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
        try:
            content = json.dumps(data, separators=(',', ':'), cls=DTDateTimeSerializer, allow_nan=False)
            log('collect data={}'.format(data))
            self.__consumer.add(content)
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
        global is_print
        is_print = isPrint


if os.name == 'nt':
    import msvcrt


    def _lock(file_):
        try:
            save_pos = file_.tell()
            file_.seek(0)
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_LOCK, 1)
            except IOError as e:
                raise DTException(e)
            finally:
                if save_pos:
                    file_.seek(save_pos)
        except IOError as e:
            raise DTException(e)


    def _unlock(file_):
        try:
            save_pos = file_.tell()
            if save_pos:
                file_.seek(0)
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_UNLCK, 1)
            except IOError as e:
                raise DTException(e)
            finally:
                if save_pos:
                    file_.seek(save_pos)
        except IOError as e:
            raise DTException(e)
elif os.name == 'posix':
    import fcntl


    def _lock(file_):
        try:
            fcntl.flock(file_.fileno(), fcntl.LOCK_EX)
        except IOError as e:
            raise DTException(e)


    def _unlock(file_):
        fcntl.flock(file_.fileno(), fcntl.LOCK_UN)
else:
    raise DTException("Python SDK is defined for NT and POSIX system.")


class _DTFileLock(object):
    def __init__(self, file_handler):
        self._file_handler = file_handler

    def __enter__(self):
        _lock(self._file_handler)
        return self

    def __exit__(self, t, v, tb):
        _unlock(self._file_handler)


class AbstractConsumer(object):
    """
        Consumer抽象类
    """
    def get_app_id(self):
        raise NotImplementedError

    def add(self, msg):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class BatchConsumer(AbstractConsumer):
    """
    同步、批量地向 DT 服务器传输数据

    通过HTTPS协议，同步地向 DT 服务器传输数据.
    但是存在网络不稳定等原因造成数据丢失的可能，因此不建议在生产环境中使用.

    触发上报的时机为以下条件满足其中之一的时候:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """
    _batchlock = threading.RLock()
    _cachelock = threading.RLock()

    def __init__(self, app_id, token, batch=20, server_url=default_server_url, timeout=30000, interval=3, compress=True,
                 max_cache_size=50):
        """
        创建 BatchConsumer

        Args:
            app_id: 项目的 APP ID
            token: 通信令牌
            batch: 指定触发上传的数据条数, 默认为 20 条, 最大 200 条
            timeout: 请求的超时时间, 单位毫秒, 默认为 30000 ms
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
        """
        self.__interval = interval
        self.__batch = min(batch, 200)
        self.__message_channel = []
        self.__max_cache_size = max_cache_size
        self.__cache_buffer = []
        self.__last_flush = time.time()
        self.__http_service = _HttpServices((urlparse(server_url)).geturl(), app_id, token, timeout)
        self.__http_service.compress = compress
        self.__app_id = app_id

    def get_app_id(self):
        return self.__app_id

    def add(self, msg):
        self._batchlock.acquire()
        try:
            self.__message_channel.append(msg)
        finally:
            self._batchlock.release()
        if len(self.__message_channel) >= self.__batch \
                or len(self.__cache_buffer) > 0:
            self.flush_once()

    def flush(self, throw_exception=True):
        while len(self.__cache_buffer) > 0 or len(self.__message_channel) > 0:
            try:
                self.flush_once(throw_exception)
            except DTIllegalDataException as e:
                log(e, level=logging.WARNING)
                continue

    def flush_once(self, throw_exception=True):
        if len(self.__message_channel) == 0 and len(self.__cache_buffer) == 0:
            return

        self._cachelock.acquire()
        self._batchlock.acquire()
        try:
            try:
                if len(self.__message_channel) == 0 and len(self.__cache_buffer) == 0:
                    return
                if len(self.__cache_buffer) == 0 or len(self.__message_channel) >= self.__batch:
                    self.__cache_buffer.append(self.__message_channel)
                    self.__message_channel = []
            finally:
                self._batchlock.release()
            msg = self.__cache_buffer[0]
            self.__http_service.send('[' + ','.join(msg) + ']', str(len(msg)))
            self.__last_flush = time.time()
            self.__cache_buffer = self.__cache_buffer[1:]
        except DTNetworkException as e:
            if throw_exception:
                raise e
        except DTIllegalDataException as e:
            self.__cache_buffer = self.__cache_buffer[1:]
            if throw_exception:
                raise e
        finally:
            if len(self.__cache_buffer) > self.__max_cache_size:
                self.__cache_buffer = self.__cache_buffer[1:]
            self._cachelock.release()

    def close(self):
        self.flush()


class DebugConsumer(AbstractConsumer):
    def __init__(self, app_id, token, server_url=default_server_url, timeout=30000):

        self.__message_channel = []
        self.__cache_buffer = []
        self.__last_flush = time.time()
        self.__http_service = _HttpServices((urlparse(server_url)).geturl(), app_id, token, timeout, compress=False)
        self.__app_id = app_id
        DTAnalytics.enable_log(True)

    def get_app_id(self):
        return self.__app_id

    def add(self, msg):
        self.__http_service.send('[' + msg + ']', str(len(msg)))

    def flush(self):
        pass

    def close(self):
        pass


class AsyncBatchConsumer(AbstractConsumer):
    """
    异步、批量地向 DT 服务器发送数据

    AsyncBatchConsumer 使用独立的线程进行数据发送，当满足以下两个条件之一时触发数据上报:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """

    def __init__(self, app_id, token, server_url=default_server_url, interval=3, flush_size=20, queue_size=100000):
        """
        创建 AsyncBatchConsumer

        Args:
            appid: 项目的 APP ID
            token: 通信令牌
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
            flush_size: 队列缓存的阈值，超过此值将立即进行发送
            queue_size: 缓存队列的大小
        """
        self.__http_service = _HttpServices(urlparse(server_url).geturl(), app_id, token, 30000)
        self.__batch = flush_size
        self.__queue = queue.Queue(queue_size)

        # 初始化发送线程
        self.__flushing_thread = self._AsyncFlushThread(self, interval)
        self.__flushing_thread.daemon = True
        self.__flushing_thread.start()
        self.__app_id = app_id

    def get_app_id(self):
        return self.__app_id

    def add(self, msg):
        try:
            self.__queue.put_nowait(msg)
        except queue.Full as e:
            raise DTNetworkException(e)

        if self.__queue.qsize() > self.__batch:
            self.flush()

    def flush(self):
        self.__flushing_thread.flush()

    def close(self):
        self.flush()
        self.__flushing_thread.stop()
        while not self.__queue.empty():
            log("当前未发送数据数: {}".format(self.__queue.qsize()))
            self._perform_request()

    def _need_drain(self):
        return self.__queue.qsize() > self.__batch

    def _perform_request(self):
        """
        同步的发送数据

        仅用于内部调用, 用户不应当调用此方法.
        """
        flush_buffer = []
        while len(flush_buffer) < self.__batch:
            try:
                flush_buffer.append(str(self.__queue.get_nowait()))
            except queue.Empty:
                break

        if len(flush_buffer) > 0:
            for i in range(3):  # 网络异常情况下重试 3 次
                try:
                    self.__http_service.send('[' + ','.join(flush_buffer) + ']', str(len(flush_buffer)))
                    return True
                except DTNetworkException as e:
                    log("{}: {}".format(e, flush_buffer), level=logging.WARNING)
                    continue
                except DTIllegalDataException as e:
                    log("{}: {}".format(e, flush_buffer), level=logging.WARNING)
                    break
            log("{}: {}".format("Data translate failed 3 times", flush_buffer), level=logging.ERROR)


    class _AsyncFlushThread(threading.Thread):
        def __init__(self, consumer, interval):
            threading.Thread.__init__(self)
            self._consumer = consumer
            self._interval = interval

            self._stop_event = threading.Event()
            self._finished_event = threading.Event()
            self._flush_event = threading.Event()

        def flush(self):
            self._flush_event.set()

        def stop(self):
            """
            停止线程
            退出时需调用此方法，以保证线程安全结束.
            """
            self._stop_event.set()
            self._finished_event.wait()

        def run(self):
            while True:
                if self._consumer._need_drain():
                    # 当当前queue size 大于batch size时，马上发送数据
                    self._flush_event.set()
                # 如果 _flush_event 标志位为 True，或者等待超过 _interval 则继续执行
                self._flush_event.wait(self._interval)
                self._consumer._perform_request()
                self._flush_event.clear()

                # 发现 stop 标志位时安全退出
                if self._stop_event.isSet():
                    break
            self._finished_event.set()


def _gzip_string(data):
    try:
        return gzip.compress(data)
    except AttributeError:
        import StringIO
        buf = StringIO.StringIO()
        fd = gzip.GzipFile(fileobj=buf, mode="w")
        fd.write(data)
        fd.close()
        return buf.getvalue()


class _HttpServices(object):
    """
    内部类，用于发送网络请求

    指定接收端地址和项目 APP ID, 实现向接收端上传数据的接口. 发送前将数据默认使用 Gzip 压缩,
    """

    def __init__(self, server_uri, app_id, token, timeout=30000, compress=True):
        self.url = server_uri
        self.app_id = app_id
        self.token = token
        self.timeout = timeout
        self.compress = compress

    def send(self, data, length):
        """使用 Requests 发送数据给服务器

        Args:
            data: 待发送的数据
            length

        Raises:
            DTIllegalDataException: 数据错误
            DTNetworkException: 网络错误
        """
        headers = {'app_id': self.app_id, 'DT-type': 'python-sdk', 'sdk-version': __version__,
                   'data-count': length, 'token': self.token}
        try:
            compress_type = 'gzip'
            if self.compress:
                data = _gzip_string(data.encode("utf-8"))
            else:
                compress_type = 'none'
                data = data.encode("utf-8")
            headers['compress'] = compress_type
            response = requests.post(self.url, data=data, headers=headers, timeout=self.timeout)
            if response.status_code == 200:
                response_data = json.loads(response.text)
                log('response={}'.format(response_data))
                if response_data["code"] == 0:
                    return True
                else:
                    raise DTIllegalDataException("Unexpected result code: " + str(response_data["code"]) \
                                                 + " reason: " + response_data["msg"])
            else:
                log('response={}'.format(response.status_code))
                raise DTNetworkException("Unexpected Http status code " + str(response.status_code))
        except ConnectionError as e:
            time.sleep(0.5)
            raise DTNetworkException("Data transmission failed due to " + repr(e))

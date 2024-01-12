# -*- coding: utf-8 -*-
import gzip
import json
import time

from urllib3.exceptions import MaxRetryError, ConnectionError

from datatower_ai.src.bean.pager_code import PAGER_CODE_SUB_NETWORK_MAX_RETRIES, PAGER_CODE_SUB_NETWORK_CONNECTION, \
    PAGER_CODE_SUB_NETWORK_OTHER
from datatower_ai.src.util.data_struct.mini_lru import MiniLru
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor, count_avg
from datatower_ai.src.util.type_check import is_str

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import requests
from datatower_ai.src.util.exception import DTIllegalDataException, DTNetworkException, DTException

from datatower_ai.src.util.logger import Logger
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class _RequestOversizeException(DTException):
    def __init__(self, factor):
        self.__factor = factor

    @property
    def factor(self):
        return self.__factor


class _DataSeparator:
    pass


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


class _HttpService(object):
    """
    内部类，用于发送网络请求

    指定接收端地址和项目 APP ID, 实现向接收端上传数据的接口. 发送前将数据默认使用 Gzip 压缩,
    """
    _simulate = None
    DEFAULT_SERVER_URL = "https://s2s.roiquery.com/sync"

    __session_cache = MiniLru(5, on_drop=lambda _, s: s.close())

    def __init__(self, timeout=3000, retries=3, compress=True):
        self.timeout = timeout
        self.compress = compress
        self.retries = retries

    def post_event(self, server_url, app_id, token, data, length):
        """使用 Requests 发送数据给服务器

        Args:
            data: 待发送的数据
            length

        Raises:
            DTIllegalDataException: 数据错误
            DTNetworkException: 网络错误
        """
        from datatower_ai.__version__ import __version__
        headers = {'app_id': app_id, 'DT-type': 'python-sdk', 'sdk-version': __version__,
                   'data-count': length, 'token': token}

        return self.__post(url=server_url, data=data, headers=headers)

    def post_raw(self, url, data, headers=None):
        try:
            return self.__post(url=url, data=data, headers=headers)
        except:
            Logger.exception("[HttpService] post_raw")
            return False

    def __post(self, url, data, headers=None):
        if headers is None:
            headers = {}

        if is_str(data) or type(data) is bytes:
            compress_type = 'gzip'
            if self.compress:
                encoded = data if type(data) is bytes else data.encode("utf-8")
                data = _gzip_string(encoded)

                count_avg("http_avg_compress_rate", len(encoded) / len(data), 1000, 5)
                Logger.debug(
                    "[HttpService] avg compress rate: {:.4f}".format(_CounterMonitor["http_avg_compress_rate"]))
            else:
                compress_type = 'none'
                data = data if type(data) is bytes else data.encode("utf-8")
            headers['compress'] = compress_type

        if len(data) > _HttpService.__MAX_SIZE:
            Logger.warning("[HttpService] Data is oversize, will try to split and resend, {} > {}".format(
                len(data), _HttpService.__MAX_SIZE)
            )
            raise _RequestOversizeException(len(data) / _HttpService.__MAX_SIZE)

        if Logger.is_print and _HttpService._simulate is not None:
            success = _HttpService._simulate >= 0
            Logger.info(
                "[HttpService] Simulating the HttpService result -> {}, {}".format(success, _HttpService._simulate))
            time.sleep(max(0, _HttpService._simulate / 1000))
            return success

        try:
            url = urlparse(url).geturl()
            session = _HttpService.__session_cache.get_or_put(url, put_func=lambda: self.__create_session())

            response = session.post(url, data=data, headers=headers, timeout=self.timeout)

            if response.status_code == 200:
                response_data = json.loads(response.text)
                if response_data["code"] == 0:
                    return True
                else:
                    raise DTIllegalDataException("Unexpected result code: " + str(response_data["code"])
                                                 + " reason: " + response_data["msg"])
            else:
                Logger.log('response={}'.format(response.status_code))
                raise DTNetworkException(response.status_code, "Unexpected Http status code " + str(
                    response.status_code) + ", reason: " + response.reason)
        except MaxRetryError as e:
            raise DTNetworkException(PAGER_CODE_SUB_NETWORK_MAX_RETRIES, "Reached max retry limit!")
        except ConnectionError as e:
            raise DTNetworkException(PAGER_CODE_SUB_NETWORK_CONNECTION, "Data transmission failed due to " + repr(e))
        except DTNetworkException as e:
            raise e
        except Exception as e:
            raise DTNetworkException(PAGER_CODE_SUB_NETWORK_OTHER, "Http failed due to " + repr(e))

    __MB = 1024 * 1024
    __MAX_SIZE = 1 * __MB

    @staticmethod
    def approx_split_data_by_mb(data, target=1, to_str=lambda x: x):
        """Divide the data to approximate size of {target} mb (after compress, with dynamic compress rate)"""
        avg_compress_rate = _CounterMonitor["http_avg_compress_rate"].value
        Logger.debug("Current average compress rate: {}".format(avg_compress_rate))
        if len(data) == 0:
            return []

        from datatower_ai.src.util.performance.time_monitor import TimeMonitor
        timer_split = TimeMonitor().start("ns_split_data")

        if avg_compress_rate <= 0:
            avg_compress_rate = 15  # default value

        target_mb = target * _HttpService.__MB

        result = []
        group = []
        size = 0
        for item in data:
            if type(item) is _DataSeparator:
                print("---> Met the _DataSeparator, size: {}".format(size))
                if len(group) > 0:
                    result.append(group)
                group = []
                size = 0
                continue

            if type(item) is bytes:
                encoded = item
            else:
                stringed = to_str(item)
                if type(stringed) is not str:
                    timer_split.stop(one_shot=True)
                    return [data]
                encoded = stringed.encode("utf-8")
            item_size = len(encoded) / avg_compress_rate
            if size + item_size >= target_mb:
                print("---> Met the target mb, size: {}, {}".format(size, size + item_size))
                if len(group) == 0:
                    result.append([encoded])
                    size = 0
                else:
                    result.append(group)
                    group = [encoded]
                    size = item_size
            else:
                group.append(encoded)
                size += item_size

        if len(group) > 0:
            result.append(group)

        new_avg_len = count_avg("http_avg_compress_len", len(data) / len(result), 1000000, 1000)
        duration = timer_split.stop(one_shot=False)

        Logger.debug("Current average upload count per request: {}, duration: {}".format(new_avg_len, duration))

        return result

    def __create_session(self):
        session = requests.Session()
        retry = Retry(total=self.retries, backoff_factor=0.3)
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

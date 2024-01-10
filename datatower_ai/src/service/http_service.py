# -*- coding: utf-8 -*-
import gzip
import json

from urllib3.exceptions import MaxRetryError, ConnectionError

from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor
from datatower_ai.src.util.type_check import is_str

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import requests
from datatower_ai.src.util.exception import DTIllegalDataException, DTNetworkException

from datatower_ai.src.util.logger import Logger
from requests.adapters import HTTPAdapter
from urllib3 import Retry


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
        if is_str(data):
            compress_type = 'gzip'
            if self.compress:
                encoded = data.encode("utf-8")
                data = _gzip_string(encoded)

                cnt = _CounterMonitor["http_compress_rate_cnt"]
                pre_sum = _CounterMonitor["http_avg_compress_rate"] * cnt
                _CounterMonitor["http_avg_compress_rate"] = (pre_sum + len(encoded) / len(data)) / (cnt + 1)
                # ~Long Short Term Memory, 5/1000
                new_cnt = (cnt + 1) % 1000
                _CounterMonitor["http_compress_rate_cnt"] = new_cnt if new_cnt != 0 else 5
                Logger.debug("[HttpService] avg compress rate: {:.4f}".format(_CounterMonitor["http_avg_compress_rate"]))
            else:
                compress_type = 'none'
                data = data.encode("utf-8")
            headers['compress'] = compress_type

        if Logger.is_print and _HttpService._simulate is not None:
            success = _HttpService._simulate > 0
            Logger.info("[HttpService] Simulating the HttpService result -> {}, {}".format(success, _HttpService._simulate))
            return success

        try:
            with requests.Session() as s:
                retry = Retry(total=self.retries, backoff_factor=0.3)
                s.mount("https://", HTTPAdapter(max_retries=retry))
                response = s.post(urlparse(url).geturl(), data=data, headers=headers, timeout=self.timeout)

                if response.status_code == 200:
                    response_data = json.loads(response.text)
                    if response_data["code"] == 0:
                        return True
                    else:
                        raise DTIllegalDataException("Unexpected result code: " + str(response_data["code"])
                                                     + " reason: " + response_data["msg"])
                else:
                    Logger.log('response={}'.format(response.status_code))
                    raise DTNetworkException("Unexpected Http status code " + str(response.status_code))
        except MaxRetryError as e:
            raise DTNetworkException("Reached max retry limit!")
        except ConnectionError as e:
            raise DTNetworkException("Data transmission failed due to " + repr(e))
        except Exception as e:
            raise DTNetworkException("Http failed due to " + repr(e))

    __MB = 1024 * 1024

    @staticmethod
    def approx_split_data_by_mb(data, to_str=lambda x: x):
        """Divide the data to approximate size of 1mb (after compress, with dynamic compress rate)"""
        avg_compress_rate = _CounterMonitor["http_avg_compress_rate"].value
        Logger.debug("Current average compress rate: {}".format(avg_compress_rate))
        if len(data) == 0:
            return []
        if avg_compress_rate <= 0:
            return [data]

        result = []
        group = []
        size = 0
        for item in data:
            stringed = to_str(item)
            if stringed is not str:
                return [data]
            item_size = len(stringed.encode("utf-8")) / avg_compress_rate
            if size + item_size >= _HttpService.__MB:
                if len(group) == 0:
                    result.append([item])
                    size = 0
                else:
                    result.append(group)
                    group = [item]
                    size = item_size
            else:
                group.append(item)
                size += item_size

        if len(group) > 0:
            result.append(group)

        avg_len = len(data) / len(result)
        old_avg_len = _CounterMonitor["http_avg_compress_len"]
        old_avg_len_cnt = _CounterMonitor["http_avg_compress_len_cnt"]
        new_avg_len = (old_avg_len * old_avg_len_cnt + avg_len) / (old_avg_len_cnt + 1)
        _CounterMonitor["http_avg_compress_len"] = new_avg_len
        new_cnt = (old_avg_len_cnt + 1) % 1000000
        _CounterMonitor["http_avg_compress_len_cnt"] = new_cnt if new_cnt != 0 else 1000

        Logger.debug("Current average upload count per request: {}".format(new_avg_len))

        return result

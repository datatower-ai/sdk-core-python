# -*- coding: utf-8 -*-
import gzip
import json
import time

from requests.exceptions import InvalidSchema
from urllib3.exceptions import MaxRetryError, ConnectionError

from datatower_ai.src.bean.pager_code import PAGER_CODE_SUB_NETWORK_MAX_RETRIES, PAGER_CODE_SUB_NETWORK_CONNECTION, \
    PAGER_CODE_SUB_NETWORK_OTHER
from datatower_ai.src.util.data_struct.mini_lru import MiniLru
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor, count_avg
from datatower_ai.src.util.performance.time_monitor import TimeMonitor
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
    def __init__(self, factor, compress_rate, compressed_size):
        self.__factor = factor
        self.__compression_rate = compress_rate
        self.__compressed_size = compressed_size

    @property
    def factor(self):
        return self.__factor

    @property
    def compression_rate(self):
        return self.__compression_rate

    @property
    def compressed_size(self):
        return self.__compressed_size


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
    Internal class that handles HTTP requests
    """

    __MB = 1024 * 1024
    __MAX_SIZE = 8 * __MB

    DEFAULT_SERVER_URL = "https://s2s.roiquery.com/sync"

    _simulate = None
    __session = None

    def __init__(self, timeout=3000, retries=3, compress=True):
        self.timeout = timeout
        self.compress = compress
        self.retries = retries
        if _HttpService.__session is None:
            _HttpService.__session = self.__create_session()

    def post_event(self, server_url, app_id, token, data, length):
        """Post data to BE

        :param server_url: URL of the server
        :param app_id: App id
        :param token: Token
        :param data: Data to send
        :param length: Length of data
        :raise DTNetworkException: Issues related with the network.
        :raise DTIllegalDataException: Network is ok, but the data sent is not valid that BE responses different status
        code.
        """
        from datatower_ai.__version__ import __version__
        headers = {'app_id': app_id, 'DT-type': 'python-sdk', 'sdk-version': __version__,
                   'data-count': length, 'token': token}

        return self.__post(url=server_url, data=data, headers=headers)

    def post_raw(self, url, data, headers=None, catch_exceptions=True):
        if catch_exceptions:
            try:
                return self.__post(url=url, data=data, headers=headers)
            except:
                Logger.exception("[HttpService] post_raw")
                return False
        else:
            return self.__post(url=url, data=data, headers=headers)

    def __post(self, url, data, headers=None):
        if headers is None:
            headers = {}

        compress_rate = 1
        if is_str(data) or type(data) is bytes:
            compress_type = 'gzip'
            if self.compress:
                timer = TimeMonitor().start("http_avg_compress_duration")
                encoded = data if type(data) is bytes else data.encode("utf-8")
                data = _gzip_string(encoded)

                compress_rate = len(encoded) / len(data)
                count_avg("http_avg_compress_rate", compress_rate, 1000, 50)
                count_avg("http_avg_compressed_size", len(data), 1000, 50)
                Logger.debug(
                    "[HttpService] avg compress rate: {:.4f}".format(_CounterMonitor["http_avg_compress_rate"]))
                timer.stop()
            else:
                compress_type = 'none'
                data = data if type(data) is bytes else data.encode("utf-8")
            headers['compress'] = compress_type

        if len(data) > _HttpService.__MAX_SIZE:
            old_avg = _CounterMonitor["http_avg_compress_rate"].value
            delta = len(data) / _HttpService.__MAX_SIZE             # guarantees to > 1
            _CounterMonitor["http_avg_compress_rate"] /= delta      # penalty on avg compress rate
            Logger.warning(
                "[HttpService] Data is oversize ({:.0f}b > {:.0f}b), will try to split and resend. "
                "Put ACR penalty into effect: {:.2f} -> {:.2f}".format(
                    len(data), _HttpService.__MAX_SIZE, old_avg, _CounterMonitor["http_avg_compress_rate"].value
                )
            )
            raise _RequestOversizeException(delta, compress_rate, len(data))

        from datatower_ai.src.util._holder import _Holder
        if _Holder.debug and _HttpService._simulate is not None:
            # Simulating the network request. Only works on Debug mode.
            timer = TimeMonitor().start("http_post")
            success = _HttpService._simulate >= 0
            Logger.info(
                "[HttpService] Simulating the HttpService result -> {}, {}".format(success, _HttpService._simulate))
            time.sleep(max(0, _HttpService._simulate / 1000))
            timer.stop()
            return success

        try:
            timer = TimeMonitor().start("http_post")
            url = urlparse(url).geturl()
            session = self.__session

            response = session.post(url, data=data, headers=headers, timeout=self.timeout)
            timer.stop()

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

    def __create_session(self):
        session = requests.Session()
        retry = Retry(total=self.retries, backoff_factor=0.3)
        session.mount("https://", HTTPAdapter(max_retries=retry))       # default 4 https
        session.mount("http://", HTTPAdapter(max_retries=retry))        # default 4 http
        return session


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    n = 100

    def log(msg):
        logging.disable(0)
        logging.info(msg)
        logging.disable(1000)

    # raw
    # log("Starts to run method 1...")
    # beg = time.time()
    # for i in range(n):
    #     _ = requests.get("https://kotlinlang.org/")
    #     _ = requests.get("https://www.python.org/")
    # time_1 = time.time() - beg
    # log("method 1 time taken: {:.2f}s, avg: {:.2f}s".format(time_1, time_1/n/2))

    # session
    log("Starts to run method 2...")
    ts_beg = time.time()
    ts_session = requests.Session()
    for _ in range(n):
        _ = ts_session.get("https://kotlinlang.org/")
        _ = ts_session.get("https://www.python.org/")
    time_2 = time.time() - ts_beg
    log("method 2 time taken: {:.2f}s, avg: {:.2f}s".format(time_2, time_2/n/2))

    # session, separate session
    # log("Starts to run method 3...")
    # beg = time.time()
    # session = requests.Session()
    # session2 = requests.Session()
    # for _ in range(n):
    #     _ = session.get("https://kotlinlang.org/")
    #     _ = session2.get("https://www.python.org/")
    # time_3 = time.time() - beg
    # log("method 3 time taken: {:.2f}s, avg: {:.2f}s".format(time_3, time_3/n/2))

    # session, separate adapter
    log("Starts to run method 4...")
    ts_beg = time.time()
    ts_session = requests.Session()
    ts_session.mount("https://kotlinlang.org", HTTPAdapter())
    ts_session.mount("https://www.python.org", HTTPAdapter())
    for _ in range(n):
        _ = ts_session.get("https://kotlinlang.org/")
        _ = ts_session.get("https://www.python.org/")
    time_4 = time.time() - ts_beg
    log("method 4 time taken: {:.2f}s, avg: {:.2f}s".format(time_4, time_4/n/2))

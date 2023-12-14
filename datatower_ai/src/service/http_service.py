import gzip
import json
import logging
import time
from typing import Dict, Optional, Union
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

    def __init__(self, timeout=3000, retries: int = 3, compress=True):
        self.timeout = timeout
        self.compress = compress
        self.retries = retries

    def post_event(self, server_url: str, app_id: str, token: str, data, length) -> bool:
        """使用 Requests 发送数据给服务器

        Args:
            data: 待发送的数据
            length

        Raises:
            DTIllegalDataException: 数据错误
            DTNetworkException: 网络错误
        """
        if Logger.is_print and _HttpService._simulate is not None:
            Logger.debug("[HttpService] Simulating the HttpService result -> {}, delay: {}ms".format(
                _HttpService._simulate > 0, _HttpService._simulate
            ))
            time.sleep(_HttpService._simulate / 1000)
            return _HttpService._simulate

        from datatower_ai.__version__ import __version__
        headers = {'app_id': app_id, 'DT-type': 'python-sdk', 'sdk-version': __version__,
                   'data-count': length, 'token': token}
        try:
            compress_type = 'gzip'
            if self.compress:
                data = _gzip_string(data.encode("utf-8"))
            else:
                compress_type = 'none'
                data = data.encode("utf-8")
            headers['compress'] = compress_type

            with requests.Session() as s:
                retry = Retry(total=self.retries, backoff_factor=0.3)
                s.mount("https://", HTTPAdapter(max_retries=retry))
                response = s.post(urlparse(server_url).geturl(), data=data, headers=headers, timeout=self.timeout)

                if response.status_code == 200:
                    response_data = json.loads(response.text)
                    Logger.log('response={}'.format(response_data), logging.DEBUG)
                    if response_data["code"] == 0:
                        return True
                    else:
                        raise DTIllegalDataException("Unexpected result code: " + str(response_data["code"]) \
                                                     + " reason: " + response_data["msg"])
                else:
                    Logger.log('response={}'.format(response.status_code))
                    raise DTNetworkException("Unexpected Http status code " + str(response.status_code))
        except ConnectionError as e:
            raise DTNetworkException("Data transmission failed due to " + repr(e))
        except Exception as e:
            raise DTNetworkException("Http failed due to " + repr(e))

    def post_raw(self, url: str, data: Union[str, Dict], headers: Optional[Dict] = None) -> bool:
        if Logger.is_print and _HttpService._simulate is not None:
            Logger.info("[HttpService] Simulating the HttpService result -> {}".format(_HttpService._simulate))
            return _HttpService._simulate

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
                        Logger.error(
                            "[HttpService] post_raw: Unexpected result code: {}, reason: {}".format(
                                response_data["code"], response_data["msg"]
                            )
                        )
                else:
                    Logger.error(
                        "[HttpService] post_raw: Unexpected Http status code {}, {}".format(response.status_code, response)
                    )
            return False
        except ConnectionError as _:
            Logger.exception("[HttpService] post_raw: Data transmission failed")
            return False

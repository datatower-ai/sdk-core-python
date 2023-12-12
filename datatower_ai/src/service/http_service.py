import gzip
import json
import time
from urllib.parse import urlparse

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

    def __init__(self, timeout=30000, retries: int = 3, compress=True):
        self.timeout = timeout
        self.compress = compress
        self.retries = retries

    def send(self, server_url: str, app_id: str, token: str, data, length):
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
                    Logger.log('response={}'.format(response_data))
                    if response_data["code"] == 0:
                        return True
                    else:
                        raise DTIllegalDataException("Unexpected result code: " + str(response_data["code"]) \
                                                     + " reason: " + response_data["msg"])
                else:
                    Logger.log('response={}'.format(response.status_code))
                    raise DTNetworkException("Unexpected Http status code " + str(response.status_code))
        except ConnectionError as e:
            time.sleep(0.5)
            raise DTNetworkException("Data transmission failed due to " + repr(e))
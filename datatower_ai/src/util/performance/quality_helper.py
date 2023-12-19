import json
from enum import Enum

from datatower_ai.src.util.logger import Logger

from datatower_ai.src.service.http_service import _HttpService
from datatower_ai.src.util.thread.thread import WorkerManager

from datatower_ai.src.util.singleton import Singleton


class _DTQualityLevel(Enum):
    ERROR = 1
    WARNING = 2
    MESSAGE = 3


class _DTQualityHelper(Singleton):
    from datatower_ai.__version__ import __version__
    import platform
    import os
    __common_data = {
        "sdk_type": "dt_python_sdk",
        "sdk_version_name": __version__,
        "os_version_name": "{}-{}".format(platform.system(), platform.release()),
        "device_model": os.name,
    }

    def __init__(self):
        self.__wm = None

    def report_quality_message(self, app_id, code, msg,
                               level=_DTQualityLevel.ERROR,
                               worker_manager=None):
        wm = worker_manager     # using if worker_manager provided.
        if wm is None:
            if self.__wm is None:
                # Instantiate only if needed. Doesn't need to keep alive when idle.
                self.__wm = WorkerManager("dt-quality_helper", keep_alive_ms=100)
            wm = self.__wm
        wm.execute(lambda: self.__report_qlt_msg_task(app_id, code, msg, level))

    @staticmethod
    def __report_qlt_msg_task(app_id, code, msg, level):
        url = "https://debug.roiquery.com/debug"
        body = _DTQualityHelper.__build_data(app_id, code, msg, level)
        if _HttpService(timeout=3000).post_raw(url, body):
            Logger.debug("[Quality] Successfully reported! (code: {}, msg: {}, level: {})".format(
                code, msg, level.value
            ))
        else:
            Logger.error("[Quality] Failed to report! (code: {}, msg: {}, level: {})".format(
                code, msg, level.value
            ))

    @staticmethod
    def __build_data(app_id, code, msg, level):
        return json.dumps({
            "app_id": app_id,
            "error_code": code,
            "error_level": level.value,
            "error_message": msg,
        }.update(_DTQualityHelper.__common_data), separators=(',', ':'))

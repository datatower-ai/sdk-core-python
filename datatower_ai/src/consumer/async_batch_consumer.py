import logging
import queue
import threading
from urllib.parse import urlparse

from datatower_ai.src.util.logger import Logger

from datatower_ai.src.util.exception import DTNetworkException, DTIllegalDataException

from datatower_ai import default_server_url
from datatower_ai.src.consumer.abstract_consumer import AbstractConsumer
from datatower_ai.src.service.http_service import _HttpService


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
        self.__token = token
        self.__server_url = server_url

        self.__http_service = _HttpService(30000)
        self.__batch = flush_size
        self.__queue = queue.Queue(queue_size)

        # 初始化发送线程
        self.__flushing_thread = self._AsyncFlushThread(self, interval)
        self.__flushing_thread.daemon = True
        self.__flushing_thread.start()
        self.__app_id = app_id

    def get_app_id(self):
        return self.__app_id

    def add(self, get_msg):
        try:
            self.__queue.put_nowait(get_msg()[0])
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
            Logger.log("当前未发送数据数: {}".format(self.__queue.qsize()))
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
                    self.__http_service.send(urlparse(self.__server_url).geturl(), self.__app_id, self.__token,
                                             '[' + ','.join(flush_buffer) + ']', str(len(flush_buffer)))
                    return True
                except DTNetworkException as e:
                    Logger.log("{}: {}".format(e, flush_buffer), level=logging.WARNING)
                    continue
                except DTIllegalDataException as e:
                    Logger.log("{}: {}".format(e, flush_buffer), level=logging.WARNING)
                    break
            Logger.log("{}: {}".format("Data translate failed 3 times", flush_buffer), level=logging.ERROR)


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
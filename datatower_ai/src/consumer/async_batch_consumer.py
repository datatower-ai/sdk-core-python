import logging
from collections import deque

from datatower_ai.src.util.performance.time_monitor import TimeMonitor

try:
    import queue
except ImportError:
    import Queue as queue
import threading
from typing import List, Deque

from datatower_ai.src.util.logger import Logger

from datatower_ai.src.util.exception import DTNetworkException, DTIllegalDataException

from datatower_ai import default_server_url
from datatower_ai.src.consumer.abstract_consumer import AbstractConsumer
from datatower_ai.src.service.http_service import _HttpService
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor


class AsyncBatchConsumer(AbstractConsumer):
    """
    异步、批量地向 DT 服务器发送数据

    AsyncBatchConsumer 使用独立的线程进行数据发送，当满足以下两个条件之一时触发数据上报:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """

    def __init__(self, app_id, token, server_url=default_server_url, interval=3, flush_size=20, queue_size=100000,
                 close_retry=1):
        """
        创建 AsyncBatchConsumer

        Args:
            appid: 项目的 APP ID
            token: 通信令牌
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
            flush_size: 队列缓存的阈值，超过此值将立即进行发送
            queue_size: 缓存队列的大小
            close_retry: close() 调用时会尝试进行数据的最后上传，使用此值限定失败时的最大重试次数（大于等于 0 时有效，小于 0 为不限制）
        """
        self.__token = token
        self.__server_url = server_url

        self.__http_service = _HttpService(30000, retries=3)
        self.__batch = max(1, flush_size)
        self.__queue = queue.Queue(max(1, queue_size))

        # 初始化发送线程
        self.__flushing_thread = self._AsyncFlushThread(self, max(0, interval))
        self.__flushing_thread.daemon = True
        self.__flushing_thread.start()
        self.__app_id = app_id
        self.__flush_buffer = deque()
        self.__close_retry = close_retry
        self.__sem = threading.Semaphore()

    def get_app_id(self):
        return self.__app_id

    def add(self, get_msg):
        self._add(get_msg())

    def _add(self, msgs: List):
        try:
            for msg in msgs:
                self.__queue.put_nowait(msg)
            _CounterMonitor["async_batch-insert"] += len(msgs)
        except queue.Full as e:
            raise DTNetworkException(e)

        if self.__queue.qsize() > self.__batch:
            self.flush()

    def flush(self):
        self.__flushing_thread.flush()

    def close(self):
        self.flush()
        self.__flushing_thread.stop()

        pre_size = -1
        retried = 0
        # 如果同一批数据发送失败 close_retry 次，则退出循环，以防止无限循环
        while not self.__queue.empty() or len(self.__flush_buffer) > 0:
            crt_size = self.__queue.qsize() + len(self.__flush_buffer)
            if pre_size == crt_size and self.__close_retry >= 0:
                if retried < self.__close_retry:
                    retried += 1
                else:
                    break
            else:
                retried = 0
            Logger.log("当前未发送数据数: {}".format(crt_size))
            pre_size = crt_size
            self._perform_request()

        unsent = self.__queue.qsize() + len(self.__flush_buffer)
        if unsent > 0:
            Logger.error("CLOSED with {} records unsent being discarded！".format(unsent))

    def _need_drain(self):
        return self.__queue.qsize() > self.__batch

    def _perform_request(self):
        """
        同步的发送数据

        仅用于内部调用, 用户不应当调用此方法.
        """
        with self.__sem:
            length = len(self.__flush_buffer)

        while length < self.__batch:
            try:
                with self.__sem:
                    self.__flush_buffer.append(str(self.__queue.get_nowait()))
                    length = len(self.__flush_buffer)
            except queue.Empty:
                break

        if length > 0:
            with self.__sem:
                # split this batch of data to approx 1mb size group.
                splits = _HttpService.approx_split_data_by_mb(self.__flush_buffer)
                self.__flush_buffer = deque()

            for split in splits:
                success = False
                timer = TimeMonitor().start("async_batch-upload")
                try:
                    Logger.warning("len: {}, split: {}".format(len(split), split))
                    success = self.__http_service.post_event(
                        self.__server_url, self.__app_id, self.__token, '[' + ','.join(split) + ']', str(len(split))
                    )
                except DTNetworkException as e:
                    Logger.log("{}: {}".format(e, split), level=logging.WARNING)
                except DTIllegalDataException as e:
                    Logger.log("{}: {}".format(e, split), level=logging.WARNING)
                finally:
                    timer.stop(one_shot=False)

                if success:
                    with self.__sem:
                        _CounterMonitor["async_batch-upload_success"] += len(split)
                else:
                    Logger.warning("Failed to upload events ({})".format(len(split)))
                    with self.__sem:
                        for item in split:
                            self.__flush_buffer.appendleft(item)

    def __del__(self):
        tm = TimeMonitor()
        Logger.log("="*80)
        Logger.log("[Statistics] 'upload' time used sum: {}, avg: {}".format(
            tm.get_sum("async_batch-upload"), tm.get_avg("async_batch-upload")
        ))
        Logger.log("[Statistics] 'track' count: {}".format(_CounterMonitor["async_batch-insert"]))
        Logger.log("[Statistics] 'uploaded' count: {}".format(_CounterMonitor["async_batch-upload_success"]))
        Logger.log("="*80)

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
                Logger.log("run")
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
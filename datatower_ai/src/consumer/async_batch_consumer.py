# -*- coding: utf-8 -*-
import logging
from collections import deque

from datatower_ai.src.util.performance.time_monitor import TimeMonitor

try:
    import queue
except ImportError:
    import Queue as queue
import threading

from datatower_ai.src.util.logger import Logger

from datatower_ai.src.util.exception import DTNetworkException, DTIllegalDataException

from datatower_ai.src.consumer.abstract_consumer import _AbstractConsumer
from datatower_ai.src.service.http_service import _HttpService, _RequestOversizeException, _DataSeparator
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor, count_avg


class AsyncBatchConsumer(_AbstractConsumer):
    """
    异步、批量地向 DT 服务器发送数据

    AsyncBatchConsumer 使用独立的线程进行数据发送，当满足以下两个条件之一时触发数据上报:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """

    def __init__(self, app_id, token, server_url=_HttpService.DEFAULT_SERVER_URL, interval=3, flush_size_kb=1024,
                 queue_size=100000, close_retry=1, num_network_threads=1):
        """
        创建 AsyncBatchConsumer

        Args:
            app_id: 项目的 APP ID
            token: 通信令牌
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
            flush_size_kb: 队列缓存的阈值（单位：kilo-byte），超过此值将立即进行发送
            queue_size: 缓存队列的大小
            close_retry: close() 调用时会尝试进行数据的最后上传，使用此值限定失败时的最大重试次数（大于等于 0 时有效，小于 0 为不限制）
        """
        super(AsyncBatchConsumer, self).__init__()
        self.__token = token
        self.__server_url = server_url

        self.__http_service = _HttpService(30000, retries=3)
        self.__flush_size_kb = max(1, flush_size_kb)
        self.__queue = deque()
        self.__max_queue_size = queue_size

        from datatower_ai.src.util.thread.thread import WorkerManager
        self.__wm = WorkerManager("AsyncBatchConsumer-wm", size=num_network_threads)

        # 初始化发送线程
        self.__flushing_thread = self._AsyncFlushThread(self, max(0, interval), self.__wm)
        self.__flushing_thread.daemon = True
        self.__flushing_thread.start()
        self.__app_id = app_id
        self.__close_retry = close_retry
        self.__sem = threading.Semaphore()


    def get_app_id(self):
        return self.__app_id

    def add(self, get_msg):
        self._add(get_msg())

    def _add(self, msgs, no_flush=False):
        time_at = TimeMonitor().start("async_batch-add_total")
        flush_size_b = self.__flush_size_kb * 1024

        if _CounterMonitor["http_avg_compressed_size"] > flush_size_b:
            _CounterMonitor["http_avg_compress_rate"] -= 0.2        # penalty. if oversize, then decrease the rate
        avg_compress_rate = _CounterMonitor["http_avg_compress_rate"].value
        if avg_compress_rate == 0:
            avg_compress_rate = 15                      # default ACR
        avg_compress_rate = max(0.01, avg_compress_rate)    # minimum 0.01

        with self.__sem:            # lock for the whole insertion phase, to avoid race overhead.
            time_add = TimeMonitor().start("async_batch-add")
            pre_len = len(self.__queue)
            last = self.__queue[-1] if pre_len != 0 else None
            group_id, acc_size = 0, 0
            if last is not None:
                group_id = last.group_id
                acc_size = last.acc_size

            num_group = 1       # counter
            i = 0
            while i < min(len(msgs), self.__max_queue_size-pre_len):
                msg = msgs[i]
                encoded = msg if type(msg) is bytes else msg.encode("utf-8")
                size = len(encoded) / avg_compress_rate         # calc approx size after compressed.

                if acc_size + size >= flush_size_b:
                    num_group += 1
                    group_id = (group_id + 1) & 1       # 0 or 1
                    acc_size = size                     # Resets to current data size
                    self.__queue.append(_QueueItem(encoded, group_id, acc_size))
                    if not no_flush:
                        self.flush()        # notify to flush when reaching flush_size_kb.
                else:
                    acc_size += size                    # Increased by current data size
                    self.__queue.append(_QueueItem(encoded, group_id, acc_size))
                i += 1
            _CounterMonitor["async_batch-insert"] += i
            _CounterMonitor["async_batch-queue_len"] = len(self.__queue)

            if i != len(msgs):
                # Has at least one event dropped.
                _CounterMonitor["async_batch-drop"] += len(msgs) - i
                self.__on_queue_full(len(msgs), i)
            else:
                # All events inserted.
                count_avg("avg_num_groups_per_add", num_group, 1000, 5)
                self.__check_is_queue_reached_threshold(pre_len)
            time_add.stop()
        time_at.stop()

    def __check_is_queue_reached_threshold(self, pre_size):
        crt_size = len(self.__queue)
        threshold = self.__max_queue_size * 0.7
        if pre_size < threshold < crt_size:
            from datatower_ai.src.util.performance.quality_helper import _DTQualityHelper, _DTQualityLevel, _CODE_ASYNC_BATCH_CONSUMER_QUEUE_REACH_THRESHOLD
            msg = ("Queue is reaching threshold (70%)! Caution: data will not be inserted when queue full. "
                   "Max len: {}, current len: {}, flush_size: {}kb, avg upload phase duration: {:.2f}ms").format(
                self.__max_queue_size, crt_size, self.__flush_size_kb, TimeMonitor().get_avg("async_batch-upload_total")
            )
            _DTQualityHelper().report_quality_message(
                app_id=self.get_app_id(),
                code=_CODE_ASYNC_BATCH_CONSUMER_QUEUE_REACH_THRESHOLD,
                msg=msg,
                level=_DTQualityLevel.WARNING
            )
            Logger.warning(msg)
            from datatower_ai.src.bean.pager_code import PAGER_CODE_CONSUMER_AB_QUEUE_REACH_THRESHOLD
            self._page_message(
                PAGER_CODE_CONSUMER_AB_QUEUE_REACH_THRESHOLD,
                "Queue reaching threshold! size: {} ({:.1f}%).".format(
                    crt_size, (crt_size / self.__max_queue_size * 100.0)
                )
            )

    def __on_queue_full(self, crt_batch_len, num_inserted):
        msg = "Queue is full ({})! Need to add: {}, added: {} and dropped: {}, flush_size: {}kb, avg upload phase duration: {:.2f}ms".format(
            self.__max_queue_size, crt_batch_len, num_inserted, crt_batch_len - num_inserted, self.__flush_size_kb,
            TimeMonitor().get_avg("async_batch-upload_total")
        )
        Logger.error("ERROR: " + msg)
        from datatower_ai.src.util.performance.quality_helper import _DTQualityHelper, _DTQualityLevel, _CODE_ASYNC_BATCH_CONSUMER_QUEUE_FULL
        _DTQualityHelper().report_quality_message(
            app_id=self.get_app_id(),
            code=_CODE_ASYNC_BATCH_CONSUMER_QUEUE_FULL,
            msg=msg,
            level=_DTQualityLevel.ERROR
        )
        from datatower_ai.src.bean.pager_code import PAGER_CODE_CONSUMER_AB_QUEUE_FULL
        self._page_message(PAGER_CODE_CONSUMER_AB_QUEUE_FULL, "Queue is full")

    def flush(self):
        self.__flushing_thread.flush()

    def close(self):
        self.flush()
        self.__flushing_thread.stop()

        pre_size = -1
        retried = 0
        # 如果同一批数据发送失败 close_retry 次，则退出循环，以防止无限循环
        while len(self.__queue) > 0:
            crt_size = len(self.__queue)
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

        unsent = len(self.__queue)
        if unsent > 0:
            Logger.error("CLOSED with {} records unsent being discarded！".format(unsent))
        AsyncBatchConsumer._print_statistics()

    def _need_drain(self):
        if len(self.__queue) == 0:
            return False

        with self.__sem:
            if len(self.__queue) == 0:
                return False

            group_id = self.__queue[0].group_id
            for item in self.__queue:
                if item.group_id != group_id:
                    return True
        return False

    def _perform_request(self):
        """
        同步的发送数据

        仅用于内部调用, 用户不应当调用此方法.
        """
        timer_ut = TimeMonitor().start("async_batch-upload_total")

        flush_buffer = []

        # get a group of data
        timer_uffq = TimeMonitor().start("async_batch-upload_fetch_from_queue")
        with self.__sem:
            group_id = None
            if len(self.__queue) != 0:
                group_id = self.__queue[0].group_id

            size = 0
            i = 0
            while i < len(self.__queue) and self.__queue[i].group_id == group_id:
                data = self.__queue.popleft().data
                size += len(data)
                flush_buffer.append(data)

            length = len(flush_buffer)
            _CounterMonitor["async_batch-queue_len"] = len(self.__queue)
            count_avg("async_batch-avg_flush_buffer_len", length, 1000, 5)
            count_avg("async_batch-avg_flush_buffer_size", size, 1000, 5)
        timer_uffq.stop()

        if length > 0:
            success = False
            put_back_to_queue = True
            timer_upload = TimeMonitor().start("async_batch-upload")
            try:
                success = self.__http_service.post_event(
                    self.__server_url, self.__app_id, self.__token, b'[' + b','.join(flush_buffer) + b']', str(length)
                )
            except DTNetworkException as e:
                Logger.log("{}, len: {}".format(e, len(flush_buffer)), level=logging.WARNING)
                from datatower_ai.src.bean.pager_code import PAGER_CODE_COMMON_NETWORK_ERROR
                self._page_message(PAGER_CODE_COMMON_NETWORK_ERROR + e.code, repr(e))
            except DTIllegalDataException as e:
                Logger.log("{}, len: {}".format(e, len(flush_buffer)), level=logging.WARNING)
                from datatower_ai.src.bean.pager_code import PAGER_CODE_COMMON_DATA_ERROR
                self._page_message(PAGER_CODE_COMMON_DATA_ERROR, repr(e))
            except _RequestOversizeException as e:
                Logger.warning("Request Oversize, factor: {}, compression rate: {}, compressed size: {}, len: {}".format(
                    e.factor, e.compression_rate, e.compressed_size, length)
                )
                if length == 1:
                    # a single event is oversize, should be dropped and notify the APIee.
                    # BE never gonna to handle this.
                    put_back_to_queue = False
                    Logger.error("A single event is oversize, this event been dropped!\n{}".format(flush_buffer))
                    from datatower_ai.src.bean.pager_code import PAGER_CODE_COMMON_NETWORK_ERROR
                    from datatower_ai.src.bean.pager_code import PAGER_CODE_SUB_NETWORK_OVERSIZE
                    self._page_message(
                        PAGER_CODE_COMMON_NETWORK_ERROR + PAGER_CODE_SUB_NETWORK_OVERSIZE,
                        "a single event is oversize, this event been dropped!"
                    )
                    _CounterMonitor["async_batch-drop"] += 1
            except Exception as e:
                Logger.exception("Exception occurred")
                from datatower_ai.src.bean.pager_code import PAGER_CODE_CONSUMER_AB_UPLOAD_ERROR
                self._page_message(PAGER_CODE_CONSUMER_AB_UPLOAD_ERROR, repr(e))

            if success:
                _CounterMonitor["async_batch-upload_success"] += length
            elif put_back_to_queue:
                # If failed, putting the current group of data back to the caching queue.
                self._add(flush_buffer, no_flush=True)

            timer_upload.stop()
        timer_ut.stop(should_record=length > 0)

    @staticmethod
    def _print_statistics():
        from datatower_ai.src.util._holder import _Holder
        if not _Holder().show_statistics:
            return
        tm = TimeMonitor()
        Logger.log("="*80)
        Logger.log("[Statistics] 'upload' time used sum: {}, avg: {}".format(
            tm.get_sum("async_batch-upload"), tm.get_avg("async_batch-upload")
        ))
        Logger.log("[Statistics] 'events' count: {}".format(_CounterMonitor["events"]))
        Logger.log("[Statistics] 'uploaded' count: {}".format(_CounterMonitor["async_batch-upload_success"]))
        Logger.log("="*80)

    class _AsyncFlushThread(threading.Thread):      # Trigger thread
        def __init__(self, consumer, interval, wm):
            threading.Thread.__init__(self)
            self._consumer = consumer
            self._interval = interval
            self._wm = wm               # Actual worker thread

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
                self._wm.execute(self._consumer._perform_request)
                self._flush_event.clear()

                # 发现 stop 标志位时安全退出
                if self._stop_event.isSet():
                    self._wm.terminate()    # 关闭 worker manager
                    break
            self._finished_event.set()


class _QueueItem(object):
    def __init__(self, data, group_id, acc_size):
        self.__data = data              # encoded data
        self.__group_id = group_id      # group identifier
        self.__acc_size = acc_size      # current sum of data size in group

    @property
    def data(self):
        return self.__data

    @property
    def group_id(self):
        return self.__group_id

    @property
    def acc_size(self):
        return self.__acc_size

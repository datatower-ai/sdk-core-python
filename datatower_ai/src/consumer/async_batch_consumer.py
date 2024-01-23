# -*- coding: utf-8 -*-
import logging
import time
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
from datatower_ai.src.service.http_service import _HttpService, _RequestOversizeException
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor, count_avg


class AsyncBatchConsumer(_AbstractConsumer):
    """
    异步、批量地向 DT 服务器发送数据

    AsyncBatchConsumer 使用独立的线程进行数据发送，当满足以下两个条件之一时触发数据上报:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """

    def __init__(self, app_id, token, server_url=_HttpService.DEFAULT_SERVER_URL, interval=3, flush_len=10000,
                 queue_size=100000, close_retry=1, num_network_threads=1):
        """
        创建 AsyncBatchConsumer

        Args:
            app_id: 项目的 APP ID
            token: 通信令牌
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
            flush_len: 队列缓存数量的阈值，超过此值将立即进行发送
            queue_size: 缓存队列的大小
            close_retry: close() 调用时会尝试进行数据的最后上传，使用此值限定失败时的最大重试次数（大于等于 0 时有效，小于 0 为不限制）
        """
        super(AsyncBatchConsumer, self).__init__()
        self.__token = token
        self.__server_url = server_url

        self.__http_service = _HttpService(30000, retries=3)
        self.__flush_len = max(1, flush_len)
        self.__queue = deque()
        self.__max_queue_size = max(1, queue_size)
        self.__acc_size = 0

        from datatower_ai.src.util.thread.thread import WorkerManager
        self.__wm = WorkerManager("AsyncBatchConsumer-wm", size=num_network_threads, daemon=True)

        # 初始化发送线程
        self.__timer_thread = self._TimerThread(self, max(0, interval), self.__wm)
        self.__timer_thread.start()
        self.__app_id = app_id
        self.__close_retry = close_retry
        self.__sem = threading.Semaphore()

    def get_app_id(self):
        return self.__app_id

    def add(self, get_msg):
        self.__timer_thread.resume_paused_timer()
        self._add(get_msg())

    def _add(self, msgs, no_flush=False):
        if len(msgs) == 0:
            return

        time_at = TimeMonitor().start("async_batch-add_total")

        encoded_msgs = [x if type(x) is bytes else x.encode("utf-8") for x in msgs]
        pre_len = len(self.__queue)
        acc_len = pre_len % self.__flush_len

        time_add = TimeMonitor().start("async_batch-add")
        inserted = 0
        group_cnt = 0
        with self.__sem:
            for encoded in encoded_msgs:
                if len(self.__queue) >= self.__max_queue_size:
                    break
                self.__queue.append(encoded)
                acc_len += 1
                crt_size = len(encoded)
                if acc_len >= self.__flush_len or self.__acc_size + crt_size == 16000000:
                    # current one is the last item in this group
                    group_cnt += 1
                    acc_len = 0
                    self.__acc_size = 0
                    if not no_flush:
                        self.flush()
                elif self.__acc_size + crt_size > 16000000:
                    # current one belongs to next group
                    group_cnt += 1
                    acc_len = 1
                    self.__acc_size = crt_size
                    if not no_flush:
                        self.flush()
                inserted += 1
            _CounterMonitor["async_batch-insert"] += inserted
            _CounterMonitor["async_batch-queue_len"] = len(self.__queue)
            count_avg("async_batch-avg_queue_len", len(self.__queue), 10000, 1000)
        time_add.stop(should_record=inserted > 0)

        count_avg("avg_num_groups_per_add", group_cnt, 1000, 100)

        if inserted == len(msgs):
            self.__check_is_queue_reached_threshold(pre_len, pre_len+inserted)
        else:
            # has dropped
            self.__on_queue_full(len(msgs), inserted)

        time_at.stop()

    def __check_is_queue_reached_threshold(self, pre_len, crt_len):
        threshold = self.__max_queue_size * 0.7
        if pre_len < threshold <= crt_len:
            from datatower_ai.src.util.performance.quality_helper import _DTQualityHelper, _DTQualityLevel, _CODE_ASYNC_BATCH_CONSUMER_QUEUE_REACH_THRESHOLD
            msg = ("Queue is reaching threshold (70%)! Caution: data will not be inserted when queue full. "
                   "Max len: {}, current len: {}, flush_size: {}, avg upload phase time taken: {:.2f}ms").format(
                self.__max_queue_size, crt_len, self.__flush_len, TimeMonitor().get_avg("async_batch-upload_total")
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
                    crt_len, (crt_len / self.__max_queue_size * 100.0)
                )
            )

    def __on_queue_full(self, crt_batch_len, num_inserted):
        msg = "Queue is full ({})! Need to add: {}, added: {} and dropped: {}, flush_size: {}, avg upload phase time taken: {:.2f}ms".format(
            self.__max_queue_size, crt_batch_len, num_inserted, crt_batch_len - num_inserted, self.__flush_len,
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
        self.__wm.execute(self._perform_request)
        self.__timer_thread.refresh_timer()

    def close(self):
        self.flush()
        self.__timer_thread.stop()

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

    def _perform_request(self):
        """
        同步的发送数据

        仅用于内部调用, 用户不应当调用此方法.
        """
        timer_ut = TimeMonitor().start("async_batch-upload_total")

        # get a group of data
        timer_uffq = TimeMonitor().start("async_batch-upload_fetch_from_queue")
        flush_buffer = []
        with (self.__sem):
            timer_uffqi = TimeMonitor().start("async_batch-upload_fetch_from_queue_in")
            acc_size = 0
            acc_len = 0

            while len(self.__queue) > 0 and acc_len < self.__flush_len:
                crt_data = self.__queue[0]
                crt_size = len(crt_data)
                if acc_size != 0 and acc_size + crt_size > 16000000:
                    # Ensure the size is not exceed, except single oversize event that will try to upload.
                    break
                flush_buffer.append(self.__queue.popleft())
                acc_size += crt_size
                acc_len += 1

            queue_size = len(self.__queue)
            _CounterMonitor["async_batch-queue_len"] = queue_size
            count_avg("async_batch-avg_queue_len", queue_size, 10000, 1000)

            length = len(flush_buffer)
            if length > 0:
                count_avg("async_batch-avg_flush_buffer_len", length, 1000, 5)
                count_avg("async_batch-avg_flush_buffer_size", acc_size, 1000, 5)

            if queue_size == 0:
                # if queue empty, reset the acc_size
                self.__acc_size = 0
            timer_uffqi.stop(should_record=length > 0)
        timer_uffq.stop(should_record=length > 0)

        timer_upload = TimeMonitor().start("async_batch-upload")
        if length > 0:
            success = False
            put_back_to_queue = True
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
                Logger.warning(
                    "Request Oversize, data size: {:.0f}b, size limit: {:.0f}b, compressed size: {:.0f}b".format(
                        e.origin_size, e.size_limit, e.compressed_size
                    )
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
                count_avg("async_batch-avg_upload_success", length, 10000, 100)
            elif put_back_to_queue:
                # If failed, putting the current group of data back to the caching queue.
                self._add(flush_buffer, no_flush=True)
        timer_upload.stop(should_record=length > 0)
        timer_ut.stop(should_record=length > 0)

    @staticmethod
    def _print_statistics():
        from datatower_ai.src.util._holder import _Holder
        if not _Holder().show_statistics:
            return
        tm = TimeMonitor()
        Logger.log("="*80)
        Logger.log("[Statistics] add phase time used sum: {}, avg: {}".format(
            tm.get_sum("async_batch-add_total"), tm.get_avg("async_batch-add_total")
        ))
        Logger.log("[Statistics] upload phase time used sum: {}, avg: {}".format(
            tm.get_sum("async_batch-upload_total"), tm.get_avg("async_batch-upload_total")
        ))
        Logger.log("[Statistics] 'events' count: {}".format(_CounterMonitor["events"]))
        Logger.log("[Statistics] 'uploaded' count: {}".format(_CounterMonitor["async_batch-upload_success"]))
        Logger.log("[Statistics] 'dropped' count: {}".format(_CounterMonitor["async_batch-drop"]))
        Logger.log("[Statistics] avgerage number of groups per add: {:.2f}".format(_CounterMonitor["avg_num_groups_per_add"].value))
        Logger.log("[Statistics] average upload length: {:.2f}, size: {:.2f}B ({:.2f}B)".format(
            _CounterMonitor["async_batch-avg_flush_buffer_len"].value,
            _CounterMonitor["async_batch-avg_flush_buffer_size"].value,
            _CounterMonitor["http_avg_compressed_size"].value
        ))
        Logger.log("="*80)

    def _can_upload(self):
        return len(self.__queue) > 0

    def _queue_size(self):
        return len(self.__queue)

    class _TimerThread(threading.Thread):
        """ Timer thread,  """
        def __init__(self, consumer, interval, wm):
            threading.Thread.__init__(self)
            self.daemon = True
            self._consumer = consumer
            self._interval = interval
            self._wm = wm               # Actual worker thread

            self._stop_event = threading.Event()
            self._finished_event = threading.Event()
            self._awake_event = threading.Event()
            self._refresh_event = threading.Event()
            self._op_sem = threading.Semaphore()
            self._resume_empty_event = threading.Event()

        def refresh_timer(self):
            """
            refresh the timer by rewaiting.
            """
            with self._op_sem:
                self._resume_empty_event.set()
                self._refresh_event.set()
                self._awake_event.set()

        def resume_paused_timer(self):
            with self._op_sem:
                self._resume_empty_event.set()

        def stop(self):
            """
            停止线程
            退出时需调用此方法，以保证线程安全结束.
            """
            with self._op_sem:
                self._resume_empty_event.set()
                self._stop_event.set()
                self._awake_event.set()
                self._finished_event.wait()

        def run(self):
            while True:
                # Wait for awaking or reaching timeout.
                self._awake_event.wait(self._interval)

                # 发现 stop 标志位时安全退出
                if self._stop_event.isSet():
                    Logger.debug("Stopping TimerT, will trigger the last upload")
                    self._wm.execute(self._consumer._perform_request)   # do the last upload
                    break

                # Refresh the timer, if flag set
                if self._refresh_event.isSet():
                    self._refresh_event.clear()
                    self._awake_event.clear()
                    continue

                if not self._consumer._can_upload():
                    Logger.debug("TimerT paused due to no need to upload")
                    self._resume_empty_event.clear()
                    self._resume_empty_event.wait()        # w/o timeout, prevent unneeded idle polling.
                else:
                    Logger.debug("TimerT triggering an upload")
                    self._wm.execute(self._consumer._perform_request)
                    self._awake_event.clear()

            self._wm.terminate()    # 关闭 worker manager
            self._finished_event.set()

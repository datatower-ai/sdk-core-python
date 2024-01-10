# coding=utf-8
import os
import time

from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor
from datatower_ai.src.util.performance.time_monitor import TimeMonitor
from datatower_ai.src.util.thread.thread import WorkerManager

from datatower_ai.src.util.json_util import json_loads_byteified


def track(worker_manager, size, dt, event_name, props, meta):
    try:
        for _ in range(size):
            dt.track(dt_id=meta.get("#dt_id", None), acid=meta.get("#acid", None), event_name=event_name,
                     properties=props, meta=meta)
    except Exception as e:
        print(type(e))
        # os._exit(1)         # force quit


def handle(dt, args):
    json_object = json_loads_byteified(args.json)
    props = json_object.pop("properties", None)
    meta = json_object
    event_name = json_object.get("#event_name", "")

    gap = args.gap
    init_size = args.init_size
    incr_beg_offset = args.incr_beg_offset
    incr_gap = args.incr_gap
    incr_size = args.incr_size
    max_size = args.max_size

    print("[TEST2] Test (gap: {}, init_size: {}, incr_beg_offset: {}, incr_gap: {}, incr_size: {}) - {}".format(
        gap, init_size, incr_beg_offset, incr_gap, incr_size, event_name
    ))

    worker_manager = WorkerManager("test_worker_manager", size=5)
    tm = TimeMonitor()

    beg_time = time.time()
    starts_to_incr = False
    last_incr_time = 0

    rounds = 1
    size = init_size
    while True:
        crt_time = time.time()
        if not starts_to_incr and crt_time - beg_time >= (incr_beg_offset / 1000.0):
            starts_to_incr = True
        if starts_to_incr and size < max_size and crt_time - last_incr_time >= (incr_gap / 1000.0):
            last_incr_time = crt_time
            size = min(max_size, size + incr_size)

        print("[TEST2] Starting task with size: {}, round: {}, time elapsed: {:.2f}ms, avg upload time: {:.2f}ms, "
              "track count: {}, uploaded count: {}, "
              "queue size: {}, inserted to queue: {}, dropped: {}, "
              "avg compress rate: {:.4f}, avg upload count: {}".format(
            size, rounds, (crt_time - beg_time) * 1000, tm.get_avg("async_batch-upload"),
            _CounterMonitor["events"], _CounterMonitor["async_batch-upload_success"],
            _CounterMonitor["async_batch-queue_size"], _CounterMonitor["async_batch-insert"], _CounterMonitor["async_batch-drop"],
            _CounterMonitor["http_avg_compress_rate"].value, _CounterMonitor["http_avg_compress_len"]
        ))
        worker_manager.execute(lambda: track(worker_manager, size, dt, event_name, props, meta))

        rounds += 1
        delta = time.time() - crt_time
        time.sleep(max(0, gap / 1000.0 - delta))


def init_parser(parser):
    parser.add_argument("json", type=str, help=None)
    parser.add_argument("--gap", type=int, default=1000, help=None)             # 间隔时间
    parser.add_argument("--init_size", type=int, default=1, help=None)          # 初始大小
    parser.add_argument("--incr_beg_offset", type=int, default=1000, help=None)    # 多久后开始增量
    parser.add_argument("--incr_gap", type=int, default=1000, help=None)        # 增量间隔时间
    parser.add_argument("--incr_size", type=int, default=0, help=None)          # 每次增量大小
    parser.add_argument("--max_size", type=int, default=1, help=None)           # 最大大小

    parser.set_defaults(op=handle)

# coding=utf-8
from __future__ import print_function
import time

from datatower_ai import Event
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor
from datatower_ai.src.util.performance.time_monitor import TimeMonitor
from datatower_ai.src.util.thread.thread import WorkerManager

from datatower_ai.src.util.json_util import json_loads_byteified


def track(worker_manager, size, dt, event_name, props, meta, use_batch_api):
    try:
        if use_batch_api:
            dt.track_batch(*[
                Event(dt_id=meta.get("#dt_id", None), acid=meta.get("#acid", None), event_name=event_name,
                      properties=props, meta=meta) for _ in range(size)
            ])
        else:
            for _ in range(size):
                dt.track(dt_id=meta.get("#dt_id", None), acid=meta.get("#acid", None), event_name=event_name,
                         properties=props, meta=meta)

    except Exception as e:
        print(type(e))
        # os._exit(1)         # force quit


def pager_func(code, msg):
    print("[TEST2 | PAGER] code: {}, msg: {}".format(code, msg))


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
    use_batch_api = args.batch_api

    dt.register_pager(pager_func)

    print("[TEST2] Test (gap: {}, init_size: {}, incr_beg_offset: {}, incr_gap: {}, incr_size: {}) - {}".format(
        gap, init_size, incr_beg_offset, incr_gap, incr_size, event_name
    ))

    wm_size = 5
    worker_manager = WorkerManager("test_worker_manager", size=wm_size)
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
              "avg compress rate: {:.4f}, avg num groups per add: {:.2f}, "
              "avg flush buffer len: {:.2f}, avg flush buffer size: {:.2f}b, "
              "avg compressed size: {:.2f}b, "
              "avg fetch from queue duration: {:.2f}ms ({:.2f}ms), avg compress duration: {:.2f}ms, "
              "avg post duration: {:.2f}ms, avg upload phase duration: {:.2f}ms, "
              "avg add time: {:.2f}ms, avg total add time: {:.2f}ms".format(
                size, rounds, (crt_time - beg_time) * 1000, tm.get_avg("async_batch-upload"),
                _CounterMonitor["events"], _CounterMonitor["async_batch-upload_success"],
                _CounterMonitor["async_batch-queue_len"], _CounterMonitor["async_batch-insert"],
                _CounterMonitor["async_batch-drop"],
                _CounterMonitor["http_avg_compress_rate"].value, _CounterMonitor["avg_num_groups_per_add"].value,
                _CounterMonitor["async_batch-avg_flush_buffer_len"].value,
                _CounterMonitor["async_batch-avg_flush_buffer_size"].value,
                _CounterMonitor["http_avg_compressed_size"].value,
                tm.get_avg("async_batch-upload_fetch_from_queue"), tm.get_avg("async_batch-upload_fetch_from_queue_in"), tm.get_avg("http_avg_compress_duration"),
                tm.get_avg("http_post"), tm.get_avg("async_batch-upload_total"),
                tm.get_avg("async_batch-add"), tm.get_avg("async_batch-add_total")
              ),
        )

        if size < wm_size:
            worker_manager.execute(lambda: track(worker_manager, size, dt, event_name, props, meta, use_batch_api))
        else:
            for _ in range(wm_size - 1):
                worker_manager.execute(lambda: track(worker_manager, size // wm_size, dt, event_name, props, meta, use_batch_api))
            worker_manager.execute(
                lambda: track(worker_manager, (size // wm_size) + (size % wm_size), dt, event_name, props, meta, use_batch_api)
            )

        (col, row) = getTerminalSize()
        try:
            import psutil
            cpu = "{:.1f}%".format(psutil.cpu_percent())
            mem = "{:.1f}%".format(psutil.virtual_memory()[2])
        except Exception as e:
            cpu = "N/A"
            mem = "N/A"

        tu = int((crt_time - beg_time) * 1000)
        hr = tu // 3600000
        mins = tu // 60000 % 60
        sec = tu // 1000 % 60
        ms = tu % 1000
        ts = "{}:{:02d}:{:02d}.{:03d}".format(hr, mins, sec, ms)

        pinned = "> Round: {}, Time used: {}, tracked: {}, uploaded: {}, dropped: {}, queue len: {} ({:.2f}%), avg add time used: {:.2f}ms, avg upload time used: {:.2f}ms, CPU: {}, MEM: {}".format(
            rounds,
            ts,
            _CounterMonitor["events"],
            _CounterMonitor["async_batch-upload_success"],
            _CounterMonitor["async_batch-drop"],
            _CounterMonitor["async_batch-queue_len"],
            _CounterMonitor["async_batch-queue_len"] / args.queue_size * 100,
            tm.get_avg("async_batch-add_total"),
            tm.get_avg("async_batch-upload_total"),
            cpu,
            mem
        )
        import math
        backs = max(0, int(math.ceil(len(pinned) / col))-1)
        print("\033[94m{}\033[0m\r".format(pinned), end="\033[{}A".format(backs) if backs > 0 else "")

        rounds += 1
        delta = time.time() - crt_time
        time.sleep(max(0, gap / 1000.0 - delta))


def getTerminalSize():
    # http://stackoverflow.com/a/566752/2646228
    import os
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct, os
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,'1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))
    return int(cr[1]), int(cr[0])


def init_parser(parser):
    parser.add_argument("json", type=str, help=None)
    parser.add_argument("--gap", type=int, default=1000, help=None)  # 间隔时间
    parser.add_argument("--init_size", type=int, default=1, help=None)  # 初始大小
    parser.add_argument("--incr_beg_offset", type=int, default=1000, help=None)  # 多久后开始增量
    parser.add_argument("--incr_gap", type=int, default=1000, help=None)  # 增量间隔时间
    parser.add_argument("--incr_size", type=int, default=0, help=None)  # 每次增量大小
    parser.add_argument("--max_size", type=int, default=1, help=None)  # 最大大小
    parser.add_argument("-batch_api", action="store_true", help=None)  # 是否使用 batch api

    parser.set_defaults(op=handle)

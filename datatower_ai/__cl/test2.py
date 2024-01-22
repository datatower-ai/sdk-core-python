# coding=utf-8
from __future__ import print_function

import random
import time

from datatower_ai import Event
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor
from datatower_ai.src.util.performance.time_monitor import TimeMonitor
from datatower_ai.src.util.thread.thread import WorkerManager

from datatower_ai.src.util.json_util import json_loads_byteified

_PURE_CHAR_SET = "qwertyuiopasdfghjklzxcvbnm"
_RANDOM_CHAR_SET = "qwertyuiopasdfghjklzxcvbnm1234567890-=_+[]{};:,.<>/?!@#$%^&*()`~|"


def random_string(length, char_set=_RANDOM_CHAR_SET):
    return ''.join(random.choice(char_set) for i in range(length))


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

jump_lines = 1

def safe_handle(dt, args):
    try:
        handle(dt, args)
    except:
        print("\n"*jump_lines)
        pass


def handle(dt, args):
    global jump_lines
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
    num_rand_str = args.num_random_str
    rand_str_len = args.random_str_len

    dt.register_pager(pager_func)

    print("[TEST2] Test (gap: {}, init_size: {}, incr_beg_offset: {}, incr_gap: {}, incr_size: {}) - {}".format(
        gap, init_size, incr_beg_offset, incr_gap, incr_size, event_name
    ))

    wm_size = args.num_send_threads
    worker_manager = WorkerManager("test_worker_manager", size=wm_size)
    tm = TimeMonitor()

    beg_time = time.time()
    starts_to_incr = False
    last_incr_time = 0

    max_rounds = args.max_rounds
    rounds = 1
    size = init_size
    while max_rounds is None or rounds <= max_rounds:
        crt_time = time.time()
        if not starts_to_incr and crt_time - beg_time >= (incr_beg_offset / 1000.0):
            starts_to_incr = True
        if starts_to_incr and size < max_size and crt_time - last_incr_time >= (incr_gap / 1000.0):
            last_incr_time = crt_time
            size = min(max_size, size + incr_size)

        print("[TEST2] Starting task with size: {}, round: {}, time elapsed: {:.2f}ms, avg upload time: {:.2f}ms, "
              "track count: {}, uploaded count: {}, "
              "queue length: {} (avg: {:.1f}), "
              "inserted to queue: {}, dropped: {}, "
              "avg compress rate: {:.4f}, avg num groups per add: {:.2f}, "
              "avg flush buffer len: {:.2f}, avg flush buffer size: {:.2f}b, "
              "avg compressed size: {:.2f}b, "
              "avg fetch from queue duration: {:.2f}ms ({:.2f}ms), avg compress duration: {:.2f}ms, "
              "avg post duration: {:.2f}ms, avg upload phase duration: {:.2f}ms, "
              "avg add time: {:.2f}ms, avg total add time: {:.2f}ms".format(
                size, rounds, (crt_time - beg_time) * 1000, tm.get_avg("async_batch-upload"),
                _CounterMonitor["events"], _CounterMonitor["async_batch-upload_success"],
                _CounterMonitor["async_batch-queue_len"], _CounterMonitor["async_batch-avg_queue_len"].value,
                _CounterMonitor["async_batch-insert"], _CounterMonitor["async_batch-drop"],
                _CounterMonitor["http_avg_compress_rate"].value, _CounterMonitor["avg_num_groups_per_add"].value,
                _CounterMonitor["async_batch-avg_flush_buffer_len"].value,
                _CounterMonitor["http_avg_original_size"].value,
                _CounterMonitor["http_avg_compressed_size"].value,
                tm.get_avg("async_batch-upload_fetch_from_queue"), tm.get_avg("async_batch-upload_fetch_from_queue_in"), tm.get_avg("http_avg_compress_duration"),
                tm.get_avg("http_post"), tm.get_avg("async_batch-upload_total"),
                tm.get_avg("async_batch-add"), tm.get_avg("async_batch-add_total")
              ),
        )
        jump_lines = print_pinned(args, rounds, beg_time, tm)

        if num_rand_str != 0 and rand_str_len != 0:
            for i in range(num_rand_str):
                props["rand_str_" + str(i)] = random_string(abs(rand_str_len))

        if size < wm_size:
            worker_manager.execute(lambda: track(worker_manager, size, dt, event_name, props, meta, use_batch_api))
        else:
            for _ in range(wm_size - 1):
                worker_manager.execute(lambda: track(worker_manager, size // wm_size, dt, event_name, props, meta, use_batch_api))
            worker_manager.execute(
                lambda: track(worker_manager, (size // wm_size) + (size % wm_size), dt, event_name, props, meta, use_batch_api)
            )

        rounds += 1
        delta = time.time() - crt_time
        time.sleep(max(0, gap / 1000.0 - delta))
    print("\n" * jump_lines)
    worker_manager.terminate()


def print_pinned(args, rounds, beg_time, tm):
    (col, row) = getTerminalSize()
    try:
        import psutil
        cpu = "{:.1f}%".format(psutil.cpu_percent())
        mem = "{:.1f}%".format(psutil.virtual_memory()[2])
    except Exception as e:
        cpu = "N/A"
        mem = "N/A"

    rtu = time.time() - beg_time
    tu = int(rtu * 1000)
    hr = tu // 3600000
    mins = tu // 60000 % 60
    sec = tu // 1000 % 60
    ms = tu % 1000
    ts = "{}:{:02d}:{:02d}.{:03d}".format(hr, mins, sec, ms)

    avg_upload_size = _CounterMonitor["http_avg_compressed_size"].value
    if avg_upload_size > 1024 * 1024:
        unit_upload_size = "{:.2f}MB".format(avg_upload_size / 1024 / 1024)
    elif avg_upload_size > 1024:
        unit_upload_size = "{:.2f}KB".format(avg_upload_size / 1024)
    else:
        unit_upload_size = "{:.2f}B".format(avg_upload_size)

    avg_org_upload_size = _CounterMonitor["http_avg_original_size"].value
    if avg_org_upload_size > 1024 * 1024:
        unit_org_upload_size = "{:.2f}MB".format(avg_org_upload_size / 1024 / 1024)
    elif avg_org_upload_size > 1024:
        unit_org_upload_size = "{:.2f}KB".format(avg_org_upload_size / 1024)
    else:
        unit_org_upload_size = "{:.2f}B".format(avg_org_upload_size)

    avg_upload_total_time = tm.get_avg("async_batch-upload_total")
    qps_theoretical = _CounterMonitor["async_batch-avg_upload_success"] * 1000 / avg_upload_total_time
    qps_buffered = qps_theoretical * 1 / 1.2

    pinned = (
        "> Round: {}, Time used: {}, tracked: {}, uploaded: {}, dropped: {}, queue len: {} ({:.2f}%), avg queue len: {:.1f} ({:.2f}%), avg add time used: {:.2f}ms, avg upload time used: {:.2f}ms\n"
        "> Avg original size: {}, avg compressed size: {}\n"
        "> CPU: {}, MEM: {}, approx QPS: {} | {} | {}"
    ).format(
        rounds,
        ts,
        _CounterMonitor["events"],
        _CounterMonitor["async_batch-upload_success"],
        _CounterMonitor["async_batch-drop"],
        _CounterMonitor["async_batch-queue_len"],
        _CounterMonitor["async_batch-queue_len"] / args.queue_size * 100,
        _CounterMonitor["async_batch-avg_queue_len"].value,
        _CounterMonitor["async_batch-avg_queue_len"].value / args.queue_size * 100,
        tm.get_avg("async_batch-add_total"),
        avg_upload_total_time,
        unit_org_upload_size, unit_upload_size,
        cpu,
        mem,
        "{:.2f}".format(_CounterMonitor["async_batch-upload_success"] / rtu) if rtu != 0 else "0.0",
        "{:.2f}".format(qps_theoretical) if avg_upload_total_time > 0 else "0.0",
        "{:.2f}".format(qps_buffered) if avg_upload_total_time > 0 else "0.0",
    )
    import math
    backs = max(0, sum(max(1, int(math.ceil(len(x) / col))) for x in pinned.split("\n")) - 1)
    print("\033[94m{}\033[0m\r".format(pinned), end="\033[{}A".format(backs))
    return backs


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
    parser.add_argument("--num_send_threads", type=int, default=5, help=None)  # 同时调用接口的线程数
    parser.add_argument("--max_rounds", type=int, default=None, help=None)  # 最大轮数
    parser.add_argument("--num_random_str", type=int, default=0, help=None)     # 随机字符串的个数
    parser.add_argument("--random_str_len", type=int, default=0, help=None)     # 随机字符串的长度

    parser.set_defaults(op=safe_handle)

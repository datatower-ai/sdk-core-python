"""Provides ability to run this package directly from cl without integrated to project."""

import argparse
import json
import logging
import re
import time
from typing import Optional

from datatower_ai import *
from datatower_ai.__version__ import __version__


def _track(args):
    print("Track")
    print("name: {}".format(args.name))
    print("properties: {}".format(args.properties))
    json_obj = json.loads(args.properties)
    dt.track(dt_id=json_obj.get("#dt_id", None), acid=json_obj.get("#acid", None), event_name=args.name, properties=json_obj)


def _user_add(args):
    print("User add")
    print("a: {}".format(args.a))


def _test(args):
    print("Test")
    print("n: {}".format(args.n))
    from datatower_ai.src.service.http_service import _HttpService
    _HttpService._simulate = None if args.ns_sim < 0 else args.ns_sim
    for i in range(0, args.n):
        dt.track("123", None, "test_event", {"#bundle_id": "", "i": i})
    dt.flush()
    dt.close()


def _test_batch(args):
    print("Test")
    print("m: {}".format(args.m))
    print("n: {}".format(args.n))
    from datatower_ai.src.service.http_service import _HttpService
    _HttpService._simulate = None if args.ns_sim < 0 else args.ns_sim
    for i in range(0, args.n):
        dt.track_batch(*[Event("123", None, "test_event_batch", {"i": i}, {"#bundle_id": ""}) for _ in range(args.m)])
    dt.flush()
    dt.close()


# ══════════════════════════ Arguments ══════════════════════════
parser = argparse.ArgumentParser(description="DataTower.ai SDK - Python (v{})".format(__version__))

# ┌──────┐
# │ Info │
# └──────┘
parser.add_argument("-v", "--version", action='version', version="{}".format(__version__))

# ┌───────────────────┐
# │ General Arguments │
# └───────────────────┘
# Required Args
parser.add_argument("--app_id", type=str, required=True, help="App id generated from DataTower.ai Dashboard.")
parser.add_argument("--server_url", type=str, required=True, help="Server URL generated from DataTower.ai Dashboard.")
parser.add_argument("--token", type=str, required=True, help="Verification token set at DataTower.ai Dashboard.")
parser.add_argument("--consumer", type=str, required=True, help="The consumer used to handle events.")
# Optional Args
parser.add_argument("--log_level", type=str, default="debug", help="Log level, available only if -d present.")
# Flags
parser.add_argument("-d", dest="f_debug", action="store_true", help="Debug mode.")

# ┌─────────────────────────────┐
# │ Consumer Specific Arguments │
# └─────────────────────────────┘
# async_batch
parser.add_argument("--interval", default=3, type=int)
parser.add_argument("--flush_size", default=20, type=int)
parser.add_argument("--queue_size", default=100000, type=int)
parser.add_argument("--close_retry", default=1, type=int)

# database_cache
parser.add_argument("--batch_size", default=50, type=int)
parser.add_argument("--network_retries", default=3, type=int)
parser.add_argument("--network_timeout", default=3000, type=int)
parser.add_argument("--num_db_threads", default=2, type=int)
parser.add_argument("--num_network_threads", default=2, type=int)
parser.add_argument("--thread_keep_alive_ms", default=1000, type=int)
parser.add_argument("--cache_size", default=5000, type=int)
parser.add_argument("--exceed_insertion_strategy", default="delete", type=str)

# ┌────────────────────────┐
# │ API Specific Arguments │
# └────────────────────────┘
subparsers = parser.add_subparsers(required=True)

track_sp = subparsers.add_parser("track", help="Tracking an event")
track_sp.add_argument("name", type=str, help="Event name")
track_sp.add_argument("properties", type=str, help="Properties (Json String)")
track_sp.set_defaults(op=(_track, "track"))

user_add_sp = subparsers.add_parser("user_add", help="XXXXX")
user_add_sp.add_argument("a", type=str, help="XXXXX")
user_add_sp.set_defaults(op=(_user_add, "user_add"))

test_sp = subparsers.add_parser("test", add_help=False)
test_sp.add_argument("n", type=int, help="XXXXX")
test_sp.set_defaults(op=(_test, "test"))

test_batch_sp = subparsers.add_parser("test_batch", add_help=False)
test_batch_sp.add_argument("m", type=int, help="size of each batch")
test_batch_sp.add_argument("n", type=int, help="num of batches")
test_batch_sp.set_defaults(op=(_test_batch, "test_batch"))


# ┌──────────────────┐
# │ Others Arguments │
# └──────────────────┘
parser.add_argument("--ns_sim", default=-1, type=int)

# ═══════════════════════════════════════════════════════════════
args = parser.parse_args()
# ═══════════════════════════════════════════════════════════════

print("Initializing...")
sep_length = max(len(args.app_id) + 10, len(args.server_url) + 14, + len(args.token) + 9, + len(args.token) + 13)
print("┏" + ("━"*sep_length) + "┓")
print("┃ \033[1mapp_id\033[0m: {:{width}} ┃".format(args.app_id, width=sep_length-10))
print("┃ \033[1mserver_url\033[0m: {:{width}} ┃".format(args.server_url, width=sep_length-14))
print("┃ \033[1mtoken\033[0m: {:{width}} ┃".format(args.token, width=sep_length-9))
print("┃ \033[1mdebug\033[0m: {:{width}} ┃".format(str(args.f_debug), width=sep_length-9))

arg_log_level = args.log_level.lower()
if re.search("^i(nfo)?$", arg_log_level):
    log_level = logging.INFO
elif re.search("^w(arn)?$", arg_log_level):
    log_level = logging.WARNING
elif re.search("^e(rror)?$", arg_log_level):
    log_level = logging.ERROR
else:
    log_level = logging.DEBUG
print("┃ \033[1mlog_level\033[0m: {:{width}} ┃".format(log_level, width=sep_length-13))

consumer_name = args.consumer
consumer = None
if consumer_name.lower() == "database_cache":
    exceed_insertion_strategy = ExceedInsertionStrategy.DELETE
    if args.exceed_insertion_strategy.upper() == "DISCARD":
        exceed_insertion_strategy = ExceedInsertionStrategy.DISCARD
    elif args.exceed_insertion_strategy.upper() == "DISCARD_HEAD":
        exceed_insertion_strategy = ExceedInsertionStrategy.DISCARD_HEAD
    elif args.exceed_insertion_strategy.upper() == "ABORT":
        exceed_insertion_strategy = ExceedInsertionStrategy.ABORT
    elif args.exceed_insertion_strategy.upper() == "IGNORE":
        exceed_insertion_strategy = ExceedInsertionStrategy.IGNORE

    consumer = DatabaseCacheConsumer(
        args.app_id, args.token, args.server_url,
        batch_size=args.batch_size,
        network_retries=args.network_retries,
        network_timeout=args.network_timeout,
        num_db_threads=args.num_db_threads,
        num_network_threads=args.num_network_threads,
        thread_keep_alive_ms=args.thread_keep_alive_ms,
        cache_size=args.cache_size,
        exceed_insertion_strategy=args.exceed_insertion_strategy
    )
elif consumer_name.lower() == "async_batch":
    consumer = AsyncBatchConsumer(
        args.app_id, args.server_url, args.token,
        interval=args.interval,
        flush_size=args.flush_size,
        queue_size=args.queue_size,
        close_retry=args.close_retry
    )
else:
    raise ValueError("Given --consumer \"{}\" is not valid!".format(consumer_name))

dt = DTAnalytics(consumer=consumer, debug=args.f_debug, log_level=log_level,)

print("┗" + ("━"*sep_length) + "┛")
print("Initialized!")
print("Starting - {}".format(args.op[1]))
args.op[0](args)     # Call Api func

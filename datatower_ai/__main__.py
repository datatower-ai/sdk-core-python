# -*- coding: utf-8 -*-
"""Provides ability to run this package directly from __cl without integrated to project."""

import argparse
import logging
import re

from datatower_ai import *
from datatower_ai.__version__ import __version__
import datatower_ai.__cl.user as user
import datatower_ai.__cl.analytics as analytics
import datatower_ai.__cl.raw_track as raw_tracker
import datatower_ai.__cl.test as test
import datatower_ai.__cl.ad as ad
from datatower_ai.src.util._holder import _Holder


def _test(dt, args):
    print("Test")
    print("n: {}".format(args.n))
    for i in range(0, args.n):
        dt.track("123", None, "test_event", {"#bundle_id": "", "i": i})
    dt.flush()
    dt.close()


def _test_batch(dt, args):
    print("Test")
    print("m: {}".format(args.m))
    print("n: {}".format(args.n))
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
subparsers = parser.add_subparsers(dest="feature")

user.init_parser(subparsers.add_parser("user", help="User related APIs"))
analytics.init_parser(subparsers.add_parser("track", help="Track a event"))
raw_tracker.init_parser(subparsers.add_parser("track_raw", help="Track a event by json string"))
ad.init_parser(subparsers.add_parser("ad", add_help=False))
test.init_parser(subparsers.add_parser("testt", add_help=False))

# test_sp = subparsers.add_parser("test", add_help=False)
# test_sp.add_argument("n", type=int, help="XXXXX")
# test_sp.set_defaults(op=_test)
#
# test_batch_sp = subparsers.add_parser("test_batch", add_help=False)
# test_batch_sp.add_argument("m", type=int, help="size of each batch")
# test_batch_sp.add_argument("n", type=int, help="num of batches")
# test_batch_sp.set_defaults(op=_test_batch)

# ┌──────────────────┐
# │ Others Arguments │
# └──────────────────┘
parser.add_argument("--ns_sim", default=-1, type=int)
parser.add_argument("-show_statistics", dest="f_show_statistics", action="store_true", help=None)

# ═══════════════════════════════════════════════════════════════
args = parser.parse_args()
# ═══════════════════════════════════════════════════════════════


arg_log_level = args.log_level.lower()
if re.search("^i(nfo)?$", arg_log_level):
    log_level = logging.INFO
    log_level_word = "Info"
elif re.search("^w(arn)?$", arg_log_level):
    log_level = logging.WARNING
    log_level_word = "Warning"
elif re.search("^e(rror)?$", arg_log_level):
    log_level = logging.ERROR
    log_level_word = "Error"
else:
    log_level = logging.DEBUG
    log_level_word = "Debug"

sep_length = max(len(args.app_id) + 10, len(args.server_url) + 14, len(args.token) + 9, len(str(args.f_debug)) + 9,
                 len(log_level_word) + 13)
print("┏" + ("━"*sep_length) + "┓")
print("┃ \033[1mapp_id\033[0m: {:{width}} ┃".format(args.app_id, width=sep_length-10))
print("┃ \033[1mserver_url\033[0m: {:{width}} ┃".format(args.server_url, width=sep_length-14))
print("┃ \033[1mtoken\033[0m: {:{width}} ┃".format(args.token, width=sep_length-9))
print("┃ \033[1mdebug\033[0m: {:{width}} ┃".format(str(args.f_debug), width=sep_length-9))
if args.f_debug:
    print("┃ \033[1mlog_level\033[0m: {:{width}} ┃".format(log_level_word, width=sep_length-13))

consumer_name = args.consumer
consumer = None
if consumer_name.lower() == "database_cache":
    # exceed_insertion_strategy = ExceedInsertionStrategy.DELETE
    # if args.exceed_insertion_strategy.upper() == "DISCARD":
    #     exceed_insertion_strategy = ExceedInsertionStrategy.DISCARD
    # elif args.exceed_insertion_strategy.upper() == "DISCARD_HEAD":
    #     exceed_insertion_strategy = ExceedInsertionStrategy.DISCARD_HEAD
    # elif args.exceed_insertion_strategy.upper() == "ABORT":
    #     exceed_insertion_strategy = ExceedInsertionStrategy.ABORT
    # elif args.exceed_insertion_strategy.upper() == "IGNORE":
    #     exceed_insertion_strategy = ExceedInsertionStrategy.IGNORE
    #
    # consumer = DatabaseCacheConsumer(
    #     app_id=args.app_id,
    #     token=args.token,
    #     server_url=args.server_url,
    #     batch_size=args.batch_size,
    #     network_retries=args.network_retries,
    #     network_timeout=args.network_timeout,
    #     num_db_threads=args.num_db_threads,
    #     num_network_threads=args.num_network_threads,
    #     thread_keep_alive_ms=args.thread_keep_alive_ms,
    #     cache_size=args.cache_size,
    #     exceed_insertion_strategy=args.exceed_insertion_strategy
    # )
    raise ValueError("database_cache consumer is not available at this time!".format(consumer_name))
elif consumer_name.lower() == "async_batch":
    consumer = AsyncBatchConsumer(
        app_id=args.app_id,
        token=args.token,
        server_url=args.server_url,
        interval=args.interval,
        flush_size=args.flush_size,
        queue_size=args.queue_size,
        close_retry=args.close_retry
    )
else:
    raise ValueError("Given --consumer \"{}\" is not valid!".format(consumer_name))

dt = DTAnalytics(consumer=consumer, debug=args.f_debug, log_level=log_level,)
_Holder().show_statistics = args.f_show_statistics

from datatower_ai.src.service.http_service import _HttpService
_HttpService._simulate = args.ns_sim

print("┗" + ("━"*sep_length) + "┛")
args.op(dt, args)     # Call Api func

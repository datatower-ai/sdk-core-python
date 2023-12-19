import json

from datatower_ai import *


def handle(dt: DTAnalytics, args):
    n = args.n
    m = args.m
    jo = json.loads(args.json)
    props = jo.pop("properties", None)
    meta = jo
    event_name = jo.get("#event_name", "")

    print("Test (n: {}, m: {}) - {}".format(n, m, event_name))

    for _ in range(n):
        if m:
            dt.track_batch(
                *[Event(dt_id=meta.get("#dt_id", None), acid=meta.get("#acid", None), event_name=event_name,
                        properties=props, meta=meta) for _ in range(args.m)]
            )
        else:
            dt.track(dt_id=meta.get("#dt_id", None), acid=meta.get("#acid", None), event_name=event_name,
                     properties=props, meta=meta)

    dt.flush()
    dt.close()


def init_parser(parser):
    parser.add_argument("json", type=str, help=None)
    parser.add_argument("-n", type=int, default=1, help=None)
    parser.add_argument("-m", type=int, help=None)

    parser.set_defaults(op=handle)

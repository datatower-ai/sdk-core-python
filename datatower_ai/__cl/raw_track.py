import json

from datatower_ai import *


def handle(dt: DTAnalytics, args):
    jo = json.loads(args.json)
    props = jo.pop("properties", None)
    meta = jo
    event_name = jo.get("#event_name", "")

    print("Track - {}".format(event_name))
    print("properties: {}".format(props))
    print("meta: {}".format(meta))

    dt.track(dt_id=meta.get("#dt_id", None), acid=meta.get("#acid", None), event_name=event_name,
             properties=props, meta=meta)

    dt.flush()
    dt.close()


def init_parser(parser):
    parser.add_argument("json", type=str, help="")

    parser.set_defaults(op=handle)

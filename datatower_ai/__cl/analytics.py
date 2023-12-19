import json

from datatower_ai import *


def handle(dt, args):
    event_name = args.event_name
    print("Track - {}".format(event_name))

    props = json.loads(args.props) if args.props is not None else None
    meta = json.loads(args.meta) if args.meta is not None else None

    dt.track(dt_id=args.dt_id, acid=args.acid, event_name=event_name, properties=props, meta=meta)

    dt.flush()
    dt.close()


def init_parser(parser):
    parser.add_argument("event_name", type=str, help="")
    parser.add_argument("--dt_id", type=str, help="")
    parser.add_argument("--acid", type=str, help="")
    parser.add_argument("props", type=str, help="")
    parser.add_argument("--meta", type=str, help="")

    parser.set_defaults(op=handle)

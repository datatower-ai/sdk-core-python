import json

from datatower_ai import *


def handle(dt: DTAnalytics, args):
    api = args.api.lower()
    print("User - {}".format(api))

    dt_id = args.dt_id
    acid = args.acid
    props = json.loads(args.props) if args.props is not None else None
    meta = json.loads(args.meta) if args.meta is not None else None

    if api == "set":
        dt.user_set(dt_id, acid, props, meta)
    elif api == "set_once":
        dt.user_set_once(dt_id, acid, props, meta)
    elif api == "unset":
        dt.user_unset(dt_id, acid, props, meta)
    elif api == "add":
        dt.user_add(dt_id, acid, props, meta)
    elif api == "append":
        dt.user_append(dt_id, acid, props, meta)
    elif api == "uniq_append":
        dt.user_uniq_append(dt_id, acid, props, meta)
    else:
        print("Given api is not recognizable")

    dt.flush()
    dt.close()


def init_parser(parser):
    parser.add_argument("api", type=str, help="The api name")
    parser.add_argument("--dt_id", type=str, help="")
    parser.add_argument("--acid", type=str, help="")
    parser.add_argument("props", type=str, help="")
    parser.add_argument("--meta", type=str, help="")

    parser.set_defaults(op=handle)

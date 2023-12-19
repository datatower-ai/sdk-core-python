import json

from datatower_ai import *


def handle(dt, args):
    api = args.api.lower()
    print("Ad - {}".format(api))

    dt_id = args.dt_id
    acid = args.acid
    props = json.loads(args.props) if args.props is not None else None
    meta = json.loads(args.meta) if args.meta is not None else None

    ad_id = args.ad_id
    ad_type = args.ad_type
    platform = args.ad_platform
    mediation = args.ad_mediation
    mediation_id = args.mediation_id
    seq = args.seq
    duration = args.duration
    result = args.result
    error_code = args.error_code
    error_msg = args.error_message
    location = args.location
    entrance = args.entrance
    value = args.value
    currency = args.currency
    country = args.country
    precision = args.precision

    if api == "load_begin":
        dt.ad.report_load_begin(ad_id, ad_type, platform, seq, mediation, mediation_id, dt_id, acid, props, meta)
    elif api == "load_end":
        dt.ad.report_load_end(ad_id, ad_type, platform, duration, result, seq, error_code, error_msg, mediation,
                              mediation_id, dt_id, acid, props, meta)
    elif api == "to_show":
        dt.ad.report_to_show(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id, dt_id, acid,
                             props, meta)
    elif api == "show":
        dt.ad.report_show(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id, dt_id, acid,
                          props, meta)
    elif api == "show_failed":
        dt.ad.report_show_failed(ad_id, ad_type, platform, location, seq, error_code, error_msg, entrance, mediation,
                                 mediation_id, dt_id, acid, props, meta)
    elif api == "close":
        dt.ad.report_close(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id, dt_id, acid,
                           props, meta)
    elif api == "click":
        dt.ad.report_click(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id, dt_id, acid,
                           props, meta)
    elif api == "rewarded":
        dt.ad.report_rewarded(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id, dt_id, acid,
                              props, meta)
    elif api == "conversion_by_click":
        dt.ad.report_conversion_by_click(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id,
                                         dt_id, acid, props, meta)
    elif api == "conversion_by_left_app":
        dt.ad.report_conversion_by_left_app(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id,
                                            dt_id, acid, props, meta)
    elif api == "conversion_by_rewarded":
        dt.ad.report_conversion_by_rewarded(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id,
                                            dt_id, acid, props, meta)
    elif api == "paid":
        dt.ad.report_paid(ad_id, ad_type, platform, seq, value, currency, precision, mediation, mediation_id, dt_id,
                          acid, props, meta)
    elif api == "paid_country":
        dt.ad.report_paid_by_country(ad_id, ad_type, platform, seq, value, precision, country, mediation, mediation_id,
                                     dt_id, acid, props, meta)
    elif api == "left_app":
        dt.ad.report_left_app(ad_id, ad_type, platform, location, seq, entrance, mediation, mediation_id,
                              dt_id, acid, props, meta)
    else:
        print("Given api is not recognizable")

    dt.flush()
    dt.close()


def init_parser(parser):
    parser.add_argument("api", type=str, help="The api name")

    parser.add_argument("--ad_id", type=str, help="")
    parser.add_argument("--ad_type", type=int, help="")
    parser.add_argument("--ad_platform", type=int, help="")
    parser.add_argument("--seq", type=str, help="")
    parser.add_argument("--ad_mediation", type=int, default=-1, help="")
    parser.add_argument("--mediation_id", type=str, default="", help="")
    parser.add_argument("--duration", type=int, help="")
    parser.add_argument("--result", type=int, help="")
    parser.add_argument("--error_code", type=int, default=0, help="")
    parser.add_argument("--error_message", type=str, default="", help="")
    parser.add_argument("--location", type=str, help="")
    parser.add_argument("--entrance", type=str, default="", help="")
    parser.add_argument("--value", type=float, help="")
    parser.add_argument("--currency", type=str, help="")
    parser.add_argument("--country", type=str, help="")
    parser.add_argument("--precision", type=str, help="")

    parser.add_argument("--dt_id", type=str, help="")
    parser.add_argument("--acid", type=str, help="")
    parser.add_argument("--props", type=str, help="")
    parser.add_argument("--meta", type=str, help="")

    parser.set_defaults(op=handle)

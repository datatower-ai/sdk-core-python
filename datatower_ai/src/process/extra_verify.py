import datetime
import re

from datatower_ai.src.util.type_check import is_number, is_str

from datatower_ai import DTMetaDataException, DTIllegalDataException

__META = (("#app_id", str), ("#bundle_id", str), ("#android_id", str), ("#gaid", str), ("#dt_id", str), ("#acid", str),
          ("#event_time", int), ("#event_syn", str))        # No need to include: ("#event_type", str)
__COMPULSORY_META = ("#app_id", "#bundle_id", "#event_time", "#event_name", "#event_type", "#event_syn")
__NAME_REGEX = "^[#$a-zA-Z][a-zA-Z0-9_]{0,63}$"

__PRESET_PROPS_COMMON = (("$uid", str), ("#dt_id", str), ("#acid", str), ("#event_syn", str), ("#session_id", str),
                         ("#device_manufacturer", str), ("#event_name", str), ("#is_foreground", bool),
                         ("#android_id", str), ("#gaid", str), ("#mcc", str), ("#mnc", str), ("#os_country_code", str),
                         ("#os_lang_code", str), ("#event_time", int), ("#bundle_id", str), ("#app_version_code", int),
                         ("#app_version_name", str), ("#sdk_type", str), ("#sdk_version_name", str), ("#os", str),
                         ("#os_version_name", str), ("#os_version_code", int), ("#device_brand", str),
                         ("#device_model", str), ("#build_device", str), ("#screen_height", int),
                         ("#screen_width", int), ("#memory_used", str), ("#storage_used", str), ("#network_type", str),
                         ("#simulator", bool), ("#fps", int), ("$ip", str), ("$country_code", str),
                         ("$server_time", int))
__PRESET_PROPS_AD = (("#ad_seq", str), ("#ad_id", str), ("#ad_type_code", str), ("#ad_platform_code", str),
                     ("#ad_entrance", str), ("#ad_result", bool), ("#ad_duration", int), ("#ad_location", str),
                     ("#errorCode", int), ("#errorMessage", str), ("#ad_value", str), ("#ad_currency", str),
                     ("#ad_precision", str), ("#ad_country_code", str), ("#ad_mediation_code", str),
                     ("#ad_mediation_id", str), ("#ad_conversion_source", str), ("#ad_click_gap", str),
                     ("#ad_return_gap", str), ("#error_code", str), ("#error_message", str), ("#load_result", str),
                     ("#load_duration", str))
__PRESET_PROPS_IAS = (("#ias_seq", str), ("#ias_original_order", str), ("#ias_order", str), ("#ias_sku", str),
                      ("#ias_price", float), ("#ias_currency", str), ("$ias_price_exchange", float))
__PRESET_EVENT = {
    "#app_install": (("#referrer_url", str), ("#referrer_click_time", int), ("#app_install_time", int),
                     ("#instant_experience_launched", bool), ("#failed_reason", str), ("#cnl", str)),
    "#session_start": (("#is_first_time", bool), ("#resume_from_background", bool), ("#start_reason", str)),
    "$app_install": (("$network_id", str), ("$network_name", str), ("$tracker_id", str), ("$tracker_name", str),
                     ("$channel_id", str), ("$channel_sub_id", str), ("$channel_ssub_id", str), ("$channel_name", str),
                     ("$channel_sub_name", str), ("$channel_ssub_name", str), ("$channel_platform_id", int),
                     ("$channel_platform_name", str), ("$attribution_source", str), ("$fraud_network_id", str),
                     ("$original_tracker_id", str), ("$original_tracker_name", str), ("$original_network_id", str),
                     ("$original_network_name", str)),
    "#session_end": (("#session_duration", int),),
    "#ad_load_begin": __PRESET_PROPS_AD,
    "#ad_load_end": __PRESET_PROPS_AD,
    "#ad_to_show": __PRESET_PROPS_AD,
    "#ad_show": __PRESET_PROPS_AD,
    "#ad_show_failed": __PRESET_PROPS_AD,
    "#ad_close": __PRESET_PROPS_AD,
    "#ad_click": __PRESET_PROPS_AD,
    "#ad_left_app": __PRESET_PROPS_AD,
    "#ad_return_app": __PRESET_PROPS_AD,
    "#ad_rewarded": __PRESET_PROPS_AD,
    "#ad_conversion": __PRESET_PROPS_AD + (("$earnings", float),),
    "#ad_paid": __PRESET_PROPS_AD,
    "#iap_purchase_success": (("#iap_order", str), ("#iap_sku", str), ("#iap_price", float), ("#iap_currency", str),
                              ("$iap_price_exchange", float)),
    "#ias_subscribe_success": __PRESET_PROPS_IAS,
    "#ias_subscribe_notify": __PRESET_PROPS_IAS + (("$original_ios_notification_type", str),)
}


def move_meta(source_properties, target, delete=True):
    if source_properties is None:
        return
    for (key, _) in __META:
        if key in source_properties.keys():
            target[key] = source_properties.get(key)
            if delete:
                del (source_properties[key])


def extra_verify(dictionary):
    for prop in __COMPULSORY_META:
        if prop not in dictionary:
            raise DTMetaDataException("Required meta property \"{}\" is missing!".format(prop))
        tp = next((x for x in __META if x[0] == prop), None)
        if tp is not None and not isinstance(dictionary[prop], tp[1]):
            raise DTMetaDataException("Type meta property \"{}\" ({}) is not valid, should be {}!".format(
                prop, type(dictionary[prop]), tp))

    if len(dictionary.get("#app_id", "")) == 0:
        raise DTMetaDataException("app_id cannot missing or be empty!")

    dt_id = dictionary.get("#dt_id", None)
    ac_id = dictionary.get("#acid", None)
    if dt_id is None:
        raise DTMetaDataException("dt_id should be provided but missing!")
    if not isinstance(dt_id, str):
        raise DTMetaDataException("dt_id should be str type!")
    if not dt_id:
        raise DTMetaDataException("dt_id can not be empty!")
    if ac_id is not None and not isinstance(ac_id, str):
        raise DTMetaDataException("acid should be str type!")

    event_name = dictionary["#event_name"]
    if not __full_match(__NAME_REGEX, event_name):
        raise DTMetaDataException("event_name must be a valid variable name.")

    if dictionary["#event_type"] == "track" and (event_name.startswith("#") or event_name.startswith("$")):
        if event_name not in __PRESET_EVENT:
            raise DTMetaDataException("event_name (\"{}\") is out of scope!".format(event_name))
        __verify_preset_properties(event_name, dictionary["properties"])
    else:
        __verify_properties(event_name, dictionary["properties"])


def __verify_preset_properties(event_name, properties):
    if not isinstance(properties, dict):
        raise DTIllegalDataException("Type of \"properties\" of preset event should be Dict!")
    for (key, value) in properties.items():
        tp = __find_prop_in_preset_event(event_name, key)
        if tp is None:
            raise DTIllegalDataException(
                "key of property (\"{}\") is out of scope for preset event (\"{}\")!".format(
                    key, event_name)
            )
        if not isinstance(value, tp[1]):
            raise DTIllegalDataException(
                "The type of value for property \"{}\" is not valid (Given: {}, Expect: {})!".format(
                    key, type(value), tp[1])
            )


def __verify_properties(event_name, properties):
    if event_name == "#user_append" or event_name == "#user_uniq_append":
        __verify_properties_value_4_list(properties, event_name)
    elif event_name == "#user_add":
        __verify_properties_value_4_number(properties, event_name)
    else:
        for key, value in properties.items():
            __verify_properties_key(key)
            __verify_properties_value(
                value,
                "Type of value ({}, {}) is not supported for key ({})".format(type(value), value, key)
            )


def __verify_properties_value_4_list(properties, event_name):
    for value in properties.values():
        if not isinstance(value, list):
            raise DTIllegalDataException("Type of properties for {} should be List".format(event_name))
        __verify_properties_value(
            value,
            "Type of value ({}, {}) is not supported!".format(type(value), value)
        )


def __verify_properties_value_4_number(properties, event_name):
    for value in properties.values():

        if not is_number(value):
            raise DTIllegalDataException(
                "Type of value ({}, {}) is not supported, should be a valid number!".format(type(value), value))


def __verify_properties_value(value, msg):
    if not is_number(value) and not is_str(value) and not isinstance(value, (list, dict, bool, datetime.datetime, datetime.date)):
        raise DTIllegalDataException(msg)


def __verify_properties_key(key):
    if not __full_match(__NAME_REGEX, key):
        raise DTIllegalDataException("")


def __full_match(regex, string, flags=0):
    """Emulate python-3.4 re.fullmatch()."""
    return re.match("(?:" + regex + r")\Z", string, flags=flags)


def __find_prop_in_preset_event(event_name, prop_name):
    props = __PRESET_EVENT.get(event_name, ())
    if len(props) == 0:
        return None
    tp = next((x for x in props if x[0] == prop_name), None)
    if tp is not None:
        return tp
    return next((x for x in __PRESET_PROPS_COMMON if x[0] == prop_name), None)

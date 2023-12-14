import datetime
import re
from typing import Dict, List, Any

from future.types.newint import long

from datatower_ai import DTMetaDataException, DTIllegalDataException

__META = ["#app_id", "#bundle_id", "#android_id", "#gaid", "#dt_id", "#acid", "#event_time", "#event_syn"]
__COMPULSORY_META = ("#bundle_id",)
__NAME_REGEX = re.compile(r"^[#$a-zA-Z][a-zA-Z0-9_]{0,63}$")

__PRESET_PROPS_COMMON = ("$uid", "#dt_id", "#acid", "#event_syn", "#session_id", "#device_manufacturer", "#event_name", "#is_foreground", "#android_id", "#gaid", "#mcc", "#mnc", "#os_country_code", "#os_lang_code", "#event_time", "#bundle_id", "#app_version_code", "#app_version_name", "#sdk_type", "#sdk_version_name", "#os", "#os_version_name", "#os_version_code", "#device_brand", "#device_model", "#build_device", "#screen_height", "#screen_width", "#memory_used", "#storage_used", "#network_type", "#simulator", "#fps", "$ip", "$country_code", "$server_time")
__PRESET_PROPS_AD = ("#ad_seq", "#ad_id", "#ad_type_code", "#ad_platform_code", "#ad_entrance", "#ad_result", "#ad_duration", "#ad_location", "#errorCode", "#errorMessage", "#ad_value", "#ad_currency", "#ad_precision", "#ad_country_code", "#ad_mediation_code", "#ad_mediation_id")
__PRESET_EVENT = {
    "#app_install": ("#referrer_url", "#referrer_click_time", "#app_install_time", "#instant_experience_launched", "#failed_reason", "#cnl"),
    "#session_start": ("#is_first_time", "#resume_from_background", "#start_reason"),
    "$app_install": ("$network_id", "$network_name", "$tracker_id", "$tracker_name", "$channel_id", "$channel_sub_id", "$channel_ssub_id", "$channel_name", "$channel_sub_name", "$channel_ssub_name", "$channel_platform_id", "$channel_platform_name", "$attribution_source", "$fraud_network_id", "$original_tracker_id", "$original_tracker_name", "$original_network_id", "$original_network_name"),
    "#session_end": ("#session_duration",),
    "#ad_show": __PRESET_PROPS_AD,
    "#ad_conversion": __PRESET_PROPS_AD,
    "#iap_purchase_success": ("#iap_order", "#iap_sku", "#iap_price", "#iap_currency", "$iap_price_exchange"),
    "#ias_subscribe_success": ("#ias_original_order", "#ias_order", "#ias_sku", "#ias_price", "#ias_currency", "$ias_price_exchange")
}


def move_meta(source_properties, target, delete: bool = True):
    if source_properties is None:
        return
    for key in __META:
        if key in source_properties.keys():
            target[key] = source_properties.get(key)
            if delete:
                del (source_properties[key])


def extra_verify(dictionary: Dict[str, Any]):
    for prop in __COMPULSORY_META:
        if prop not in dictionary:
            raise DTMetaDataException("Required meta property \"{}\" is missing!".format(prop))

    if len(dictionary.get("#app_id", "")) == 0:
        raise DTMetaDataException("app_id cannot missing or be empty!")

    dt_id = dictionary.get("#dt_id", None)
    ac_id = dictionary.get("#acid", None)
    if dt_id is None and ac_id is None:
        raise DTMetaDataException("At least one of dt_id or ac_id should be provided!")
    if dt_id is not None and not isinstance(dt_id, str):
        raise DTMetaDataException("dt_id should be str!")
    if ac_id is not None and not isinstance(ac_id, str):
        raise DTMetaDataException("acid should be str!")

    event_name = dictionary["#event_name"]
    if not __NAME_REGEX.match(event_name):
        raise DTMetaDataException("event_name must be a valid variable name.")

    if dictionary["#event_type"] == "track" and (event_name.startswith("#") or event_name.startswith("$")):
        if event_name not in __PRESET_EVENT:
            raise DTMetaDataException("event_name (\"{}\") is out of scope!".format(event_name))
        __verify_preset_properties(event_name, dictionary["properties"])
    else:
        __verify_properties(event_name, dictionary["properties"])


def __verify_preset_properties(event_name: str, properties):
    if not isinstance(properties, Dict):
        raise DTIllegalDataException("Type of \"properties\" of preset event should be Dict!")
    for (key, value) in properties.items():
        if key not in __PRESET_EVENT[event_name] and key not in __PRESET_PROPS_COMMON:
            raise DTIllegalDataException(
                "key of property (\"{}\") is not valid and out of scope for preset event (\"{}\")!".format(key, event_name)
            )


def __verify_properties(event_name: str, properties):
    if event_name == "#user_append" or event_name == "#user_uniq_append":
        __verify_properties_4_list(properties, event_name)
    elif event_name == "#user_add":
        __verify_properties_4_number_list(properties, event_name)
    else:
        for key, value in properties.items():
            __verify_properties_key(key)
            __verify_properties_value(
                value,
                "Type of value ({}, {}) is not supported for key ({})".format(type(value), value, key)
            )


def __verify_properties_4_list(properties, event_name):
    if not isinstance(properties, List):
        raise DTIllegalDataException("Type of properties for {} should be List".format(event_name))
    for value in properties:
        __verify_properties_value(
            value,
            "Type of value ({}, {}) is not supported!".format(type(value), value)
        )


def __verify_properties_4_number_list(properties, event_name):
    if not isinstance(properties, List):
        raise DTIllegalDataException("Type of properties for {} should be List".format(event_name))
    for value in properties:
        if not isinstance(value, (int, float, long)):
            raise DTIllegalDataException(
                "Type of value ({}, {}) is not supported, should be a valid number!".format(type(value), value))


def __verify_properties_value(value, msg):
    if not isinstance(value, (int, float, long, str, List, Dict, bool, datetime.datetime, datetime.date)):
        raise DTIllegalDataException(msg)


def __verify_properties_key(key):
    if not __NAME_REGEX.fullmatch(key):
        raise DTIllegalDataException("")

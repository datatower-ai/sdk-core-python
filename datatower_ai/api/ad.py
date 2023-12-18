from enum import Enum
from typing import Union, List, Dict

from datatower_ai.api.base import _DTApi
from datatower_ai.src.consumer.abstract_consumer import _AbstractConsumer


class AdType(Enum):
    IDLE = -1
    BANNER = 0
    INTERSTITIAL = 1
    NATIVE = 2
    REWARDED = 3
    REWARDED_INTERSTITIAL = 4
    APP_OPEN = 5
    MREC = 6


class AdPlatform(Enum):
    UNDISCLOSED = -2
    IDLE = -1
    ADMOB = 0
    MOPUB = 1
    ADCOLONY = 2
    APPLOVIN = 3
    CHARTBOOST = 4
    FACEBOOK = 5
    INMOBI = 6
    IRONSOURCE = 7
    PANGLE = 8
    SNAP_AUDIENCE_NETWORK = 9
    TAPJOY = 10
    UNITY_ADS = 11
    VERIZON_MEDIA = 12
    VUNGLE = 13
    ADX = 14
    COMBO = 15
    BIGO = 16
    HISAVANA = 17
    APPLOVIN_EXCHANGE = 18
    MINTEGRAL = 19
    LIFTOFF = 20
    A4G = 21
    GOOGLE_AD_MANAGER = 22
    FYBER = 23
    MAIO = 24
    CRITEO = 25
    MYTARGET = 26
    OGURY = 27
    APPNEXT = 28
    KIDOZ = 29
    SMAATO = 30
    START_IO = 31
    VERVE = 32
    LOVINJOYADS = 33
    YANDEX = 34
    REKLAMUP = 35


class AdMediation(Enum):
    IDLE = -1
    MOPUB = 0
    MAX = 1
    HISAVANA = 2
    COMBO = 3
    TOPON = 4
    TRADPLUS = 5
    TOBID = 6
    ADMOB = 7


def _get_value(raw: Union[AdType, AdPlatform, AdMediation, int]) -> int:
    if raw is int:
        return raw
    if isinstance(raw, (AdType, AdPlatform, AdMediation)):
        try:
            return raw.value
        except:
            # Compatible to py 2.7 which has no .value property.
            return raw


class DTAdReport(_DTApi):
    def __init__(self, consumer: _AbstractConsumer, debug: bool):
        super(DTAdReport, self).__init__(consumer, debug)

    def report_load_begin(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            seq: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report Ad begins to load.

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param seq: Sequence identifier of a serial of events.
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id)
        self.__add(dt_id, acid, "track", "#ad_load_begin", props, meta)

    def report_load_end(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            duration: int,
            result: bool,
            seq: str,
            error_code: int = 0,
            error_message: str = "",
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report Ad is finish loading.

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param duration: Duration of Ad loading.
        :param result: The Result of Ad loads, True for successful and vice versa.
        :param seq: Sequence identifier of a serial of events.
        :param error_code: Error code if error.
        :param error_message: Error message if error.
        :param mediation: Mediation platform.
        :param mediation_id: The id from Mediation platform.
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id, others={
            "#load_duration": duration,
            "#load_result": result,
            "#error_code": error_code,
            "#error_message": error_message
        })
        self.__add(dt_id, acid, "track", "#ad_load_end", props, meta)

    def report_to_show(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report requesting to show Ad

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance)
        self.__add(dt_id, acid, "track", "#ad_to_show", props, meta)

    def report_show(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report requesting to show Ad

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance)
        self.__add(dt_id, acid, "track", "#ad_show", props, meta)

    def report_show_failed(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            error_code: int,
            error_message: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report Ad is failed to show

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param error_code: Error code.
        :param error_message: Error Message.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,location,
                                         entrance, {"#error_code": error_code, "#error_message": error_message})
        self.__add(dt_id, acid, "track", "#ad_show_failed", props, meta)

    def report_close(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report Ad is closed

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance)
        self.__add(dt_id, acid, "track", "#ad_close", props, meta)

    def report_click(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report Ad is clicked

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance)
        self.__add(dt_id, acid, "track", "#ad_click", props, meta)

    def report_rewarded(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report rewarded from Ad

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance)
        self.__add(dt_id, acid, "track", "#ad_rewarded", props, meta)

    def report_conversion_by_click(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report conversion by clicking

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance, {"#ad_conversion_source": "by_click"})
        self.__add(dt_id, acid, "track", "#ad_conversion", props, meta)

    def report_conversion_by_left_app(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report conversion by leaving app

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance, {"#ad_conversion_source": "by_left_app"})
        self.__add(dt_id, acid, "track", "#ad_conversion", props, meta)

    def report_conversion_by_rewarded(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report conversion by rewarded

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance, {"#ad_conversion_source": "by_rewarded"})
        self.__add(dt_id, acid, "track", "#ad_conversion", props, meta)

    def report_paid(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            seq: str,
            value: float,
            currency: str,
            precision: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report the value from displaying Ad

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param seq: Sequence identifier of a serial of events.
        :param value: The value of this Ad
        :param currency: Currency.
        :param precision: Precision of the value
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id, others={
            "#ad_value": value, "#ad_currency": currency, "#ad_precision": precision
        })
        self.__add(dt_id, acid, "track", "#ad_paid", props, meta)

    def report_paid_by_country(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            seq: str,
            value: float,
            precision: str,
            country: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report the value from displaying Ad

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param seq: Sequence identifier of a serial of events.
        :param value: The value of this Ad.
        :param precision: Precision of the value.
        :param country: Country.
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id, others={
            "#ad_value": value, "#ad_precision": precision, "#ad_country_code": country
        })
        self.__add(dt_id, acid, "track", "#ad_paid", props, meta)

    def report_left_app(
            self,
            ad_id: str,
            ad_type: Union[AdType, int],
            platform: Union[AdPlatform, int],
            location: str,
            seq: str,
            entrance: str,
            mediation: Union[AdMediation, int] = AdMediation.IDLE,
            mediation_id: str = "",
            dt_id: str = None,
            acid: str = None,
            properties: Dict = None,
            meta: Dict = None
    ):
        """ Report leaving the app

        :param dt_id: A unique id per user per device, either dt_it or acid should be provided.
        :param acid: A unique id per user per device, either dt_it or acid should be provided.
        :param ad_id: minimum unit id of this Ad.
        :param ad_type: Type of the Ad.
        :param platform: Ad platform.
        :param location: The location Ad set.
        :param seq: Sequence identifier of a serial of events.
        :param entrance: Entrance of Ad
        :param mediation: Mediation platform
        :param mediation_id: The id from Mediation platform
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        props = self.__update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                                         location, entrance)
        self.__add(dt_id, acid, "track", "#ad_left_app", props, meta)

    @staticmethod
    def __update_properties(properties, ad_id, ad_type, platform, seq, mediation, mediation_id,
                            location="", entrance="", others=None) -> Dict:
        props = properties if properties is not None else {}
        props.update({
            "#ad_id": ad_id,
            "#ad_type_code": _get_value(ad_type),
            "#ad_platform_code": _get_value(platform),
            "#ad_seq": seq,
            "#ad_mediation_code": mediation,
            "#ad_mediation_id": mediation_id,
            "#ad_location": location,
            "#ad_entrance": entrance,
        })

        if others is Dict:
            props.update(others)
        return props

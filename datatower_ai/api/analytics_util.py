from datatower_ai.api.base import _DTApi
from datatower_ai.src.util.performance.time_monitor import TimeMonitor


class DTAnalyticsUtils(_DTApi):
    def __init__(self, consumer, debug):
        super(DTAnalyticsUtils, self).__init__(consumer, debug)
        self.__timers = {}

    def track_timer_start(self, event_name, dt_id = None, acid = None):
        """ Initialize and start a timer for the event (associate with IDs), unit: millisecond.

        :param event_name: Name of the event.
        :param dt_id: A unique id per user per device.
        :param acid: A unique id per user per device.
        """
        key = self.__get_key(event_name, dt_id, acid)
        self.__timers[key] = TimeMonitor().start(key)

    def track_timer_pause(self, event_name, dt_id = None, acid = None):
        """ Pause the timer with given event name (associate with IDs).

        :param event_name: Name of the event.
        :param dt_id: A unique id per user per device.
        :param acid: A unique id per user per device.
        """
        key = self.__get_key(event_name, dt_id, acid)
        timer = self.__timers.get(key, None)
        if timer is None:
            return
        timer.pause()

    def track_timer_resume(self, event_name, dt_id = None, acid = None):
        """ Resume the timer with given event name (associate with IDs)

        :param event_name: Name of the event.
        :param dt_id: A unique id per user per device.
        :param acid: A unique id per user per device.
        """
        key = self.__get_key(event_name, dt_id, acid)
        timer = self.__timers.get(key, None)
        if timer is None:
            return
        timer.resume()

    def track_timer_stop(self, event_name, dt_id = None, acid = None,
                         properties = None, meta = None):
        """ Stop the timer with event name (associate with IDs) and report the duration in ms together with
        properties and meta.

        :param event_name: Name of the event.
        :param dt_id: A unique id per user per device.
        :param acid: A unique id per user per device.
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        key = self.__get_key(event_name, dt_id, acid)
        timer = self.__timers.get(key, None)
        if timer is not None:
            duration = timer.stop(one_shot=True)
            del self.__timers[key]
            if duration >= 0:
                if properties is None:
                    properties = {}
                properties["#event_duration"] = int(duration)
        self._add(dt_id, acid, "track", event_name, properties, meta)

    @staticmethod
    def __get_key(event_name, dt_id = None, acid = None):
        return "en={}&dt_id={}&acid={}".format(event_name, dt_id, acid)

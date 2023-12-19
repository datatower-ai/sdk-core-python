from datatower_ai.src.util.exception import DTMetaDataException


class Event:
    def __init__(self, dt_id=None, acid=None, event_name=None, properties=None, meta=None):
        """Event data

        :param dt_id: A unique identifier per user per device.
        :param acid: A unique identifier per user per device.
        :param event_name: The name of this event.
        :param properties: The properties inside "properties" field, with allowing of meta properties set together. But
        meta properties will be extracted and rest will be placed inside "properties" field.
        :param meta: Meta properties of this event. Only preset meta properties will be used otherwise discard. This
        argument can be ignored if metas is already putted inside properties.
        """
        self.__dt_id = dt_id
        self.__acid = acid
        self.__event_name = event_name
        self.__properties = properties
        self.__meta = meta

        if self.__dt_id is None and self.__acid is None:
            raise DTMetaDataException("At least one of dt_id and acid should be provided!")

    @property
    def dt_id(self):
        return self.__dt_id

    @property
    def acid(self):
        return self.__acid

    @property
    def event_name(self):
        return self.__event_name

    @property
    def properties(self):
        return self.__properties

    @property
    def meta(self):
        return self.__meta

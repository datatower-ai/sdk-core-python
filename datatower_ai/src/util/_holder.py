from datatower_ai.src.util.singleton import Singleton


class _Holder(Singleton):
    def __init__(self):
        self.show_statistics = False
        self.__debug = None

    @property
    def debug(self):
        return self.__debug if self.__debug is not None else False

    @debug.setter
    def debug(self, value):
        if self.__debug is None and type(value) is bool:
            self.__debug = value        # once

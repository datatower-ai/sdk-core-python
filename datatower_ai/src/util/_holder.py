from datatower_ai.src.util.singleton import Singleton


class _Holder(Singleton):
    def __init__(self):
        self.show_statistics = False

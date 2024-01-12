# -*- coding: utf-8 -*-
class _AbstractConsumer(object):
    """
        Consumer抽象类
    """
    def __init__(self):
        self.__pagers = set()

    def get_app_id(self):
        raise NotImplementedError

    def add(self, get_msg):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def register_pager(self, pager):
        """
        Register a pager to listen errors or warnings from SDK.

        :param pager: A function with signature: (code: int, message: str) -> None.
        """
        self.__pagers.add(pager)

    def unregister_pager(self, pager):
        """
        Unregister this pager.
        """
        self.__pagers.remove(pager)

    def _page_message(self, code, message):
        for pager in self.__pagers:
            try:
                pager(code, message)
            except:
                from datatower_ai.src.util.logger import Logger
                Logger.exception("Failed to page to a pager")

# -*- coding: utf-8 -*-
class _AbstractConsumer(object):
    """
        Consumer抽象类
    """
    def get_app_id(self):
        raise NotImplementedError

    def add(self, get_msg):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
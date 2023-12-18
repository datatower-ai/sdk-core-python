from typing import Callable, List


class _AbstractConsumer(object):
    """
        Consumer抽象类
    """
    def get_app_id(self):
        raise NotImplementedError

    def add(self, get_msg: Callable[[], List[str]]):
        raise NotImplementedError

    def flush(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
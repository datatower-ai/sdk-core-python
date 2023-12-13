""" Usages:
CounterMonitor["xxx"] = value
value = CounterMonitor["xxx"]

CounterMonitor["xxx"] + value
CounterMonitor["xxx"] - value
CounterMonitor["xxx"] * value
CounterMonitor["xxx"] / value
CounterMonitor["xxx"] // value
CounterMonitor["xxx"] % value

CounterMonitor["xxx"] += value
CounterMonitor["xxx"] -= value
CounterMonitor["xxx"] *= value
CounterMonitor["xxx"] /= value
CounterMonitor["xxx"] //= value
CounterMonitor["xxx"] %= value
"""

import sys
from typing import Dict, Union, Callable, Any

from datatower_ai.src.util.thread.swmr_lock import SingleWriteMultiReadLock


class CmCounter:
    def __init__(self, key):
        self.__key = key

    @property
    def value(self):
        return CounterMonitor._get_value(self.__key)

    def __iadd__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x + o))
        return self

    def __isub__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x - o))
        return self

    def __imul__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x * o))
        return self

    def __idiv__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x / o))
        return self

    def __ifloordiv__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x // o))
        return self

    def __itruediv__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x / o))
        return self

    def __imod__(self, other):
        self._op(other, lambda o: CounterMonitor._op(self.__key, lambda x: x % o))
        return self

    def __lt__(self, other):
        return self._op(other, lambda x: self.value < x)

    def __gt__(self, other):
        return self._op(other, lambda x: self.value > x)

    def __le__(self, other):
        return self < other or self == other

    def __ge__(self, other):
        return self > other or self == other

    def __eq__(self, other):
        return self._op(other, lambda x: self.value == x)

    def __ne__(self, other):
        return not self == other

    def __add__(self, other):
        return self._op(other, lambda x: self.value + x)

    def __sub__(self, other):
        return self._op(other, lambda x: self.value - x)

    def __mul__(self, other):
        return self._op(other, lambda x: self.value * x)

    def __div__(self, other):
        return self._op(other, lambda x: self.value / x)

    def __floordiv__(self, other):
        return self._op(other, lambda x: self.value // x)

    def __truediv__(self, other):
        return self._op(other, lambda x: self.value / x)

    def __mod__(self, other):
        return self._op(other, lambda x: self.value % x)

    def __str__(self):
        return "{}".format(self.value)

    def __repr__(self):
        return "{}".format(self.value)

    @staticmethod
    def _op(other, op: Callable[[Union[int, float]], Any]):
        if type(other) is int or type(other) is float:
            return op(other)
        elif isinstance(other, CmCounter):
            return op(other.value)


class CounterMonitorMeta(type):
    __table: Dict[str, Union[int, float]] = {}     # {"key": (avg, count, acc)}
    __locker = SingleWriteMultiReadLock()
    
    @staticmethod
    def _op(key, op: Callable[[Union[int, float]], Union[int, float]]):
        CounterMonitor.__locker.acquire_write()
        ov = CounterMonitor.__table.get(key, 0)
        CounterMonitor.__table[key] = op(ov)
        CounterMonitor.__locker.release_write()

    @staticmethod
    def _get_value(key):
        CounterMonitor.__locker.acquire_read()
        value = CounterMonitor.__table.get(key, 0)
        CounterMonitor.__locker.release_read()
        return value

    def __setitem__(cls, key, value: Union[int, float, CmCounter]):
        v = value if not isinstance(value, CmCounter) else value.value
        CounterMonitor.__locker.acquire_write()
        CounterMonitor.__table[key] = v
        CounterMonitor.__locker.release_write()

    def __getitem__(cls, item):
        return CmCounter(item)


if sys.version_info[0] >= 3:
    class CounterMonitor(object, metaclass=CounterMonitorMeta):
        pass
else:
    class CounterMonitor(object):
        __metaclass__ = CounterMonitorMeta
        pass


if __name__ == "__main__":
    CounterMonitor["test"] = 20
    print("set: {}".format(CounterMonitor["test"]))

    CounterMonitor["test"] += 1
    print("add 1: {}".format(CounterMonitor["test"]))

    CounterMonitor["test"] -= 2
    print("minus 2: {}".format(CounterMonitor["test"]))

    CounterMonitor["test"] *= 2
    print("multiply 2: {}".format(CounterMonitor["test"]))

    CounterMonitor["test"] /= 2
    print("divide by 2: {}".format(CounterMonitor["test"]))

    CounterMonitor["test"] %= 100
    print("mod by 100: {}".format(CounterMonitor["test"]))

    print("> 10? {}".format(CounterMonitor["test"] > 10))
    print(">= 10? {}".format(CounterMonitor["test"] >= 10))
    print("< 10? {}".format(CounterMonitor["test"] < 10))
    print("<= 10? {}".format(CounterMonitor["test"] <= 10))
    print("== 10? {}".format(CounterMonitor["test"] == 10))
    print("!= 10? {}".format(CounterMonitor["test"] != 10))
    print("== 19? {}".format(CounterMonitor["test"] == 19))
    print("> 19? {}".format(CounterMonitor["test"] > 19))
    print(">= 19? {}".format(CounterMonitor["test"] >= 19))

    print("test: {}".format(CounterMonitor["test"]))
    print("test - 3: {}".format(CounterMonitor["test"] - 3))
    print("test + 10: {}".format(CounterMonitor["test"] + 10))
    print("test * 2: {}".format(CounterMonitor["test"] * 2))
    print("test / 3: {}".format(CounterMonitor["test"] / 3))
    print("test % 4: {}".format(CounterMonitor["test"] % 4))
    print("test: {}".format(CounterMonitor["test"]))

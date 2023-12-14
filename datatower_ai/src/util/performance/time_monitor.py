import logging
import time
from threading import Semaphore
from typing import Dict, Tuple, Callable, Optional

from datatower_ai.src.util.logger import Logger

from datatower_ai.src.util.singleton import Singleton


class TmTimer:
    def __init__(self,
                 key: Optional[str] = None,
                 record_func: Optional[Callable[[str, float], None]] = None
                 ):
        """This timer will automatically be started when TimeMonitor().start called"""
        self.__key = key
        self.__total_time = 0
        self.__start_time = time.time()
        self.__status = 0     # 0: started, 1: paused, 2: stopped
        self.__record_func = record_func

    def pause(self):
        """Resume the timer, worked only if it is started and not paused"""
        if self.__status == 0:
            self.__total_time += time.time() - self.__start_time
            self.__status = 1
        else:
            Logger.warning("[TimeMonitor] Timer pause is called (\"%s\") but the timer is not started (%d)!" % (
                self.__key, self.__status
            ))

    def resume(self):
        """Resume the timer, worked only if it's paused"""
        if self.__status == 1:
            self.__start_time = time.time()
            self.__status = 0
        else:
            Logger.warning("[TimeMonitor] Timer resume is called (\"%s\") but the timer is not paused (%d)!" % (self.__key, self.__status))

    def stop(self, one_shot: bool = True) -> float:
        """Get current time used from start to stop except pausing gap in milliseconds, -1 if such index not start yet.

        :param one_shot: Only use once. If True, will not be saved and not track further state (e.g. sum, avg).
        """
        if self.__status == 2:
            Logger.warning("[TimeMonitor] Timer stop is called (\"%s\") but the timer is stopped already!" % self.__key)
            return -1

        if self.__status == 0:
            self.__total_time += time.time() - self.__start_time

        self.__status = 2
        self.__start_time = 0
        if not one_shot and self.__record_func is not None and self.__key is not None:
            self.__record_func(self.__key, self.__total_time)
        return self.__total_time * 1000

    def peek(self) -> float:
        """Peek the current time elapsed in milliseconds.
        """
        if self.__status == 0:
            return (self.__total_time + time.time() - self.__start_time) * 1000
        else:
            return self.__total_time * 1000


class TimeMonitor(Singleton):
    """This monitor is used for tracking time performance in indices
    ```
    monitor = TimeMonitor()
    timer = monitor.start("example")
    timer.pause("example")
    timer.resume("example")
    elapsed = monitor.stop("example")

    sum = monitor.get_sum("example")
    average = monitor.get_avg("example")
    count = monitor.get_count("example")
    monitor.delete("example")
    ```
    """
    __sem = Semaphore()

    def __init__(self):
        self.__table = {}     # {"key": (avg, count)}

    def start(self, key: str) -> TmTimer:
        return TmTimer(key, self._record)

    def _record(self, key: str, elapsed: float):
        is_get = TimeMonitor.__sem.acquire(timeout=0.01)
        if not is_get:
            return
        tp = self.__table.get(key, (0, 0))
        new_tp = ((tp[0] * tp[1] + elapsed) / (tp[1] + 1), tp[1] + 1)
        self.__table[key] = new_tp
        TimeMonitor.__sem.release()

    def get_sum(self, key: str) -> float:
        """Sum of time performance of such index in milliseconds,
        -1 if such index is not counted yet
        """
        if key not in self.__table:
            return -1
        return self.__table[key][0] * self.__table[key][1] * 1000

    def get_avg(self, key: str) -> float:
        """Average of time performance of such index in milliseconds,
        -1 if such index is not counted yet
        """
        if key not in self.__table:
            return -1
        return self.__table[key][0] * 1000

    def get_count(self, key: str) -> int:
        if key not in self.__table:
            return -1
        return self.__table[key][1]

    def delete(self, key: str) -> Tuple:
        if key not in self.__table:
            return ()
        tp = self.__table[key]
        del self.__table[key]
        return tp

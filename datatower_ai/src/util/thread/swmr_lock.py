import fcntl
import threading

from datatower_ai.src.util.logger import Logger


class SingleWriteMultiReadLock:
    """The lock that allows multiple reads or single write run concurrently

    Source: https://www.oreilly.com/library/view/python-cookbook/0596001673/ch06s04.html
    """
    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self):
        """Acquire for read, will add a read which will block acquire_write(), will not block further op"""
        self._acquire()
        try:
            self._readers += 1
        finally:
            self._release()

    def release_read(self):
        """Release for read, remove a read"""
        self._acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._notify_all()
        finally:
            self._release()

    def acquire_write(self):
        """Acquire for write, block until all read finished.
        This will also block further read/write until release_write()"""
        self._acquire()
        while self._readers > 0:
            self._wait()

    def release_write(self):
        """Release for write"""
        self._release()

    def __del__(self):
        try:
            self._release()
        except:
            pass

    def _acquire(self):
        self._read_ready.acquire()

    def _release(self):
        self._read_ready.release()

    def _wait(self):
        self._read_ready.wait()

    def _notify_all(self):
        self._read_ready.notify_all()

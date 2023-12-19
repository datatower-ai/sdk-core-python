import fcntl
import os
from threading import Semaphore

from datatower_ai.src.util.exception import DTException

if os.name == 'nt':
    import msvcrt


    def _lock(file_):
        try:
            save_pos = file_.tell()
            file_.seek(0)
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_LOCK, 1)
            except IOError as e:
                raise DTException(e)
            finally:
                if save_pos:
                    file_.seek(save_pos)
        except IOError as e:
            raise DTException(e)


    def _unlock(file_):
        try:
            save_pos = file_.tell()
            if save_pos:
                file_.seek(0)
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_UNLCK, 1)
            except IOError as e:
                raise DTException(e)
            finally:
                if save_pos:
                    file_.seek(save_pos)
        except IOError as e:
            raise DTException(e)
elif os.name == 'posix':
    import fcntl


    def _lock(file_):
        try:
            fcntl.flock(file_.fileno(), fcntl.LOCK_EX)
        except IOError as e:
            raise DTException(e)


    def _unlock(file_):
        fcntl.flock(file_.fileno(), fcntl.LOCK_UN)
else:
    raise DTException("Python SDK is defined for NT and POSIX system.")


class FileLock(object):
    def __init__(self, file_name):
        self.__fd = open(file_name, "w")
        self.__semaphore = Semaphore()
        self.__count = 0

    def __enter__(self):
        self.__lock()
        return self

    def __exit__(self, t, v, tb):
        self.__unlock()

    def acquire(self):
        self.__lock()

    def release(self):
        self.__unlock()

    def __lock(self):
        self.__semaphore.acquire()
        _lock(self.__fd)
        self.__count += 1           # This lock can reentrant in same process, therefore add a counter.
        self.__semaphore.release()

    def __unlock(self):
        self.__semaphore.acquire()
        self.__count = max(0, self.__count - 1)
        if self.__count <= 0:
            _unlock(self.__fd)
        self.__semaphore.release()

import fcntl
import os

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
    def __init__(self, file_name: str):
        self.__fd = open(file_name, "w")

    def __enter__(self):
        _lock(self.__fd)
        return self

    def __exit__(self, t, v, tb):
        _unlock(self.__fd)

    def acquire(self):
        _lock(self.__fd)

    def release(self):
        _unlock(self.__fd)

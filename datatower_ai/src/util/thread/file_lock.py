import fcntl


class FileLock:
    def __init__(self, file_name: str):
        super().__init__()
        self.__fd = open(file_name, "w")

    def acquire(self):
        fcntl.flock(self.__fd.fileno(), fcntl.LOCK_EX)

    def release(self):
        fcntl.flock(self.__fd.fileno(), fcntl.LOCK_UN)

    def __del__(self):
        self.__fd.close()

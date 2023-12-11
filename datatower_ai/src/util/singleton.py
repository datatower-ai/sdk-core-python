import threading

_sem = threading.Semaphore()


class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called.

    - Thread Safe

    Modified from: https://stackoverflow.com/a/6798042/8424572
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            _sem.acquire()
            if cls not in cls._instances:
                cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
            _sem.release()
        return cls._instances[cls]


class Singleton(_Singleton('SingletonMeta', (object,), {})):
    pass


if __name__ == "__main__":
    class A(Singleton):
        pass

    class B(Singleton):
        pass

    print(A() == A())
    print(A() == B())

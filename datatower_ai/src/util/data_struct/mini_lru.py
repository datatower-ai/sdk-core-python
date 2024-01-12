import time
from heapq import heapify, heappush, heappop, heappushpop

from datatower_ai.src.util.singleton import Singleton


class _NoSuchItem(Singleton):
    pass


class MiniLru(object):
    """
    Minimum impl of Lru algorithm, which is timestamp-based and by min-heap.
    """
    def __init__(self, size, on_drop=lambda key, obj: None):
        self.__size = size
        self.__items = dict()
        self.__on_drop = on_drop
        self.__heap = []
        heapify(self.__heap)

    def put(self, key, obj):
        for _ in range(len(self.__heap) - self.__size + 1):
            item = heappop(self.__heap)
            self.__on_drop(item.key, item.obj)
            del self.__items[item.key]
        new_item = _MiniLruItem(key, obj, time.time())
        heappush(self.__heap, new_item)
        self.__items[key] = new_item

    def update(self, key, new_obj):
        if key in self.__items:
            self.__items[key].obj = new_obj
            self.__items[key].timestamp = time.time()
            heapify(self.__heap)
            return True
        return False

    def update_or_put(self, key, obj):
        if not self.update(key, obj):
            self.put(key, obj)

    def get(self, key, default=None):
        if key in self.__items:
            item = self.__items[key]
            item.timestamp = time.time()
            heapify(self.__heap)
            return item.obj
        else:
            return default

    def remove(self, key):
        if key in self.__items:
            item = self.__items[key]
            self.__on_drop(item.key, item.obj)
            del self.__items[key]
            self.__heap.remove(item)
            heapify(self.__heap)

    def get_or_put(self, key, put_func):
        obj = self.get(key, _NoSuchItem())
        if type(obj) is _NoSuchItem:
            obj = put_func()
            self.put(key, obj)
        return obj

    def __setitem__(self, key, value):
        self.update_or_put(key, value)

    def __getitem__(self, key):
        return self.get(key, None)

    def __delitem__(self, key):
        self.remove(key)

    def __str__(self):
        return str(map(str, self.__heap))


class _MiniLruItem(object):
    def __init__(self, key, obj, timestamp):
        self.__key = key
        self.__obj = obj
        self.__timestamp = timestamp

    @property
    def key(self):
        return self.__key

    @property
    def obj(self):
        return self.__obj

    @obj.setter
    def obj(self, value):
        self.__obj = value

    @property
    def timestamp(self):
        return self.__timestamp

    @timestamp.setter
    def timestamp(self, value):
        self.__timestamp = value

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __le__(self, other):
        return self.timestamp <= other.timestamp

    def __str__(self):
        return "({}, {})".format(self.key, self.obj)

    def __repr__(self):
        return "MiniLruItem({}, {})".format(self.key, self.obj)


if __name__ == '__main__':
    lru = MiniLru(5)
    lru.put("a", 1)
    print(lru)
    lru.put("b", 2)
    print(lru)
    lru.put("c", 3)
    print(lru)
    lru.put("d", 4)
    print(lru)
    lru.put("e", 5)
    print(lru)
    lru.put("f", 6)
    print(lru)
    lru.put("g", 7)
    print(lru)
    print("Get c: {}".format(lru.get("c")))
    lru.put("h", 8)
    print(lru)

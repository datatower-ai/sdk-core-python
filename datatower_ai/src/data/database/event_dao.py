import time

from datatower_ai.src.strategy.exceed_insertion_strategy import ExceedInsertionStrategy
from datatower_ai.src.util.performance.counter_monitor import _CounterMonitor
from datatower_ai.src.util.performance.time_monitor import TimeMonitor

_table_name = "events"


class DTEventEntity:
    def __init__(self, data, app_id, server_url, token, created_at = time.time(), identifier = -1):
        self.data = data
        self.app_id = app_id
        self.server_url = server_url
        self.token = token
        self.created_at = created_at
        self.identifier = identifier

    @staticmethod
    def create_statement():
        return """
        CREATE TABLE IF NOT EXISTS %s
        (
            _id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            app_id TEXT,
            server_url TEXT,
            token TEXT,
            created_at INTEGER,
            queried INTEGER DEFAULT 0
        );
        """ % _table_name

    @staticmethod
    def query_all_unqueried_statement():
        return """
            SELECT _id, data, app_id, server_url, token, created_at FROM %s WHERE queried = 0;
        """ % _table_name

    @staticmethod
    def query_batch_unquired_statement(limit, offset):
        return """
            SELECT _id, data, app_id, server_url, token, created_at FROM %s 
            WHERE queried = 0 
            LIMIT %d OFFSET %d;
        """ % (_table_name, limit, offset)

    @staticmethod
    def delete_all_statement():
        return """
            DELETE FROM %s;
        """ % _table_name

    @staticmethod
    def delete_by_id_statement():
        return """
            DELETE FROM %s WHERE _id = ?;
        """ % _table_name

    @staticmethod
    def set_queried_statement(queried):
        return """
            UPDATE %s 
            SET queried = %d
            WHERE _id = ?;
        """ % (_table_name, 1 if queried else 0)

    @staticmethod
    def get_unqueried_size_statement():
        return """
            SELECT COUNT(*) FROM %s WHERE queried = 0;
        """ % _table_name

    def insert_statement(self):
        return """
        INSERT INTO %s 
        (data, app_id, server_url, token, created_at) 
        VALUES ('%s', '%s', '%s', '%s', %d);
        """ % (_table_name, self.data, self.app_id, self.server_url, self.token, self.created_at)

    @staticmethod
    def delete_oldest_statement(num = 1):
        return """
        DELETE FROM %s WHERE _id IN (
            SELECT _id FROM %s ORDER BY created_at LIMIT %d
        );
        """ % (_table_name, _table_name, num)

    def __str__(self):
        return (
                "DTEventEntity{\n" +
                "    id: %d,\n" % self.identifier +
                "    app_id: %s,\n" % self.app_id +
                "    server_url: %s,\n" % self.server_url +
                "    token: %s,\n" % self.token +
                "    data: %s,\n" % self.data +
                "    created_at: %d\n" % self.created_at +
                "}"
        )


def _from_query_result(raw):
    result = []
    for row in raw:
        result.append(DTEventEntity(row[1], row[2], row[3], row[4], row[5], identifier=row[0]))
    return result


class DTEventDao:
    def __init__(
            self,
            write_func,
            read_func
    ):
        self.__write_func = write_func
        self.__read_func = read_func
        self.__virtual_size = -1    # get db len at first run.

    @staticmethod
    def create(cursor):
        """Create table

        :param cursor: Database cursor
        """
        cursor.execute(DTEventEntity.create_statement())

    def insert(self, entity, cache_size, strategy):
        """Insert single entity to producer table

        :param entity: DTEventEntity
        :param cache_size:
        :param strategy:
        """
        self.__write_func(lambda c: self.__insert_inner(c, entity, cache_size=cache_size, strategy=strategy))

    def __insert_inner(self, c, entity, cache_size, strategy):
        (beg, end) = self.__get_insert_range(c, 1, cache_size, strategy)

        self.invalidate_virtual_size(c)
        if beg < end:
            c.execute(entity.insert_statement())
            self.__virtual_size += 1

    def insert_batch(self, cache_size, strategy, *entities):
        """Insert batch of entities to table

        :param entities: DTEventEntities
        :param cache_size:
        :param strategy:
        """
        self.__write_func(lambda c: self.__insert_batch_inner(c, cache_size, strategy, *entities))

    def __insert_batch_inner(self, c, cache_size, strategy, *entities):
        (beg, end) = self.__get_insert_range(c, len(entities), cache_size, strategy)

        self.invalidate_virtual_size(c)
        for i in range(beg, end):
            c.execute(entities[i].insert_statement())
        self.__virtual_size += end - beg

    def __get_insert_range(self, c, entities_size,
                           cache_size, strategy):
        """ [int, int) """
        new_size = entities_size + self.__virtual_size
        exceed = new_size - cache_size
        if cache_size > 0 and exceed > 0:
            if strategy == ExceedInsertionStrategy.DELETE:
                c.execute(DTEventEntity.delete_oldest_statement(exceed))
                self.__virtual_size -= exceed
                return 0, entities_size
            elif strategy == ExceedInsertionStrategy.DISCARD:
                return 0, entities_size - exceed
            elif strategy == ExceedInsertionStrategy.DISCARD_HEAD:
                return exceed, entities_size
            elif strategy == ExceedInsertionStrategy.ABORT:
                return 0, 0
            elif strategy == ExceedInsertionStrategy.IGNORE:
                return 0, entities_size
        return 0, entities_size

    def query_all(self):
        """Query all entities in table

        :return: all entities
        """
        result = self.__write_func(lambda c: self.__query_all_inner(c))
        if result is None:
            return []
        return result

    @staticmethod
    def __query_all_inner(c):
        c.execute(DTEventEntity.query_all_unqueried_statement())
        result = c.fetchall()
        DTEventDao.__set_to_queried(c, *map(lambda x: (x[0],), result))
        return _from_query_result(result)

    def query_batch(self, limit, offset = 0):
        """Query `limit` entities starting from `offset`'s row in table

        :return: `limit` entities
        """
        result = self.__write_func(lambda c: self.__query_batch_inner(c, limit, offset))
        if result is None:
            return []
        return result

    @staticmethod
    def __query_batch_inner(c, limit, offset):
        c.execute(DTEventEntity.query_batch_unquired_statement(limit, offset))
        result = c.fetchall()
        DTEventDao.__set_to_queried(c, *map(lambda x: (x[0],), result))
        return _from_query_result(result)

    @staticmethod
    def __set_to_queried(c, *ids):
        timer = TimeMonitor().start_with("event_dao-set_queried")
        c.executemany(DTEventEntity.set_queried_statement(True), ids)
        timer.stop(one_shot=False)
        _CounterMonitor["event_dao-set_queried"] += len(ids)

    def restore_to_unqueried(self, ids):
        self.__write_func(lambda c: self.__restore_to_unqueried_inner(c, ids))

    @staticmethod
    def __restore_to_unqueried_inner(c, ids):
        c.executemany(DTEventEntity.set_queried_statement(False), ids)

    def delete_by_ids(self, ids):
        """Delete all entities in table
        """
        self.__write_func(lambda c: self.__delete_by_ids_inner(c, ids))

    def __delete_by_ids_inner(self, c, ids):
        self.__virtual_size -= len(ids)
        c.executemany(DTEventEntity.delete_by_id_statement(), ids)

    def __len__(self):
        # result = self.__read_func(self.__size)
        # if result is None:
        #     return 0
        # return result

        self.invalidate_virtual_size(None)
        # Uses virtual size instead of querying actual size (I/O) to saving time.
        return self.__virtual_size

    def invalidate_virtual_size(self, c=None, force=False):
        if force or self.__virtual_size < 0:
            result = self.__size(c) if c is not None else self.__read_func(self.__size)
            if result is None:
                self.__virtual_size = 0
            else:
                self.__virtual_size = max(0, result)

    @staticmethod
    def __size(c):
        c.execute(DTEventEntity.get_unqueried_size_statement())
        result = c.fetchone()
        return result[0]

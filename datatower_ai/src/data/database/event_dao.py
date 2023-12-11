import time
from sqlite3 import Cursor
from typing import List, Tuple, Callable, Any, Optional

from future.types.newint import long

_table_name = "events"


class DTEventEntity:
    def __init__(self, data: str, app_id: str, server_url: str, token: str, created_at: long = time.time(), identifier: long = -1):
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
    def query_batch_unquired_statement(limit: int, offset: int):
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
    def set_queried_statement(queried: bool):
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


def _from_query_result(raw: List[tuple]) -> List[DTEventEntity]:
    result = []
    for row in raw:
        result.append(DTEventEntity(row[1], row[2], row[3], row[4], row[5], identifier=row[0]))
    return result


class DTEventDao:
    def __init__(
            self,
            write_func: Callable[[Callable[[Cursor], Any]], Optional[Any]],
            read_func: Callable[[Callable[[Cursor], Any]], Optional[Any]]
    ):
        self.__write_func = write_func
        self.__read_func = read_func
        self.__virtual_size = 0

    @staticmethod
    def create(cursor: Cursor):
        """Create table

        :param cursor: Database cursor
        """
        cursor.execute(DTEventEntity.create_statement())

    def insert(self, entity: DTEventEntity):
        """Insert single entity to producer table

        :param entity: DTEventEntity
        """
        self.__write_func(lambda c: self.__insert_inner(c, entity))

    def __insert_inner(self, c: Cursor, entity: DTEventEntity):
        self.__virtual_size += 1
        c.execute(entity.insert_statement())

    def insert_batch(self, *entities: DTEventEntity):
        """Insert batch of entities to table

        :param entities: DTEventEntities
        """
        self.__write_func(lambda c: self.__insert_batch_inner(c, *entities))

    def __insert_batch_inner(self, c: Cursor, *entities: DTEventEntity):
        self.__virtual_size += len(entities)
        for entity in entities:
            c.execute(entity.insert_statement())

    def query_all(self) -> List[DTEventEntity]:
        """Query all entities in table

        :return: all entities
        """
        result = self.__write_func(lambda c: self.__query_all_inner(c))
        if result is None:
            return []
        return result

    @staticmethod
    def __query_all_inner(c: Cursor):
        c.execute(DTEventEntity.query_all_unqueried_statement())
        result = c.fetchall()
        c.executemany(DTEventEntity.set_queried_statement(True), map(lambda x: (x[0],), result))
        return _from_query_result(result)

    def query_batch(self, limit: int, offset: int = 0) -> List[DTEventEntity]:
        """Query `limit` entities starting from `offset`'s row in table

        :return: `limit` entities
        """
        result = self.__write_func(lambda c: self.__query_batch_inner(c, limit, offset))
        if result is None:
            return []
        return result

    @staticmethod
    def __query_batch_inner(c: Cursor, limit: int, offset: int):
        c.execute(DTEventEntity.query_batch_unquired_statement(limit, offset))
        result = c.fetchall()
        c.executemany(DTEventEntity.set_queried_statement(True), map(lambda x: (x[0],), result))
        return _from_query_result(result)

    def restore_to_unquired(self, ids: List[Tuple]):
        self.__write_func(lambda c: self.__restore_to_unquired_inner(c, ids))

    @staticmethod
    def __restore_to_unquired_inner(c: Cursor, ids: List[Tuple]):
        c.executemany(DTEventEntity.set_queried_statement(False), ids)

    def delete_by_ids(self, ids: List[Tuple]):
        """Delete all entities in table
        """
        self.__write_func(lambda c: self.__delete_by_ids_inner(c, ids))

    def __delete_by_ids_inner(self, c: Cursor, ids: List[Tuple]):
        self.__virtual_size -= len(ids)
        c.executemany(DTEventEntity.delete_by_id_statement(), ids)

    def __len__(self):
        # result = self.__read_func(self.__size)
        # if result is None:
        #     return 0
        # return result

        # Uses virtual size instead of querying actual size (I/O) to saving time.
        return max(0, self.__virtual_size)

    # @staticmethod
    # def __size(c: Cursor):
    #     c.execute(DTEventEntity.get_unqueried_size_statement())
    #     result = c.fetchone()
    #     return result[0]

import sqlite3
from sqlite3 import Cursor
from typing import Union, Optional

from future.types.newint import long


class DTConfigEntity:
    def __init__(self, name: str, value, identifier: long = -1):
        self.name = name
        self.__value = value
        self.identifier = identifier

    @property
    def value(self):
        return self.__value

    @staticmethod
    def create_statement():
        return """
            CREATE TABLE IF NOT EXISTS configs
            (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                value TEXT
            );
        """

    @staticmethod
    def query_statement(name: str):
        return """
        SELECT _id, name, value FROM configs WHERE name = '%s'
        """ % name

    def inert_statement(self):
        return """
        INSERT INTO configs 
        (name, value) 
        VALUES ('%s', '%s');
        """ % (self.name, self.value)

    @staticmethod
    def update_statement(name: str, value):
        return """
        UPDATE configs 
        SET value = '%s' 
        WHERE name = '%s';
        """ % (value, name)

    @staticmethod
    def delete_statement(name: str):
        return """
        DELETE FROM configs 
        WHERE name = '%s';
        """ % name

    def __str__(self):
        return (
            "DTConfigEntity{\n" +
            "    id: %d,\n" % self.identifier +
            "    name: %s,\n" % self.name +
            "    value: %s\n" % self.value +
            "}"
        )


def _from_query_result(raw: Optional[tuple]) -> Optional[DTConfigEntity]:
    if raw is None:
        return None
    return DTConfigEntity(raw[1], raw[2], identifier=raw[0])


class DTConfigDao:
    def __init__(self, conn: sqlite3.Connection):
        self.__conn = conn

    @staticmethod
    def create(cursor: Cursor):
        cursor.execute(DTConfigEntity.create_statement())

    def insert(self, entity: DTConfigEntity):
        """Insert single entity to table

        :param entity: DTEventEntity
        """
        c = self.__conn.cursor()
        c.execute(entity.inert_statement())
        c.close()

    def update(self, name: str, value):
        """Update an entity from table

        :param name: Name
        :param value: Value
        """
        c = self.__conn.cursor()
        c.execute(DTConfigEntity.update_statement(name, value))
        c.close()

    def query(self, name: str) -> Union[DTConfigEntity, None]:
        """Query entity by name from table

        :return: queried entity
        """
        c = self.__conn.cursor()
        c.execute(DTConfigEntity.query_statement(name))
        result = c.fetchone()
        c.close()
        return _from_query_result(result)

    def delete(self, name: str):
        """Delete an entities from table"""
        c = self.__conn.cursor()
        c.execute(DTConfigEntity.delete_statement(name))
        c.close()

import logging
import sqlite3
from sqlite3 import Cursor, OperationalError
from typing import Callable, Any, Optional

from datatower_ai.src.data.database.config_dao import DTConfigDao
from datatower_ai.src.data.database.event_dao import DTEventDao
from datatower_ai.src.util.logger import Logger
from datatower_ai.src.util.performance.time_monitor import TimeMonitor
from datatower_ai.src.util.singleton import Singleton
from datatower_ai.src.util.thread.file_lock import FileLock
from datatower_ai.src.util.thread.swmr_lock import SingleWriteMultiReadLock

"""
DT Database version note:
v1: 
    - Created events table and configs table
"""
DT_DB_VERSION = 1


class _DTDatabaseBase:
    """Database for DT Core

    This is a singleton class, feel safe to call `DTDatabase()` multiple times.

    Needs to add a new DAO?
    1. add it as a class property in __init__(), self.xxx_dao = XXXDao()
    2. add its creator to __on_create()
    """

    def __init__(self, name: str):
        self.__conn = sqlite3.connect(name, check_same_thread=False)
        self.__swmr_lock = SingleWriteMultiReadLock()
        self.__db_file_lock = FileLock(name + ".lock")

        # Place DAOs below
        self.event_dao = DTEventDao(self.do_write, self.do_read)
        self.configs_dao = DTConfigDao(self.__conn)
        # Place DAOs above

        self.__init_db()

    def __init_db(self):
        crt_version = self.get_version()

        cursor = self.__conn.cursor()
        if crt_version == 0:
            self.__on_create(cursor, DT_DB_VERSION)
            self.__set_version(DT_DB_VERSION, cursor, False)
        elif crt_version != DT_DB_VERSION:
            if crt_version < DT_DB_VERSION:
                success = self.__on_upgrade(cursor, crt_version, DT_DB_VERSION)
            else:
                success = self.__on_downgrade(cursor, crt_version, DT_DB_VERSION)

            Logger.log("DT-DB, is version migration (%d -> %d) success?: %s" % (crt_version, DT_DB_VERSION, success), logging.DEBUG)
            if success:
                self.__set_version(DT_DB_VERSION, cursor, False)
        else:
            Logger.log("DT-DB, version unchanged: %d" % crt_version, logging.DEBUG)

        self.__conn.commit()
        cursor.close()

    @staticmethod
    def __on_create(cursor: Cursor, version: int):
        Logger.log("DT-DB, on_create: %d" % version, logging.DEBUG)

        # Place DAO creator below
        DTEventDao.create(cursor)
        DTConfigDao.create(cursor)
        # Place DAO creator above

        Logger.log("DT-DB, on_create: %d, finished" % version, logging.DEBUG)

    @staticmethod
    def __on_upgrade(cursor: Cursor, crt_version: int, target_version: int) -> bool:
        Logger.log("DT-DB, on_upgrade: %d -> %d" % (crt_version, target_version), logging.DEBUG)
        # no need to implement yet
        return True

    @staticmethod
    def __on_downgrade(cursor: Cursor, crt_version: int, target_version: int) -> bool:
        Logger.log("DT-DB, on_downgrade: %d -> %d" % (crt_version, target_version), logging.DEBUG)
        # no need to implement yet
        return True

    def get_version(self) -> int:
        """Get current database version

        :return: Current database version
        """
        c = self.__conn.cursor()
        c.execute("PRAGMA user_version;")
        result = c.fetchone()[0]
        c.close()
        return result

    def __set_version(self, version: int, cursor: sqlite3.Cursor, commit: bool = True):
        cursor.execute("PRAGMA user_version = %d;" % version)
        if commit:
            self.__conn.commit()

    def do_write(self, func: Callable[[Cursor], Any]) -> Optional[Any]:
        """Only one write and no read(s) is allowed simultaneously

        Use this method to insert/update/delete data if using this Database in concurrent env.
        """
        timer = TimeMonitor().start("acquire_db_file_lock")
        self.__db_file_lock.acquire()
        timer.stop(one_shot=False)
        self.__swmr_lock.acquire_write()
        try:
            cursor = self.__conn.cursor()
            result = func(cursor)
            cursor.close()
            self.__conn.commit()
        except OperationalError as e:
            Logger.log("DT-Database, do_write, \n" + repr(e), logging.ERROR)
            result = None
        finally:
            self.__swmr_lock.release_write()
            self.__db_file_lock.release()
        return result

    def do_read(self, func: Callable[[Cursor], Any]) -> Optional[Any]:
        """Allows multiple read and no write at the sametime

        Use this method to query data if using this Database in concurrent env.
        """
        timer = TimeMonitor().start("acquire_db_file_lock")
        self.__db_file_lock.acquire()
        timer.stop(one_shot=False)
        self.__swmr_lock.acquire_read()
        try:
            cursor = self.__conn.cursor()
            result = func(cursor)
            cursor.close()
        except OperationalError as e:
            Logger.log("DT-Database, do_read, \n" + repr(e), logging.ERROR)
            result = None
        finally:
            self.__swmr_lock.release_read()
            self.__db_file_lock.release()
        return result


class _DTDatabase(Singleton):
    def __init__(self):
        self.__instances = {}

    def get(self, name: str) -> _DTDatabaseBase:
        if name not in self.__instances:
            # "datatower_ai.db"
            self.__instances[name] = _DTDatabaseBase(name)
        return self.__instances[name]

    def get_by_app_id(self, app_id: str) -> _DTDatabaseBase:
        return self.get("datatower_ai_{}.db".format(app_id))


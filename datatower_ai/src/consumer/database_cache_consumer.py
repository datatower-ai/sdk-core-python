import logging
from threading import Semaphore
from typing import Callable, Dict, List, Tuple

from datatower_ai.src.consumer.abstract_consumer import AbstractConsumer
from datatower_ai.src.data.database.dt_database import _DTDatabase

from datatower_ai.src.data.database.event_dao import DTEventEntity, DTEventDao

from datatower_ai.src.service.http_service import _HttpService
from datatower_ai.src.strategy.exceed_insertion_strategy import ExceedInsertionStrategy
from datatower_ai.src.util.logger import Logger
from datatower_ai.src.util.performance.counter_monitor import CounterMonitor

from datatower_ai.src.util.thread.thread import WorkerManager, Task

from datatower_ai.src.util.performance.time_monitor import TimeMonitor

from datatower_ai import default_server_url


class DatabaseCacheConsumer(AbstractConsumer):
    def __init__(self, app_id, token, server_url=default_server_url, batch_size: int = 50,
                 network_retries: int = 3, network_timeout: int = 3000, num_db_threads: int = 2,
                 num_network_threads: int = 2, thread_keep_alive_ms: int = -1,
                 cache_size: int = 5000,
                 exceed_insertion_strategy: ExceedInsertionStrategy = ExceedInsertionStrategy.DELETE):
        """Uploading with Database caching support, to prevent data loss.

        * Async
        * Database caching

        :param app_id: App id (DataTower.ai dashboard).
        :param token: Communication token (DataTower.ai dashboard).
        :param server_url: Server url (DataTower.ai dashboard).
        :param batch_size: Size of each uploading batch.
        :param network_retries: Maximum of retries allowed for each uploading request.
        :param network_timeout: Allowed timeout in milliseconds of each uploading request.
        :param num_db_threads: Number of threads for Database operation (w/r).
        :param num_network_threads: Number of threads for uploading.
        :param thread_keep_alive_ms: Time in milliseconds, that the thread will be held when idle for such time. After
        this time, the thread will be terminated. BTW, those threads will be recreated once new task arrived. (
        Valid value is >= 0. Otherwise, unlimited).
        :param cache_size: The maximum size of cache (number of events) can be held.
        :param exceed_insertion_strategy: The insertion strategy applied when num of records in database is
        reached 'cache_size'.
        """
        network_timer = TimeMonitor().start("db_cache_network")
        self.__network_wm = WorkerManager("db_cache_network", max(1, num_network_threads), keep_alive_ms=thread_keep_alive_ms,
                                          on_all_workers_stop=lambda: network_timer.pause(),
                                          on_all_worker_revived=lambda: network_timer.resume(),
                                          on_terminate=lambda: network_timer.stop(one_shot=False))

        database_timer = TimeMonitor().start("db_cache_database")
        self.__db_wm = WorkerManager("db_cache_database", max(1, num_db_threads), keep_alive_ms=thread_keep_alive_ms,
                                     on_all_workers_stop=lambda: database_timer.pause(),
                                     on_all_worker_revived=lambda: database_timer.resume(),
                                     on_terminate=lambda: database_timer.stop(one_shot=False))

        self.__http_service = _HttpService(max(0, network_timeout), max(0, network_retries))
        self.__app_id = app_id
        self.__token = token
        self.__server_url = server_url
        self.__batch_size = max(1, batch_size)
        self.__cache_size = cache_size
        self.__exceed_insertion_strategy = exceed_insertion_strategy
        self.__timer = TimeMonitor().start("db_cache_consumer")

    def get_app_id(self):
        return self.__app_id

    def add(self, get_msg: Callable[[], List[str]]):
        self.__db_wm.execute(lambda: self.__insert_to_db(get_msg, self.__cache_size, self.__exceed_insertion_strategy))

    def flush(self):
        self.__db_wm.execute(self.__query_from_db)

    def close(self):
        self.__db_wm.execute(self.__query_from_db)
        self.__db_wm.execute(lambda: self.__network_wm.terminate())
        self.__db_wm.terminate()

    def __insert_to_db(self, get_data: Callable[[], List[str]], cache_size: int, strategy: ExceedInsertionStrategy):
        data = get_data()

        events = [DTEventEntity(x, self.__app_id, self.__server_url, self.__token) for x in data]

        timer = TimeMonitor().start("UploadFromDbTask-insert")
        _DTDatabase().event_dao.insert_batch(*events, cache_size=cache_size, strategy=strategy)
        timer.stop(one_shot=False)
        CounterMonitor["UploadFromDbTask-insert"] += len(events)

        self.__db_wm.execute(self.__query_from_db)

    def __query_from_db(self):
        self.__db_wm.execute(_QueryFromDbTask(self.__db_wm, self.__network_wm, self.__http_service, self.__batch_size))

    def __del__(self):
        Logger.log("="*80)
        Logger.log("[Statistics] db_cache_consumer time used: {:.2f}".format(
            self.__timer.stop())
        )
        Logger.log("[Statistics] db_cache_network time used: {:.2f}".format(
            TimeMonitor().get_sum("db_cache_network"))
        )
        Logger.log("[Statistics] db_cache_database time used: {:.2f}".format(
            TimeMonitor().get_sum("db_cache_database"))
        )
        Logger.log("[Statistics] 'Query' time sum: {:.2f}, avg: {:.2f}, count: {}".format(
            TimeMonitor().get_sum("UploadFromDbTask-query"),
            TimeMonitor().get_avg("UploadFromDbTask-query"),
            CounterMonitor["UploadFromDbTask-query"]))
        Logger.log("[Statistics] 'mark_queried' time used: {:.2f}, avg: {:.2f}".format(
            TimeMonitor().get_sum("event_dao-set_queried"), TimeMonitor().get_avg("event_dao-set_queried"))
        )
        Logger.log("[Statistics] 'Insert' time sum: {:.2f}, avg: {:.2f}, count: {}".format(
            TimeMonitor().get_sum("UploadFromDbTask-insert"),
            TimeMonitor().get_avg("UploadFromDbTask-insert"),
            CounterMonitor["UploadFromDbTask-insert"]))
        Logger.log("[Statistics] 'delete' time sum: {:.2f}, avg: {:.2f}, count: {}".format(
            TimeMonitor().get_sum("UploadFromDbTask-delete"),
            TimeMonitor().get_avg("UploadFromDbTask-delete"),
            CounterMonitor["UploadFromDbTask-delete"]))
        Logger.log("[Statistics] 'Update' time sum: {:.2f}, avg: {:.2f}, count: {}".format(
            TimeMonitor().get_sum("UploadFromDbTask-update_unquired"),
            TimeMonitor().get_avg("UploadFromDbTask-update_unquired"),
            CounterMonitor["UploadFromDbTask-update_unquired"]))
        Logger.log("[Statistics] 'Send' time sum: {:.2f}, avg: {:.2f}, count: {}".format(
            TimeMonitor().get_sum("UploadFromDbTask-send"),
            TimeMonitor().get_avg("UploadFromDbTask-send"),
            CounterMonitor["UploadFromDbTask-send"]))
        Logger.log("[Statistics] 'acquire_db_file_lock' time sum: {:.2f}, avg: {:.2f}".format(
            TimeMonitor().get_sum("acquire_db_file_lock"), TimeMonitor().get_avg("acquire_db_file_lock")
        ))
        Logger.log("[Statistics] total success: {}, total failed: {}".format(
            CounterMonitor["UploadFromDbTask-success"], CounterMonitor["UploadFromDbTask-fail"]))
        Logger.log("="*80)


class _QueryFromDbTask(Task):
    __has_pending_task = False
    __is_doing = False

    __sem = Semaphore()

    def __init__(self, db_wm, nw_wm, http_service, batch_size):
        self.__db_wm = db_wm
        self.__nw_wm = nw_wm
        self.__http_service = http_service
        self.__batch_size = max(1, batch_size)

    def run(self):
        _QueryFromDbTask.__sem.acquire()
        if _QueryFromDbTask.__is_doing:
            # Rejects _QueryFromDbTask while current one is doing and combine these call to a single later call.
            _QueryFromDbTask.__has_pending_task = True
            _QueryFromDbTask.__sem.release()
            return
        _QueryFromDbTask.__is_doing = True
        _QueryFromDbTask.__sem.release()

        dao = _DTDatabase().event_dao

        # Upper bound of size of queried entities this task: (x * BATCH_SIZE * 2)_min
        # where (x * BATCH_SIZE * 2) > length.
        length = len(dao)

        if length == 0:
            self.__phase_end(no_need_upload=True)
            return

        num_queried = 0
        n = self.__batch_size

        while num_queried < length:
            entities = _QueryFromDbTask.query_entities(dao, n*2, 0)

            if len(entities) == 0:
                Logger.log("[ReadFromDbAndUpload] entities is empty, length=%d, num_queried=%d, break" % (length, num_queried), logging.DEBUG)
                dao.invalidate_virtual_size(force=True)       # length is unmatched with actual, needs to invalidate
                break

            num_queried += len(entities)

            # group by app_id, server_url, token
            grouped = {}
            for entity in entities:
                key = "{}${}${}".format(entity.app_id, entity.server_url, entity.token)
                if key in grouped:
                    grouped[key].append(entity)
                else:
                    grouped[key] = [entity]

            # divide by reporting batch size
            for etts in grouped.values():
                for i in range(len(etts) + n - 1):
                    ett = etts[i*n:(i+1)*n]
                    if len(ett) == 0:
                        continue
                    self.__nw_wm.execute(_UploadTask(self.__http_service, ett, self.__after_upload))

        self.__phase_end()

    @staticmethod
    def query_entities(dao: DTEventDao, batch_size: int, offset: int = 0) -> List[DTEventEntity]:
        monitor = TimeMonitor()
        timer = monitor.start("UploadFromDbTask-query")
        if batch_size > 0 and offset >= 0:
            entities = dao.query_batch(limit=batch_size, offset=offset)
        else:
            entities = dao.query_all()
        timer.stop(one_shot=len(entities) <= 0)
        CounterMonitor["UploadFromDbTask-query"] += len(entities)
        return entities

    def __after_upload(self, success: bool, ids: List[Tuple[int]]):
        """Ensure all uploading is done whatever succeed or failed, before calling phase end"""
        if success:
            CounterMonitor["UploadFromDbTask-success"] += len(ids)
        else:
            CounterMonitor["UploadFromDbTask-fail"] += len(ids)

        if success:
            timer = TimeMonitor().start("UploadFromDbTask-delete")
            self.__db_wm.execute(lambda: _DTDatabase().event_dao.delete_by_ids(ids))
            timer.stop(one_shot=False)
            CounterMonitor["UploadFromDbTask-delete"] += len(ids)
        else:
            timer = TimeMonitor().start("UploadFromDbTask-update_unquired")
            self.__db_wm.execute(lambda: _DTDatabase().event_dao.restore_to_unquired(ids))
            timer.stop(one_shot=False)
            CounterMonitor["UploadFromDbTask-update_unquired"] += len(ids)

        Logger.log("[Upload Statistic] total success: %s, total failed: %s" % (
            CounterMonitor["UploadFromDbTask-success"], CounterMonitor["UploadFromDbTask-fail"]
        ), logging.INFO)

    def __phase_end(self, no_need_upload: bool = False):
        #Logger.debug("[ReadFromDbAndUpload] Phase finished, has_pending: {}".format(_QueryFromDbTask.__has_pending_task))
        _QueryFromDbTask.__sem.acquire()
        _QueryFromDbTask.__is_doing = False
        has_pending = _QueryFromDbTask.__has_pending_task
        _QueryFromDbTask.__has_pending_task = False
        _QueryFromDbTask.__sem.release()

        if not no_need_upload and has_pending:
            self.__db_wm.execute(_QueryFromDbTask(self.__db_wm, self.__nw_wm, self.__http_service, self.__batch_size))


class _UploadTask(Task):
    def __init__(self, http_service: _HttpService, entities, after_upload: Callable[[bool, List[Tuple[int]]], None]):
        self.__http_service = http_service
        self.__entities = entities
        self.__after_upload = after_upload

    def run(self):
        if len(self.__entities) == 0:
            self.__after_upload(True, [])

        is_success = False

        app_id = self.__entities[0].app_id
        server_url = self.__entities[0].server_url
        token = self.__entities[0].token

        data = '[' + ','.join(map(lambda x: x.data, self.__entities)) + ']'

        timer = TimeMonitor().start("UploadFromDbTask-send")
        try:
            is_success = self.__http_service.post_event(
                app_id=app_id,
                server_url=server_url,
                token=token,
                data=data,
                length=str(len(self.__entities))
            )
        except:
            Logger.error("[UploadTask] send failed!")
        finally:
            timer.stop(one_shot=False)
            CounterMonitor["UploadFromDbTask-send"] += len(self.__entities) if is_success else 0

        if is_success:
            Logger.log("[UploadTask] Uploaded {} events! ({}, {})".format(len(self.__entities), server_url, app_id), logging.DEBUG)
        else:
            Logger.log("[UploadTask] Upload failed for {} events! ({}, {})".format(len(self.__entities), server_url, app_id), logging.WARNING)

        self.__after_upload(is_success, [(x.identifier,) for x in self.__entities])

    def fallback(self, reason: Tuple[int, str]):
        Logger.warning(
            "[UploadTask] Task is not run, reason: \"{}\". This batch of data (length: {}) will be retained ".format(
                reason[1], len(self.__entities)
            )
        )
        self.__after_upload(False, [(x.identifier,) for x in self.__entities])

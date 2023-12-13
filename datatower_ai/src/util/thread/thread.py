import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Callable

from datatower_ai.src.util.logger import Logger
from future.types.newint import long

from threading import Thread, Condition, Semaphore, Event

try:
    import queue
except ImportError:
    import Queue as queue


class Task(ABC):
    @abstractmethod
    def run(self):
        pass


class _Overtime:
    def __init__(self, time_allowed: float):
        self.__time_allowed = time_allowed

    def is_overtime(self, start_time: float):
        return time.time() - start_time > self.__time_allowed

    def __lt__(self, other):
        if isinstance(other, _Overtime):
            return self.__time_allowed < other.__time_allowed
        else:
            return self.__time_allowed < other


class _TERMINATE:
    pass


_TERMINATE_SIG = _TERMINATE()


class Worker(Thread):
    """Thread executing task from given task queue"""

    def __init__(
            self, name: str, idx: int, tasks: queue.Queue, condition: Condition, semaphore: Semaphore,
            barrier: Event, barrier_timeout_ms: long = 100,
            allowed_overtime_ms: long = -1,
            on_terminate: Callable[[int], None] = lambda _: None,
    ):
        Thread.__init__(self, name=name)
        self.__idx = idx
        self.__tasks = tasks
        self.__name = name
        self.__condition = condition
        self.__semaphore = semaphore
        self.__barrier = barrier
        self.__barrier_timeout = barrier_timeout_ms
        self.__latest_time = 0
        self.__allowed_overtime = allowed_overtime_ms / 1000
        self.__on_terminate = on_terminate
        Logger.log("[%s] is initializing" % self.__name, logging.DEBUG)

    def run(self):
        """Keep running in the thread to grab and run the task from tasks queue.

        Waiting if no task available yet.
        """
        while True:
            self.__barrier.wait(self.__barrier_timeout / 1000)

            self.__semaphore.acquire()
            if self.__tasks.empty():
                self.__semaphore.release()
                halt = self.__handle_empty_tasks()
            else:
                try:
                    (target_time, task) = self.__tasks.get_nowait()
                    self.__semaphore.release()
                    halt = self.__handle_task(target_time, task)
                except TypeError as e:
                    self.__tasks.put_nowait(
                        (time.time()/1000 + max(0.0, self.__allowed_overtime), _Overtime(self.__allowed_overtime))
                    )
                    self.__semaphore.release()
                    halt = False
                    Logger.log("Thread#run, get_nowait()\n" + repr(e), logging.ERROR)

            if halt:
                self.__on_terminate(self.__idx)
                break

    def __handle_empty_tasks(self) -> bool:
        self.__condition.acquire()
        if self.__allowed_overtime >= 0:
            # Logger.debug(
            #     "%s tasks is empty, sent overtime signal (%.2fs).",self.__name, self.__allowed_overtime
            # )
            self.__semaphore.acquire()
            self.__tasks.put_nowait((time.time()/1000 + self.__allowed_overtime, _Overtime(self.__allowed_overtime)))
            self.__semaphore.release()

            # wait to handle overtime sig or be notified by other task.
            self.__condition.wait(self.__allowed_overtime)
        else:
            # Logger.debug("%s tasks is empty, wait...", self.__name)
            self.__condition.wait()  # wait for task notification.
        self.__condition.release()
        return False

    def __handle_task(self, target_time: float, task) -> bool:
        """When tasks is not empty

        :returns: Is need to halt this worker
        """
        # Logger.debug("%s get something", self.__name)

        pre_time = self.__latest_time
        crt_time = time.time()
        self.__latest_time = crt_time

        # terminate signal
        if isinstance(task, _TERMINATE):
            Logger.log("[%s] get the terminate signal (qsize: %d)" % (self.__name, self.__tasks.qsize()), logging.DEBUG)
            return True

        # delayed task
        if target_time > crt_time:
            # Logger.debug(
            #     "%s get a task but time is not met (current: %.3f, target: %.3f, diff: %.2fms)",
            #     self.__name, crt_time, target_time, (target_time - crt_time) * 1000
            # )
            self.__semaphore.acquire()
            self.__tasks.put_nowait((target_time, task))  # put it back
            self.__semaphore.release()

            self.__condition.acquire()
            self.__condition.wait(time.time() - target_time)
            self.__condition.release()
            return False

        # overtime checker
        if isinstance(task, _Overtime):
            if pre_time == 0:
                return False
            # Logger.debug("%s get the overtime signal (target: %.3fs, actual: %.3fs)", self.__name,
            #              self.__allowed_overtime, crt_time - pre_time)

            # If overtime condition is met, terminate. Otherwise, ignore.
            if task.is_overtime(pre_time):
                Logger.log("[%s] get the overtime signal and confirmed" % self.__name, logging.DEBUG)
                return True
            return False

        # normal task
        try:
            # Logger.debug("%s get a task, doing...", self.__name)
            if isinstance(task, Task):
                task.run()
            else:
                task()
            # Logger.debug("%s finished the task", self.__name)
        except TypeError as e:
            Logger.log(
                "[%s] Type of task is not valid, get: %s\n%s" % (self.__name, type(task), repr(e)), logging.ERROR
            )
        except Exception as e:
            Logger.log("[%s] Exception occur during running task()\n%s" % (self.__name, repr(e)), logging.ERROR)


class WorkerManager:
    """Worker Manager is used to dispatch tasks to worker(s).

    Provided features:
        - Pooling
        - Queue
        - Delayed execution
        - halt when idle and recreate when needed
    """

    def __init__(self,
                 name: str,
                 size: int = 1,
                 keep_alive_ms: long = -1,
                 on_all_workers_stop: Callable = lambda: (),
                 on_terminate: Callable = lambda: ()
                 ):
        """Creating a WorkerManager with fix number of workers

        :param name: Name of this worker manager, workers will be named by ${name}#0 ... ${name}#${size}
        :param size: Number of workers under this WorkerManager
        :param keep_alive_ms: Keep worker alive time in milliseconds after queue is empty (not more task to do)
        :param on_all_workers_stop: Callback when all workers terminated.
        """
        Logger.log("[WorkerManager] WorkerManager(name: %s, size: %d) is initializing" % (name, size), logging.DEBUG)
        self.__name = name
        self.__size = size
        self.__condition = Condition()
        self.__semaphore = Semaphore()
        self.__queue = queue.PriorityQueue()
        self.__workers = []
        self.__terminated = False
        self.__started = False
        self.__keep_alive_ms = keep_alive_ms
        self.__barrier = Event()
        self.__barrier.set()
        self.__on_terminate = on_terminate
        self.__on_all_workers_stop = on_all_workers_stop

    def __len__(self):
        return self.__size

    def __create_worker(self, idx: int) -> Worker:
        return Worker(
            "%s#%d" % (self.__name, idx), idx, self.__queue, self.__condition, self.__semaphore, self.__barrier,
            barrier_timeout_ms=100,
            allowed_overtime_ms=self.__keep_alive_ms,
            on_terminate=self.__on_worker_terminate
        )

    def start(self):
        """Starting all workers"""
        if self.__started:
            return

        Logger.log("[%s] is starting" % self, logging.DEBUG)
        self.__workers = [
            self.__create_worker(i) for i in range(0, self.__size)
        ]
        for worker in self.__workers:
            worker.start()
        self.__started = True
        self.__terminated = False
        Logger.log("[%s] is started" % self, logging.DEBUG)

    def __revive_all_workers(self):
        """Revive all stopped workers, work iff this WM is started"""
        if not self.__started:
            return
        for i in range(0, len(self.__workers)):
            if not self.__workers[i].is_alive():
                self.__workers[i] = self.__create_worker(i)
                self.__workers[i].start()
                Logger.log("[%s] awaking stopped worker #%d" % (self, i), logging.DEBUG)

    def execute(self, task, delay: int = 0) -> bool:
        """Dispatch the task to workers.

        Will notify a worker to prevent the state of all worker is in wait.

        :param task: The task to do, accepts: function/method/lambda or the class inherited Task.
        :param delay: Schedule the task execution by given delay in milliseconds.
        :return bool: Is this task successfully scheduled, returns False if this WorkerManager is terminated.
        """
        if self.__terminated:
            Logger.log("[%s] received a task, but worker manager is terminated" % self, logging.DEBUG)
            return False

        self.__revive_all_workers()
        if not self.__started:
            Logger.log("[%s] is not started when calling execute(), starting..." % self, logging.DEBUG)
            self.start()

        target_time = time.time() + delay / 1000

        self.__queue.put((target_time, task))
        self.__condition.acquire()
        self.__condition.notify(1)
        self.__condition.release()
        return True

    def terminate(self):
        """Terminate current WorkerManager and wait for all worker joined."""
        Logger.log(
            "TERMINATE {}, self.__started: {}, self.__terminated: {}".format(
                self, self.__started, self.__terminated
            )
        )
        if not self.__started or self.__terminated:
            return
        Logger.log("[%s] terminating..." % self, logging.DEBUG)

        for _ in range(len(self.__workers)+1):
            self.__queue.put((float("inf"), _TERMINATE_SIG))  # puts to the very last

        self.__condition.acquire()
        self.__condition.notify_all()
        self.__condition.release()

        for i in range(0, self.__size):
            worker = self.__workers[i]
            if isinstance(worker, Worker) and worker.is_alive():
                self.__workers[i].join()
                Logger.log("JOINED {}, {}".format(self, i))

        self.__terminated = True
        self.__started = False
        self.__queue.queue.clear()

        self.__on_terminate()
        Logger.log("[%s] terminated!" % self, logging.DEBUG)

    def __on_worker_terminate(self, idx):
        has_alive = False
        for i in range(len(self.__workers)):
            if i != idx and self.__workers[i].is_alive():
                has_alive = True
                break
        if not has_alive:
            self.__on_all_workers_stop()

    def place_barrier(self):
        """Place the barrier to pause all workers.

        REMEMBER to call `remove_barrier()` later, otherwise workers won't go.
        """
        self.__barrier.clear()

    def remove_barrier(self):
        """Remove the barrier and continue the worker."""
        self.__barrier.set()

    def __str__(self):
        return "WorkerManager(name: %s, size: %d, keep_alive_ms: %d)" % (self.__name, self.__size, self.__keep_alive_ms)


if __name__ == "__main__":
    def xx():
        num = random.Random().randint(0, 100)
        Logger.log(str(num), logging.DEBUG)


    wm = WorkerManager("wmm", 5)
    wm.start()
    for _ in range(0, 100):
        wm.execute(xx, delay=random.Random().randint(0, 50))
    wm.terminate()

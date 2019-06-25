import logging
import time
from concurrent.futures import Executor, ThreadPoolExecutor, Future
from datetime import timedelta
from threading import RLock, Timer, Event
from typing import List, Dict

from pymongo.collection import Collection
from pymongo.errors import NetworkTimeout

from utils.caller import Caller


class AsyncWriteCache:

    def __init__(self, collection: Collection, cache_size: int, flush_time: timedelta):
        self._col: Collection = collection
        self._cache_size: int = cache_size
        self._flush_time: timedelta = flush_time

        self.flush_callback = Caller()
        self.error_callback = Caller()

        self._values: List[Dict] = []
        self._timer: Timer = None
        self._write_pool: Executor = ThreadPoolExecutor()
        self._lock: RLock = RLock()

        self._set_timer()

    def insert_one(self, value: Dict) -> None:
        """ Insert an entry into the cache
        :param value: mongodb document
        """
        self._values.append(value)
        self._check_for_flush()

    def insert_many(self, values: List[Dict]) -> None:
        """ Insert many entries into the cache (needed for concurrency) Think about if otherwise someone inserts a value
        between self.__insert_all and self._values = []
        :param values:
        """
        self._values.extend(values)
        self._check_for_flush()

    def _check_for_flush(self) -> None:
        with self._lock:
            if len(self._values) >= self._cache_size:
                self._write()

    def flush(self) -> None:
        """ Must be called before cache is destroyed. It forces to write from memory to database.
        """
        event = Event()
        self.flush_callback.append(event.set)
        self._write()
        event.wait()
        self.flush_callback.remove(event.set)

    def _write(self):
        with self._lock:
            tmp_values = self._values
            self._values = []
            future: Future = self._write_pool.submit(self._insert_many, tmp_values)
            future.add_done_callback(lambda f: self._write_done(tmp_values, f.exception()))

    def _write_done(self, values: List[Dict], ex: Exception = None):
        if ex:
            self.error_callback.call(ex, values)
            return

        self.flush_callback.call()

    def _insert_many(self, docs: List[Dict]):
        while True:
            try:
                self._col.insert_many(docs)
                break
            except NetworkTimeout as ex:
                logging.exception(
                    'NetworkTimeout: during inserting this values: {}\n Trying again after 10 seconds'.format(docs))
                time.sleep(10)
            except Exception as ex:
                raise ex

    def _set_timer(self):
        self._timer = Timer(self._flush_time.total_seconds(), self._timer_up)
        self._timer.start()

    def _timer_up(self):
        if len(self._values) > 0:
            self._write()

        self._set_timer()



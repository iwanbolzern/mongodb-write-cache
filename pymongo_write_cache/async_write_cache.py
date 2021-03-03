import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import timedelta
from threading import RLock, Timer, Event
from typing import List, Dict, Optional

from pymongo.collection import Collection
from pymongo.errors import NetworkTimeout

from pymongo_write_cache.utils.caller import Caller


class AsyncWriteCache:

    def __init__(self, collection: Collection, cache_size: int, flush_time: timedelta,
                 retry_interval: timedelta = timedelta(seconds=10), retry_max: int = 10):
        """Asynchronous write cache for a pymongo collection. It is called async because it does all database operations
        in a background thread and calls to insert_one and insert_many are returned immediately. The cache can be configured
        with a cache_size and a flush_time.
        :param collection: pymongo collection to enable caching
        :param cache_size: max cache size. If this size is reached, the cache is flushed into the database.
        :param flush_time: max timedelta between two flushes. If cache_size is never reached, this timedelta defines, in
        which interval the cache is periodically flushed.
        :param retry_interval: if the database is not reachable, this values specifies the dead time between two tries
        for writing into the database.
        :param retry_max: if the database was not reachable for the specified value, the error will be propagated.
        """
        self._col = collection
        self._cache_size = cache_size
        self._flush_time = flush_time
        self._retry_interval = retry_interval
        self._retry_max = retry_max

        self.flush_callback = Caller()
        self.error_callback = Caller()

        self._values: List[Dict] = []
        self._timer: Optional[Timer] = None
        self._write_pool = ThreadPoolExecutor()
        self._lock = RLock()

        self._set_timer()

    def insert_one(self, value: Dict) -> None:
        """Insert an entry into the cache
        :param value: pymongo document
        """
        self._values.append(value)
        self._check_cache_size_and_flush()

    def insert_many(self, values: List[Dict]) -> None:
        """Insert many entries into the cache. In case e.g. cache_size is set to 5 and one passes 10 documents, the
         cache_size is ignored and all 10 documents are written at the same time!
        :param values: List of pymongo documents
        """
        self._values.extend(values)
        self._check_cache_size_and_flush()

    def _check_cache_size_and_flush(self) -> None:
        with self._lock:
            if len(self._values) >= self._cache_size:
                self._write()

    def flush(self) -> None:
        """Forces write remaining items in memory to database. Must be called before cache is destroyed and will only
        return after all items are stored in the database.
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
        for i in range(self._retry_max):
            try:
                self._col.insert_many(docs)
                break
            except NetworkTimeout as ex:
                if i >= self._retry_max:
                    raise ex

                logging.exception(f'NetworkTimeout during insert\n'
                                  f'Trying again after {self._retry_interval.total_seconds()} seconds')
                time.sleep(self._retry_interval.total_seconds())
            except Exception as ex:
                raise ex

    def _set_timer(self):
        self._timer = Timer(self._flush_time.total_seconds(), self._timer_up)
        self._timer.start()

    def _timer_up(self):
        if len(self._values) > 0:
            self._write()

        self._set_timer()

from datetime import timedelta

from pymongo.collection import Collection

from pymongo_write_cache.mongo_db_async_cache import AsyncWriteCache


class SyncWriteCache(AsyncWriteCache):

    def __init__(self, collection: Collection, cache_size: int, flush_time: timedelta,
                 retry_interval: timedelta = timedelta(seconds=10), retry_max: int = 10):
        """Synchronous write cache for a pymongo collection. It is called sync because it does all database operations
        in the calling thread. This means, call to insert_one and insert_many can take longer because if the cache size
        is reached, it will execute a synchronous insert into the database.
        :param collection: pymongo collection to enable caching
        :param cache_size: max cache size. If this size is reached, the cache is flushed into the database.
        :param flush_time: max timedelta between two flushes. If cache_size is never reached, this timedelta defines, in
        which interval the cache is periodically flushed.
        :param retry_interval: if the database is not reachable, this values specifies the dead time between two tries
        for writing into the database.
        :param retry_max: if the database was not reachable for the specified value, the error will be propagated.
        """
        super(SyncWriteCache, self).__init__(collection, cache_size, flush_time, retry_interval, retry_max)

    def _write(self):
        with self._lock:
            tmp_values = self._values
            self._values = []
            try:
                self._insert_many(tmp_values)
                self._write_done(tmp_values)
            except Exception as ex:
                self._write_done(tmp_values, ex)




from datetime import timedelta

from pymongo.collection import Collection

from mongo_db_async_cache import AsyncWriteCache


class SyncWriteCache(AsyncWriteCache):

    def __init__(self, collection: Collection, cache_size: int, flush_time: timedelta):
        super(SyncWriteCache, self).__init__(collection, cache_size, flush_time)

    def _write(self):
        with self._lock:
            tmp_values = self._values
            self._values = []
            try:
                self._insert_many(tmp_values)
                self._write_done(tmp_values)
            except Exception as ex:
                self._write_done(tmp_values, ex)




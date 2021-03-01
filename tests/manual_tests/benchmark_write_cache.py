from datetime import timedelta, datetime
from threading import Semaphore

from pymongo import MongoClient

from pymongo_write_cache.mongo_db_async_cache import AsyncWriteCache
from pymongo_write_cache.mongo_db_sync_cache import SyncWriteCache

buffer_time = timedelta(seconds=20)
buffer_size = int(1e4)
test_size = int(1e5)


def get_test_collection():
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_client.drop_database('test_db')
    database = mongo_client['test_db']
    return database['test_col']


def test_write_col(col, n):
    start = datetime.now()
    for i in range(n):
        col.insert_one({'test': n})
    print('Needed time for inserting {} values: {}\n'.format(n, datetime.now() - start))


def test_write_cache(cache: AsyncWriteCache, n):
    sem = Semaphore()
    cache.flush_callback.append(sem.release)
    start = datetime.now()
    for i in range(n):
        cache.insert_one({'test': n})
    for _ in range(5):
        sem.acquire()
    print('Needed time for inserting {} values: {}\n'.format(n, datetime.now() - start))


if __name__ == '__main__':
    print('Testing without write cache')
    test_write_col(get_test_collection(), buffer_size * test_size)

    print('Testing Sync Write Cache')
    write_cache = SyncWriteCache(get_test_collection(), buffer_size, buffer_time)
    test_write_cache(write_cache, buffer_size * test_size)

    print('Testing Async Write Cache')
    write_cache = AsyncWriteCache(get_test_collection(), buffer_size, buffer_time)
    test_write_cache(write_cache, buffer_size * test_size)


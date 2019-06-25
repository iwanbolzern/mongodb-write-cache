# Write Cache for pymongo
If you have many single writes to your MongoDB and you don't have immediate needs that
this values are persisted, this package will improve your write speed by 20 times. I've 
used it in a project where sensor values were stored for further analysis. The values are 
persisted when either the buffer is filled or the flush time has elapsed. 

## Usage
```python
# create normal pymongo connection
mongo_client = MongoClient('localhost', 27017)
database = mongo_client['test_db']
collection = database['test_col']

# create write cache with 10k buffer size and 20 seconds flush time
cache = SyncWriteCache(collection, cache_size=10000, flush_time=timedelta(seconds=20))
cache.insert_one({'my-first-document': 'yeeyy'})
cache.insert_one({'my-first-document': 'yeeyy'})
...
cache.flush()
```

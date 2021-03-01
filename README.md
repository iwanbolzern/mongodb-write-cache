# Simple to use write Cache for pymongo
If you have many single writes to MongoDB or any other MongoDB compatible database such
as AWS DocumentDB and you have the requirement that these values must be eventually present,
this package will improve your write speed by up to 20 times. This can be used if you for example are
building a data integration service that should capture data from many individual sensors and this
data shall be stored for further analysis.

The present implementation accomplishes this by filling a write cache up to a specified threshold and then
bulk-inserts the queued values. Or waits until the flush_time is reached and inserts all values captured until
this event.

## Usage
```python
from datetime import timedelta

from pymongo import MongoClient

from pymongo_write_cache import SyncWriteCache


# create normal pymongo connection
mongo_client = MongoClient('localhost', 27017)
database = mongo_client['test_db']
collection = database['test_col']

# create a write cache with 10k buffer size and 20 seconds flush time. This means, flush my cache 
# after either 10k objects are received or 20 seconds have passed.
cache = SyncWriteCache(collection, cache_size=10000, flush_time=timedelta(seconds=20))
cache.insert_one({'my-first-document': 'yeeyy'})
cache.insert_one({'my-second-document': 'yeeyy'})
...
cache.flush()
```

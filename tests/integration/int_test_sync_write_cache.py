import copy
import time
from datetime import timedelta
from unittest import TestCase

from pymongo import MongoClient

from pymongo_write_cache import SyncWriteCache


class TestMongoDbSyncWriteCache(TestCase):

    def setUp(self) -> None:
        self.buffer_time = timedelta(seconds=20)
        self.buffer_size = 10

        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_client.drop_database('test_db')
        self.database = self.mongo_client['test_db']
        self.col = self.database['test_col']

        self.subject = SyncWriteCache(self.col, self.buffer_size, self.buffer_time)

    def test_insert_time_buffer(self):
        data = {'value1': 1, 'value2': [1, 2, 4]}
        # insert data
        self.subject.insert_one(copy.deepcopy(data))

        # immediately check if they are already in db (they should not be)
        docs = list(self.col.find())
        self.assertEqual(0, len(docs))

        # wait a bit longer than buffer time
        time.sleep(self.buffer_time.total_seconds() + 2)

        # check again if now document is in database
        docs = list(self.col.find())
        self.assertEqual(1, len(docs))
        del docs[0]['_id']
        self.assertDictEqual(data, docs[0])

    def test_insert_count_buffer(self):
        data = {'value1': 1, 'value2': [1, 2, 4]}
        for _ in range(self.buffer_size):
            self.subject.insert_one(copy.deepcopy(data))

        docs = list(self.col.find())
        self.assertEqual(self.buffer_size, len(docs))
        for doc in docs:
            del doc['_id']
            self.assertDictEqual(data, doc)

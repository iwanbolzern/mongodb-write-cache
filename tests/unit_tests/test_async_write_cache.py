import time
from datetime import timedelta
from threading import Event
from unittest import TestCase
from unittest.mock import Mock

from pymongo_write_cache import AsyncWriteCache


class TestAsyncCache(TestCase):

    def setUp(self) -> None:
        self.collection = Mock()

        self.subject = AsyncWriteCache(self.collection, 10, timedelta(seconds=5))

    def test_insert_many_time_based(self):
        data = self._gen_data(6)
        event = Event()
        self.subject.flush_callback.append(event.set)
        self.subject.insert_many(data[:2])
        self.subject.insert_many(data[2:4])
        self.subject.insert_many(data[4:6])

        self.collection.insert_many.assert_not_called()
        event.wait(7)
        self.collection.insert_many.assert_called_once_with(data)

    def test_insert_many_count_based(self):
        data = self._gen_data(10)
        event = Event()
        self.subject.flush_callback.append(event.set)
        self.subject.insert_many(data)

        event.wait(2)
        self.collection.insert_many.assert_called_once_with(data)

    def test_insert(self):
        data = self._gen_data(10)
        event = Event()
        self.subject.flush_callback.append(event.set)
        for item in data:
            self.subject.insert_one(item)

        event.wait(2)
        self.collection.insert_many.assert_called_once_with(data)

    def test_flush(self):
        data = self._gen_data(6)
        self.subject.insert_many(data)
        self.subject.flush()
        self.collection.insert_many.assert_called_once_with(data)

    def _gen_data(self, count):
        return [{'test': i} for i in range(count)]

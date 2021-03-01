from collections import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable


class Caller:
    """ An object were callbacks can be registered and called """

    def __init__(self):
        self.callbacks = []
        self.thread_pool = ThreadPoolExecutor()

    def append(self, cb: Callable):
        """Register cb as a new callback. Will not register duplicates.
        :param cb: callback to be registered
        """
        if cb not in self.callbacks:
            self.callbacks.append(cb)

    def remove(self, cb: Callable):
        """Un-register cb from the callbacks
        :param cb: cb to remove from the list of callbacks
        """
        self.callbacks.remove(cb)

    def call(self, *args, **kwargs):
        """Call the callbacks registered with the given args and kwargs
        :param args: positional arguments passed to the callback functions
        :param kwargs: key word arguments passed to the callback functions
        """
        for cb in self.callbacks:
            cb(*args, **kwargs)

    def call_async(self, *args, **kwargs):
        """Call the callbacks registered with the given args and kwargs in the context of
         a ThreadPoolExecutor
         :param args:
         :param kwargs:
         :return: """
        for cb in self.callbacks:
            future = self.thread_pool.submit(cb, *args, **kwargs)
            future.add_done_callback(lambda f: f.result())  # bring error in caller thread

    def __iter__(self) -> Iterable[Callable]:
        return self.callbacks.__iter__()

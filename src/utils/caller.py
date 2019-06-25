from concurrent.futures import ThreadPoolExecutor


class Caller:
    """ An object were callbacks can be registered and called """

    def __init__(self):
        """ Create the object """
        self.callbacks = []
        self.thread_pool = ThreadPoolExecutor()

    def append(self, cb):
        """ Register cb as a new callback. Will not register duplicates. """
        if cb not in self.callbacks:
            self.callbacks.append(cb)

    def remove(self, cb):
        """ Un-register cb from the callbacks """
        self.callbacks.remove(cb)

    def call(self, *args):
        """ Call the callbacks registered with the arguments args """
        for cb in self.callbacks:
            cb(*args)

    def call_async(self, *args):
        """ Call the callbacks registered with the arguments args """
        for cb in self.callbacks:
            self.thread_pool.submit(cb, *args)

    def __iter__(self):
        return self.callbacks.__iter__()

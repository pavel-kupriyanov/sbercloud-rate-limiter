from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Optional
from threading import Timer, Lock


class RepeatableTimer:
    seconds: int
    running: bool
    _timer: Optional[Timer]
    _callback: Callable

    def __init__(self, seconds: int, callback: Callable):
        self.seconds = seconds
        self.running = False
        self._timer = None
        self._callback = callback

    def callback(self):
        if self.running:
            self._callback()
        self.running = False
        self.start()

    def start(self):
        if not self.running:
            self._timer = Timer(self.seconds, self.callback)
            self._timer.start()
            self.running = True

    def stop(self):
        self.running = False
        if self._timer:
            self._timer.cancel()


class Limiter:
    counter: int
    limit: int
    lock: Lock

    def __init__(self, limit: int):
        self.counter = 0
        self.limit = limit
        self.lock = Lock()

    def __enter__(self):
        if self.counter + 1 >= self.limit:
            self.lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.counter += 1

    def reset(self):
        self.counter = 0
        if self.lock.locked():
            self.lock.release()


class RateLimitedExecutor(ThreadPoolExecutor):
    interval: int = 60
    limiter: Limiter
    timer: RepeatableTimer

    def __init__(self, max_per_interval: int, max_workers=None,
                 thread_name_prefix='', initializer=None, initargs=()):
        super().__init__(max_workers, thread_name_prefix, initializer, initargs)
        self.limiter = Limiter(max_per_interval)
        self.timer = RepeatableTimer(self.interval, self.limiter.reset)

    def submit(self, __fn, *args, **kwargs) -> Future:
        if not self.timer.running:
            self.timer.start()

        with self.limiter:
            return super().submit(__fn, *args, **kwargs)

    def shutdown(self, wait: bool = True):
        self.timer.stop()
        return super().shutdown(wait)

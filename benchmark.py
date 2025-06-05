"""
Get faster write throughout using Python client for FoundationDB.
"""

from __future__ import annotations
from typing import Tuple, Optional, Any
import queue
from multiprocessing.pool import ThreadPool
import random
import time
import threading
import multiprocessing as mp

from shared.logger import LOGGER
import fdb
fdb.api_version(710)

mp.set_start_method('fork')  # Must be 'fork', not the default 'spawn'

LOGGER.setLevel("INFO")

THREADS_PER_PUT_PROCESS: int = 2
NUM_PUT_PROCESSES: int = 2
THREADS_PER_GET_PROCESS: int = 2
NUM_GET_PROCESSES: int = 2
DEFAULT_OFFSET: int = 8192
MAX_QUEUE_SIZE = 1024
START_BYTES = b''
END_BYTES = b'\xff'


def random_bytes(length: Optional[int] = None) -> bytes:
    """
    Generate a random bytes string.
    """
    digits = '0123456789abcdef'
    length = length or random.randint(16, 128)
    out: bytes = bytes(''.join(random.choice(digits) for _ in range(length)), encoding='utf8')
    return out


class FDB:
    """
    High-throughput interface to FoundationDB.
    """

    def __init__(
        self, 
        threads_per_get_process: int = THREADS_PER_GET_PROCESS, 
        num_get_processes: int = NUM_GET_PROCESSES, 
        threads_per_put_process: int = THREADS_PER_PUT_PROCESS, 
        num_put_processes: int = NUM_PUT_PROCESSES, 
        max_queue_size: int = MAX_QUEUE_SIZE, 
        start_processes: bool = True
    ) -> None:
        
        self.put_counter = 0
        self.mp_queue = mp.Queue(max_queue_size)
        self.get_processes = []
        self.put_processes = []
        self.key_value_dict: dict[bytes, bytes] = {}

        # Start put processes
        for _ in range(num_put_processes):
            process: mp.Process = mp.Process(
                target=self.create_put_process, 
                args=(self.mp_queue, threads_per_put_process,)
            )
            self.put_processes.append(process)
        if start_processes:
            self.start_processes()
        
        # Start read processes
        for _ in range(num_get_processes):
            process: mp.Process = mp.Process(
                target=self.create_get_process, 
                args=(self.mp_queue, threads_per_get_process,)
            )
            self.get_processes.append(process)
        if start_processes:
            self.start_processes()
    
    def start_processes(self) -> None:
        for process in self.put_processes:
            process.start()
        for process in self.get_processes:
            process.start()
    
    def __setitem__(self, key: bytes, value: bytes) -> None:
        self.mp_queue.put((key, value,))
    
    def __getitem__(self, key: bytes) -> None:
        pass

    def create_put_process(self, mp_queue: mp.Queue, threads_per_process: int) -> None:
        """Each process creates several threads to saturate the C client."""
        thread_queue: queue.Queue[Any] = queue.Queue(MAX_QUEUE_SIZE)
        db = fdb.open()
        thread_pool: list[threading.Thread] = []
        for _ in range(threads_per_process):
            thread: threading.Thread = threading.Thread(target=self.put_from_queue, args=(thread_queue, db,))
            thread.start()
            thread_pool.append(thread)
        while 1:
            obj: Any = mp_queue.get()
            if obj is None:
                break
            thread_queue.put(obj)

    def put_from_queue(self, thread_queue: queue.Queue[Any], db) -> None:
        """Actually put the key/value pair into FoundationDB."""
        while 1:
            obj: Any = thread_queue.get()
            if obj is None:
                break
            k, v = obj
            db[k] = v
    
    def first_key_after(self, db, key: bytes, offset: int = DEFAULT_OFFSET):
        x = db.get_range(
            key,
            fdb.KeySelector.first_greater_than(key) + offset,  # pyrefly: ignore # pylint: disable=no-member
        )
        return x[-1].key if x else None

    def skip_keys(
        self, db, offset: int = DEFAULT_OFFSET, max_keys: Optional[int] = None
    ):
        current_key = b""
        next_key = self.first_key_after(db, current_key, offset=offset)
        counter = 0
        yield next_key
        while current_key != next_key:
            counter += 1
            current_key = next_key
            next_key = self.first_key_after(db, current_key, offset=offset)
            yield current_key
            if max_keys and counter > max_keys:
                break

    def read_all(self):
        queue: queue.Queue[Tuple[bytes, bytes] | None] = mp.Queue()
        process: mp.Process = mp.Process(target=self.parallel_read, args=(queue,))
        process.start()
        while (item := queue.get()) is not None:
            yield item
        process.join()

    def parallel_read(
        self,
        yield_queue: mp.Queue,
        max_keys: Optional[int] = None,
        num_threads: Optional[int] = 32,
        increment: Optional[int] = DEFAULT_OFFSET,
    ):
        executor: ThreadPool = ThreadPool(num_threads)
        streaming_queue: queue.Queue[Any] = queue.Queue()
        db = fdb.open()

        def _get_range(current_key, next_key):
            for k, v in db.get_range(current_key, next_key):
                streaming_queue.put((k, v))
            streaming_queue.put(None)

        futures = []

        def _start_threads():
            current_key = b""
            for next_key in self.skip_keys(db, offset=increment, max_keys=max_keys):
                future = executor.apply_async(
                    _get_range,
                    (
                        current_key,
                        next_key,
                    ),
                )
                futures.append(future)
                current_key = next_key

        queueing_thread = threading.Thread(target=_start_threads)
        queueing_thread.start()

        endings = 0
        time.sleep(1)
        while (
            not all(future.ready() for future in futures)
            or endings < len(futures)
            or not queueing_thread.is_alive()
        ):
            thing: Any = streaming_queue.get()
            if thing is None:
                endings += 1
                continue
            key, value = thing
            yield_queue.put((key, value,))



def benchmark_writes(start_processes: bool = True) -> None:
    '''Test the throughput''' 
    f: FDB = FDB(start_processes=start_processes)
    counter = 0
    while 1:
        key: bytes = random_bytes(length=16)
        value: bytes = random_bytes(length=64)
        f.mp_queue.put((key, value))
        counter += 1
        if counter % 1000 == 0:
            LOGGER.info('counter: %s', counter)


def big_read()  -> None:
    f: FDB = FDB(start_processes=False)
    counter = 0
    for i in f.read_all():
        counter += 1
        if counter % 10000 == 0:
            LOGGER.info('read counter: %s: %s', counter, i)


if __name__ == '__main__':
    t = threading.Thread(target=big_read)
    t.start()
    # big_read()
    u = threading.Thread(target=benchmark_writes)
    u.start()
    # t.join()
    # big_read()
    f: FDB = FDB(start_processes=True)
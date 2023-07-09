# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=redefined-outer-name,arguments-renamed,fixme
"""Concurrency concepts that adapt to the shared memory support in the current runtime.

Performance-optimized concurrency in Python prefers `multiprocessing` to avoid the global
interpreter lock. However, this requires shared memory provided via mount at `/dev/shm`. This
is not provided in serverless runtimes. In environments where multiprocessing is not supported,
we fall back to multithreading.
"""
import logging
import multiprocessing
import multiprocessing.managers
import multiprocessing.synchronize
import threading
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import AbstractContextManager
from typing import (
    Any,
    Generic,
    Type,
    TypeVar,
    Union,
)

from typing_extensions import Self

from pyiceberg.typedef import RecursiveDict
from pyiceberg.utils.config import Config

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Synchronized(Generic[T], AbstractContextManager):  # type: ignore
    """A context manager that provides concurrency-safe access to a value."""

    value: T
    lock: threading.Lock

    def __init__(self, value: T, lock: threading.Lock):
        super().__init__()
        self.value = value
        self.lock = lock

    def __enter__(self) -> T:
        """Acquires a lock, allowing access to the wrapped value."""
        self.lock.acquire()
        return self.value

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Releases the lock, allowing other threads to access the value."""
        self.lock.release()


class ManagedExecutor(Executor):
    """An executor that provides synchronization."""

    def synchronized(self, value: T) -> Synchronized[T]:
        raise NotImplementedError


class ManagedThreadPoolExecutor(ThreadPoolExecutor, ManagedExecutor):
    """A thread pool executor that provides synchronization."""

    def __enter__(self) -> Self:
        """Returns the executor itself as a context manager."""
        super().__enter__()
        return self

    def synchronized(self, value: T) -> Synchronized[T]:
        lock = threading.Lock()
        return Synchronized(value, lock)


class ManagedProcessPoolExecutor(ProcessPoolExecutor, ManagedExecutor):
    """A process pool executor provides synchronization."""

    manager: multiprocessing.managers.SyncManager

    def __init__(self) -> None:
        super().__init__()
        self.manager = multiprocessing.Manager()

    def __enter__(self) -> Self:
        """Returns the executor itself as a context manager."""
        self.manager.__enter__()
        super().__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Exits the executor and the manager."""
        super().__exit__(exc_type, exc_value, traceback)
        self.manager.__exit__(exc_type, exc_value, traceback)

    def synchronized(self, value: T) -> Synchronized[T]:
        lock = self.manager.Lock()
        return Synchronized(value, lock)


def _get_executor_class(concurrency_mode: Union[str, RecursiveDict, None], mp_avail: bool) -> Type[Executor]:
    """Returns the executor class for the given concurrency mode."""
    if concurrency_mode == "process":
        return ProcessPoolExecutor
    if concurrency_mode == "thread":
        return ThreadPoolExecutor
    if concurrency_mode is None and mp_avail:
        return ProcessPoolExecutor
    if concurrency_mode is None and not mp_avail:
        logger.debug("Falling back to thread pool executor")
        return ThreadPoolExecutor

    raise ValueError(f"Invalid concurrency mode: {concurrency_mode}")


def _get_managed_executor_class(concurrency_mode: Union[str, RecursiveDict, None], mp_avail: bool) -> Type[ManagedExecutor]:
    """Returns the managed executor class for the given concurrency mode."""
    if concurrency_mode == "process":
        return ManagedProcessPoolExecutor
    if concurrency_mode == "thread":
        return ManagedThreadPoolExecutor
    if concurrency_mode is None and mp_avail:
        return ManagedProcessPoolExecutor
    if concurrency_mode is None and not mp_avail:
        logger.debug("Falling back to managed thread pool executor")
        return ManagedThreadPoolExecutor

    raise ValueError(f"Invalid concurrency mode: {concurrency_mode}")


def _mp_avail() -> "bool":
    """Returns whether multiprocessing is available."""
    try:
        with ProcessPoolExecutor() as executor:
            executor.map(logger.debug, ["Multi-processing available in current runtime"])
    except Exception as err:
        logger.debug("Multi-processing not available in current runtime: %s", err)
        return False

    return True


mp_avail = _mp_avail()

concurrency_mode: Union[str, RecursiveDict, None] = Config().config.get("concurrency-mode")

DynamicExecutor: Type[Executor] = _get_executor_class(concurrency_mode, mp_avail)

DynamicManagedExecutor: Type[ManagedExecutor] = _get_managed_executor_class(concurrency_mode, mp_avail)

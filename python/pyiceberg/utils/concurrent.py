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
"""Concurrency concepts that support multi-threading."""
import threading
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import AbstractContextManager
from typing import Any, Generic, TypeVar

from typing_extensions import Self

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

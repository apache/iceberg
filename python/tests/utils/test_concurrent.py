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
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Dict, Optional
from unittest import mock

from pyiceberg.utils.concurrent import (
    ManagedProcessPoolExecutor,
    ManagedThreadPoolExecutor,
    _concurrency_mode,
    _get_executor_class,
    _get_managed_executor_class,
)

AUTO_ENV: Dict[str, Optional[str]] = {}
PROCESS_ENV = {"PYICEBERG_CONCURRENCY_MODE": "process"}
THREAD_ENV = {"PYICEBERG_CONCURRENCY_MODE": "thread"}


@mock.patch.dict(os.environ, AUTO_ENV)
def test_configured_auto_concurrency() -> None:
    assert _concurrency_mode() == "thread"


@mock.patch.dict(os.environ, PROCESS_ENV)
def test_configured_process_concurrency() -> None:
    assert _concurrency_mode() == "process"


@mock.patch.dict(os.environ, THREAD_ENV)
def test_configured_thread_concurrency() -> None:
    assert _concurrency_mode() == "thread"


def test_select_process_executor() -> None:
    exec_cls = _get_executor_class("process")
    managed_exec_cls = _get_managed_executor_class("process")

    assert exec_cls == ProcessPoolExecutor
    assert managed_exec_cls == ManagedProcessPoolExecutor


def test_select_thread_executor() -> None:
    exec_cls = _get_executor_class("thread")
    managed_exec_cls = _get_managed_executor_class("thread")

    assert exec_cls == ThreadPoolExecutor
    assert managed_exec_cls == ManagedThreadPoolExecutor

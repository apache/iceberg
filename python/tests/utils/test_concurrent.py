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
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional
from unittest import mock

import pytest

from pyiceberg.utils.concurrent import ExecutorFactory

EMPTY_ENV: Dict[str, Optional[str]] = {}
VALID_ENV = {"PYICEBERG_MAX_WORKERS": "5"}
INVALID_ENV = {"PYICEBERG_MAX_WORKERS": "invalid"}


def test_create_reused() -> None:
    first = ExecutorFactory.get_or_create()
    second = ExecutorFactory.get_or_create()
    assert isinstance(first, ThreadPoolExecutor)
    assert first is second


@mock.patch.dict(os.environ, EMPTY_ENV)
def test_max_workers_none() -> None:
    assert ExecutorFactory.max_workers() is None


@mock.patch.dict(os.environ, VALID_ENV)
def test_max_workers() -> None:
    assert ExecutorFactory.max_workers() == 5


@mock.patch.dict(os.environ, INVALID_ENV)
def test_max_workers_invalid() -> None:
    with pytest.raises(ValueError):
        ExecutorFactory.max_workers()

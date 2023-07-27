import pytest
import os
from concurrent.futures import ThreadPoolExecutor
from unittest import mock
from pyiceberg.exceptions import InvalidConfigurationError
from pyiceberg.utils.concurrent import ExecutorFactory

EMPTY_ENV = {}
VALID_ENV = {"PYICEBERG_MAX_WORKERS": "5"}
INVALID_ENV = {"PYICEBERG_MAX_WORKERS": "invalid"}


def test_create_reused():
    first = ExecutorFactory.create()
    second = ExecutorFactory.create()
    assert isinstance(first, ThreadPoolExecutor)
    assert first is second


@mock.patch.dict(os.environ, EMPTY_ENV)
def test_max_workers_none():
    assert ExecutorFactory.max_workers() is None


@mock.patch.dict(os.environ, VALID_ENV)
def test_max_workers():
    assert ExecutorFactory.max_workers() == 5


@mock.patch.dict(os.environ, INVALID_ENV)
def test_max_workers_invalid():
    with pytest.raises(InvalidConfigurationError):
        ExecutorFactory.max_workers()

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
from unittest import mock

import yaml

from pyiceberg.utils.config import Config, _lowercase_dictionary_keys

EXAMPLE_ENV = {"PYICEBERG_CATALOG__PRODUCTION__URI": "https://service.io/api"}


def test_config() -> None:
    """To check if all the file lookups go well without any mocking"""
    assert Config()


@mock.patch.dict(os.environ, EXAMPLE_ENV)
def test_from_environment_variables() -> None:
    assert Config().get_catalog_config("production") == {"uri": "https://service.io/api"}


@mock.patch.dict(os.environ, EXAMPLE_ENV)
def test_from_environment_variables_uppercase() -> None:
    assert Config().get_catalog_config("PRODUCTION") == {"uri": "https://service.io/api"}


def test_from_configuration_files(tmp_path_factory) -> None:  # type: ignore
    config_path = str(tmp_path_factory.mktemp("config"))
    with open(f"{config_path}/.pyiceberg.yaml", "w", encoding="utf-8") as file:
        yaml.dump({"catalog": {"production": {"uri": "https://service.io/api"}}}, file)

    os.environ["PYICEBERG_HOME"] = config_path
    assert Config().get_catalog_config("production") == {"uri": "https://service.io/api"}


def test_lowercase_dictionary_keys() -> None:
    uppercase_keys = {"UPPER": {"NESTED_UPPER": {"YES"}}}
    expected = {"upper": {"nested_upper": {"YES"}}}
    assert _lowercase_dictionary_keys(uppercase_keys) == expected  # type: ignore

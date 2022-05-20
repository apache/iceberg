#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
from dataclasses import FrozenInstanceError
from unittest.mock import patch

import pytest

from iceberg.schema import Schema
from iceberg.table.base import TableBuilder

VALUE_ERROR_IDENTIFIER_NONE = "Table identifier has invalid value: None"
TEST_TABLE_IDENTIFIER = ("com", "org", "dept", "table")


def test_table_builder_raises_value_error_at_instantiation():
    with patch("iceberg.table.base.TableBuilder.__post_init__", side_effect=ValueError(VALUE_ERROR_IDENTIFIER_NONE)):
        with pytest.raises(ValueError, match=VALUE_ERROR_IDENTIFIER_NONE):
            TableBuilder(None, Schema(schema_id=1))


def test_table_builder_is_immutable():
    # Given
    table_builder = TableBuilder(TEST_TABLE_IDENTIFIER, Schema(schema_id=1))
    # Then
    with pytest.raises(FrozenInstanceError):
        # When
        table_builder.schema = Schema(schema_id=2)

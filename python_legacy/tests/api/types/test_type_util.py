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

from iceberg.api.types import (BooleanType, NestedField, StructType)
from iceberg.api.types import type_util
from iceberg.exceptions import ValidationException
import pytest


def test_invalid_schema_via_index_by_name():
    bool_type1 = NestedField.required(1, "a", BooleanType.get())
    bool_type2 = NestedField.required(2, "a", BooleanType.get())

    with pytest.raises(ValidationException) as context:
        type_util.index_by_name(StructType.of([bool_type1, bool_type2]))
    assert str(context.value) == 'Invalid schema: multiple fields for name a: 1 and 2'


def test_valid_schema_via_index_by_name():
    bool_type1 = NestedField.required(1, "a", BooleanType.get())
    bool_type2 = NestedField.required(2, "b", BooleanType.get())

    assert {'a': 1, 'b': 2} == type_util.index_by_name(StructType.of([bool_type1, bool_type2]))


def test_validate_schema_via_index_by_name_for_nested_type():
    nested_type = NestedField.required(
        1, "a", StructType.of(
            [NestedField.required(2, "b", StructType.of(
                [NestedField.required(3, "c", BooleanType.get())])),
             NestedField.required(4, "b.c", BooleanType.get())]))

    with pytest.raises(ValidationException) as context:
        type_util.index_by_name(StructType.of([nested_type]))
    assert str(context.value) == 'Invalid schema: multiple fields for name a.b.c: 3 and 4'

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

import uuid
from decimal import Decimal

import pytest

from iceberg.expressions import base
from iceberg.types import (
    BinaryType,
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    StringType,
    UUIDType,
)


@pytest.mark.parametrize(
    "operation,opposite_operation",
    [
        (base.Operation.TRUE, base.Operation.FALSE),
        (base.Operation.FALSE, base.Operation.TRUE),
        (base.Operation.IS_NULL, base.Operation.NOT_NULL),
        (base.Operation.NOT_NULL, base.Operation.IS_NULL),
        (base.Operation.IS_NAN, base.Operation.NOT_NAN),
        (base.Operation.NOT_NAN, base.Operation.IS_NAN),
        (base.Operation.LT, base.Operation.GT_EQ),
        (base.Operation.LT_EQ, base.Operation.GT),
        (base.Operation.GT, base.Operation.LT_EQ),
        (base.Operation.GT_EQ, base.Operation.LT),
        (base.Operation.EQ, base.Operation.NOT_EQ),
        (base.Operation.NOT_EQ, base.Operation.EQ),
        (base.Operation.IN, base.Operation.NOT_IN),
        (base.Operation.NOT_IN, base.Operation.IN),
    ],
)
def test_negation_of_operations(operation, opposite_operation):
    assert operation.negate() == opposite_operation


@pytest.mark.parametrize(
    "operation",
    [
        base.Operation.NOT,
        base.Operation.AND,
        base.Operation.OR,
    ],
)
def test_raise_on_no_negation_for_operation(operation):
    with pytest.raises(ValueError) as exc_info:
        operation.negate()

    assert str(exc_info.value) == f"No negation defined for operation {operation}"


def test_accessor_base_class(foo_struct):
    """Test retrieving a value at a position of a container using an accessor"""

    uuid_value = uuid.uuid4()

    foo_struct.set(0, "foo")
    foo_struct.set(1, "bar")
    foo_struct.set(2, "baz")
    foo_struct.set(3, 1)
    foo_struct.set(4, 2)
    foo_struct.set(5, 3)
    foo_struct.set(6, 1.234)
    foo_struct.set(7, Decimal("1.234"))
    foo_struct.set(8, uuid_value)
    foo_struct.set(9, True)
    foo_struct.set(10, False)
    foo_struct.set(11, b"\x19\x04\x9e?")

    assert base.Accessor(position=0, iceberg_type=StringType).get(foo_struct) == "foo"
    assert base.Accessor(position=1, iceberg_type=StringType).get(foo_struct) == "bar"
    assert base.Accessor(position=2, iceberg_type=StringType).get(foo_struct) == "baz"
    assert base.Accessor(position=3, iceberg_type=IntegerType).get(foo_struct) == 1
    assert base.Accessor(position=4, iceberg_type=IntegerType).get(foo_struct) == 2
    assert base.Accessor(position=5, iceberg_type=IntegerType).get(foo_struct) == 3
    assert base.Accessor(position=6, iceberg_type=FloatType).get(foo_struct) == 1.234
    assert base.Accessor(position=7, iceberg_type=DecimalType).get(foo_struct) == Decimal("1.234")
    assert base.Accessor(position=8, iceberg_type=UUIDType).get(foo_struct) == uuid_value
    assert base.Accessor(position=9, iceberg_type=BooleanType).get(foo_struct) == True
    assert base.Accessor(position=10, iceberg_type=BooleanType).get(foo_struct) == False
    assert base.Accessor(position=11, iceberg_type=BinaryType).get(foo_struct) == b"\x19\x04\x9e?"

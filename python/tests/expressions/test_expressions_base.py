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
from typing import Any, Union

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


class FooStruct:
    """An example of an object that abides by StructProtocol"""

    def __init__(self):
        self.content = {}

    def get(self, pos: int) -> Any:
        return self.content[pos]

    def set(self, pos: int, value) -> None:
        self.content[pos] = value


class FooAccessor(base.Accessor[FooStruct]):
    """An accessor for FooStruct objects"""

    def get(self, container: FooStruct) -> Union[bool, bytes, float, int, str, Decimal, uuid.UUID]:
        return container.get(self.position)


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


def test_accessor_base_class():
    """Test retrieving a value at a position of a container using an accessor"""

    uuid_value = uuid.uuid4()

    container = FooStruct()
    container.set(0, "foo")
    container.set(1, "bar")
    container.set(2, "baz")
    container.set(3, 1)
    container.set(4, 2)
    container.set(5, 3)
    container.set(6, 1.234)
    container.set(7, Decimal("1.234"))
    container.set(8, uuid_value)
    container.set(9, True)
    container.set(10, False)
    container.set(11, b"\x19\x04\x9e?")

    assert FooAccessor(position=0, iceberg_type=StringType).get(container) == "foo"
    assert FooAccessor(position=1, iceberg_type=StringType).get(container) == "bar"
    assert FooAccessor(position=2, iceberg_type=StringType).get(container) == "baz"
    assert FooAccessor(position=3, iceberg_type=IntegerType).get(container) == 1
    assert FooAccessor(position=4, iceberg_type=IntegerType).get(container) == 2
    assert FooAccessor(position=5, iceberg_type=IntegerType).get(container) == 3
    assert FooAccessor(position=6, iceberg_type=FloatType).get(container) == 1.234
    assert FooAccessor(position=7, iceberg_type=DecimalType).get(container) == Decimal("1.234")
    assert FooAccessor(position=8, iceberg_type=UUIDType).get(container) == uuid_value
    assert FooAccessor(position=9, iceberg_type=BooleanType).get(container) == True
    assert FooAccessor(position=10, iceberg_type=BooleanType).get(container) == False
    assert FooAccessor(position=11, iceberg_type=BinaryType).get(container) == b"\x19\x04\x9e?"

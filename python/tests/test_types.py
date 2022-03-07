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

import pytest

from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

non_parameterized_types = [
    (1, BooleanType),
    (2, IntegerType),
    (3, LongType),
    (4, FloatType),
    (5, DoubleType),
    (6, DateType),
    (7, TimeType),
    (8, TimestampType),
    (9, TimestamptzType),
    (10, StringType),
    (11, UUIDType),
    (12, BinaryType),
]


@pytest.mark.parametrize("input_index, input_type", non_parameterized_types)
def test_repr_primitive_types(input_index, input_type):
    assert isinstance(eval(repr(input_type())), input_type)


@pytest.mark.parametrize(
    "input_type, result",
    [
        (BooleanType(), True),
        (IntegerType(), True),
        (LongType(), True),
        (FloatType(), True),
        (DoubleType(), True),
        (DateType(), True),
        (TimeType(), True),
        (TimestampType(), True),
        (TimestamptzType(), True),
        (StringType(), True),
        (UUIDType(), True),
        (BinaryType(), True),
        (DecimalType(32, 3), True),
        (FixedType(8), True),
        (ListType(1, StringType(), True), False),
        (
            MapType(1, StringType(), 2, IntegerType(), False),
            False,
        ),
        (
            StructType(
                NestedField(1, "required_field", StringType(), is_optional=False),
                NestedField(2, "optional_field", IntegerType(), is_optional=True),
            ),
            False,
        ),
        (NestedField(1, "required_field", StringType(), is_optional=False), False),
    ],
)
def test_is_primitive(input_type, result):
    assert input_type.is_primitive == result


def test_fixed_type():
    type_var = FixedType(length=5)
    assert type_var.length == 5
    assert str(type_var) == "fixed[5]"
    assert repr(type_var) == "FixedType(length=5)"
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == FixedType(5)
    assert type_var != FixedType(6)


def test_decimal_type():
    type_var = DecimalType(precision=9, scale=2)
    assert type_var.precision == 9
    assert type_var.scale == 2
    assert str(type_var) == "decimal(9, 2)"
    assert repr(type_var) == "DecimalType(precision=9, scale=2)"
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == DecimalType(9, 2)
    assert type_var != DecimalType(9, 3)


def test_struct_type():
    type_var = StructType(
        NestedField(1, "optional_field", IntegerType(), is_optional=True),
        NestedField(2, "required_field", FixedType(5), is_optional=False),
        NestedField(
            3,
            "required_field",
            StructType(
                NestedField(4, "optional_field", DecimalType(8, 2), is_optional=True),
                NestedField(5, "required_field", LongType(), is_optional=False),
            ),
            is_optional=False,
        ),
    )
    assert len(type_var.fields) == 3
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == eval(repr(type_var))
    assert type_var != StructType(NestedField(1, "optional_field", IntegerType(), is_optional=True))


def test_list_type():
    type_var = ListType(
        1,
        StructType(
            NestedField(2, "optional_field", DecimalType(8, 2), is_optional=True),
            NestedField(3, "required_field", LongType(), is_optional=False),
        ),
        False,
    )
    assert isinstance(type_var.element.type, StructType)
    assert len(type_var.element.type.fields) == 2
    assert type_var.element.field_id == 1
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == eval(repr(type_var))
    assert type_var != ListType(
        1,
        StructType(
            NestedField(2, "optional_field", DecimalType(8, 2), is_optional=True),
        ),
        True,
    )


def test_map_type():
    type_var = MapType(1, DoubleType(), 2, UUIDType(), False)
    assert isinstance(type_var.key.type, DoubleType)
    assert type_var.key.field_id == 1
    assert isinstance(type_var.value.type, UUIDType)
    assert type_var.value.field_id == 2
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == eval(repr(type_var))
    assert type_var != MapType(1, LongType(), 2, UUIDType(), False)
    assert type_var != MapType(1, DoubleType(), 2, StringType(), True)


def test_nested_field():
    field_var = NestedField(
        1,
        "optional_field1",
        StructType(
            NestedField(
                2,
                "optional_field2",
                ListType(
                    3,
                    DoubleType(),
                    element_is_optional=False,
                ),
                is_optional=True,
            ),
        ),
        is_optional=True,
    )
    assert field_var.is_optional
    assert not field_var.is_required
    assert field_var.field_id == 1
    assert isinstance(field_var.type, StructType)
    assert str(field_var) == str(eval(repr(field_var)))


@pytest.mark.parametrize("input_index,input_type", non_parameterized_types)
@pytest.mark.parametrize("check_index,check_type", non_parameterized_types)
def test_non_parameterized_type_equality(input_index, input_type, check_index, check_type):
    if input_index == check_index:
        assert input_type() == check_type()
    else:
        assert input_type() != check_type()

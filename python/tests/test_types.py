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

from iceberg.types import (BinaryType, BooleanType, DateType, DecimalType, DoubleType, FixedType,
                           FloatType, IntegerType, ListType, LongType, MapType, NestedField, StringType,
                           StructType, TimestampType, TimestamptzType, TimeType, UUIDType)
import pytest


@pytest.mark.parametrize("input_type",
                         [BooleanType, IntegerType, LongType, FloatType, DoubleType, DateType, TimeType,
                          TimestampType, TimestamptzType, StringType, UUIDType, BinaryType])
def test_repr_primitive_types(input_type):
    assert input_type == eval(repr(input_type))


def test_fixed_type():
    type_var = FixedType(5)
    assert type_var.length == 5
    assert str(type_var) == str(eval(repr(type_var)))


def test_decimal_type():
    type_var = DecimalType(9, 2)
    assert type_var.precision == 9
    assert type_var.scale == 2
    assert str(type_var) == str(eval(repr(type_var)))


def test_struct_type():
    type_var = StructType([NestedField(True, 1, "optional_field", IntegerType),
                           NestedField(False, 2, "required_field", FixedType(5)),
                           NestedField(False, 3, "required_field", StructType([
                               NestedField(True, 4, "optional_field", DecimalType(8, 2)),
                               NestedField(False, 5, "required_field", LongType)]))])
    assert len(type_var.fields) == 3
    assert str(type_var) == str(eval(repr(type_var)))


def test_list_type():
    type_var = ListType(NestedField(False, 1, "required_field", StructType([
        NestedField(True, 2, "optional_field", DecimalType(8, 2)),
        NestedField(False, 3, "required_field", LongType)])))
    assert isinstance(type_var.element.type, StructType)
    assert len(type_var.element.type.fields) == 2
    assert type_var.element.field_id == 1
    assert str(type_var) == str(eval(repr(type_var)))


def test_map_type():
    type_var = MapType(NestedField(True, 1, "optional_field", DoubleType),
                       NestedField(False, 2, "required_field", UUIDType))
    assert type_var.key.type == DoubleType
    assert type_var.key.field_id == 1
    assert type_var.value.type == UUIDType
    assert type_var.value.field_id == 2
    assert str(type_var) == str(eval(repr(type_var)))


def test_nested_field():
    field_var = NestedField(True, 1, "optional_field1", StructType([
        NestedField(True, 2, "optional_field2", ListType(
            NestedField(False, 3, "required_field3", DoubleType))),
        NestedField(False, 4, "required_field4", MapType(
            NestedField(True, 5, "optional_field5", TimeType),
            NestedField(False, 6, "required_field6", UUIDType)))]))
    assert field_var.is_optional
    assert not field_var.is_required
    assert field_var.field_id == 1
    assert isinstance(field_var.type, StructType)
    assert str(field_var) == str(eval(repr(field_var)))

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
# pylint: disable=W0123,W0613

import pytest

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
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
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel

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
                NestedField(1, "required_field", StringType(), required=False),
                NestedField(2, "optional_field", IntegerType(), required=True),
            ),
            False,
        ),
        (NestedField(1, "required_field", StringType(), required=False), False),
    ],
)
def test_is_primitive(input_type, result):
    assert input_type.is_primitive == result


def test_fixed_type():
    type_var = FixedType(length=5)
    assert len(type_var) == 5
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
        NestedField(1, "optional_field", IntegerType(), required=True),
        NestedField(2, "required_field", FixedType(5), required=False),
        NestedField(
            3,
            "required_field",
            StructType(
                NestedField(4, "optional_field", DecimalType(8, 2), required=True),
                NestedField(5, "required_field", LongType(), required=False),
            ),
            required=False,
        ),
    )
    assert len(type_var.fields) == 3
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == eval(repr(type_var))
    assert type_var != StructType(NestedField(1, "optional_field", IntegerType(), required=True))


def test_list_type():
    type_var = ListType(
        1,
        StructType(
            NestedField(2, "optional_field", DecimalType(8, 2), required=True),
            NestedField(3, "required_field", LongType(), required=False),
        ),
        False,
    )
    assert isinstance(type_var.element_field.field_type, StructType)
    assert len(type_var.element_field.field_type.fields) == 2
    assert type_var.element_field.field_id == 1
    assert str(type_var) == str(eval(repr(type_var)))
    assert type_var == eval(repr(type_var))
    assert type_var != ListType(
        1,
        StructType(
            NestedField(2, "optional_field", DecimalType(8, 2), required=True),
        ),
        True,
    )


def test_map_type():
    type_var = MapType(1, DoubleType(), 2, UUIDType(), False)
    assert isinstance(type_var.key_field.field_type, DoubleType)
    assert type_var.key_field.field_id == 1
    assert isinstance(type_var.value_field.field_type, UUIDType)
    assert type_var.value_field.field_id == 2
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
                    element_required=False,
                ),
                required=True,
            ),
        ),
        required=True,
    )
    assert field_var.required
    assert not field_var.optional
    assert field_var.field_id == 1
    assert isinstance(field_var.field_type, StructType)
    assert str(field_var) == str(eval(repr(field_var)))


@pytest.mark.parametrize("input_index,input_type", non_parameterized_types)
@pytest.mark.parametrize("check_index,check_type", non_parameterized_types)
def test_non_parameterized_type_equality(input_index, input_type, check_index, check_type):
    if input_index == check_index:
        assert input_type() == check_type()
    else:
        assert input_type() != check_type()


# Examples based on https://iceberg.apache.org/spec/#appendix-c-json-serialization


class IcebergTestType(IcebergBaseModel):
    __root__: IcebergType


def test_serialization_boolean():
    assert BooleanType().json() == '"boolean"'


def test_deserialization_boolean():
    assert IcebergTestType.parse_raw('"boolean"') == BooleanType()


def test_str_boolean():
    assert str(BooleanType()) == "boolean"


def test_repr_boolean():
    assert repr(BooleanType()) == "BooleanType()"


def test_serialization_int():
    assert IntegerType().json() == '"int"'


def test_deserialization_int():
    assert IcebergTestType.parse_raw('"int"') == IntegerType()


def test_str_int():
    assert str(IntegerType()) == "int"


def test_repr_int():
    assert repr(IntegerType()) == "IntegerType()"


def test_serialization_long():
    assert LongType().json() == '"long"'


def test_deserialization_long():
    assert IcebergTestType.parse_raw('"long"') == LongType()


def test_str_long():
    assert str(LongType()) == "long"


def test_repr_long():
    assert repr(LongType()) == "LongType()"


def test_serialization_float():
    assert FloatType().json() == '"float"'


def test_deserialization_float():
    assert IcebergTestType.parse_raw('"float"') == FloatType()


def test_str_float():
    assert str(FloatType()) == "float"


def test_repr_float():
    assert repr(FloatType()) == "FloatType()"


def test_serialization_double():
    assert DoubleType().json() == '"double"'


def test_deserialization_double():
    assert IcebergTestType.parse_raw('"double"') == DoubleType()


def test_str_double():
    assert str(DoubleType()) == "double"


def test_repr_double():
    assert repr(DoubleType()) == "DoubleType()"


def test_serialization_date():
    assert DateType().json() == '"date"'


def test_deserialization_date():
    assert IcebergTestType.parse_raw('"date"') == DateType()


def test_str_date():
    assert str(DateType()) == "date"


def test_repr_date():
    assert repr(DateType()) == "DateType()"


def test_serialization_time():
    assert TimeType().json() == '"time"'


def test_deserialization_time():
    assert IcebergTestType.parse_raw('"time"') == TimeType()


def test_str_time():
    assert str(TimeType()) == "time"


def test_repr_time():
    assert repr(TimeType()) == "TimeType()"


def test_serialization_timestamp():
    assert TimestampType().json() == '"timestamp"'


def test_deserialization_timestamp():
    assert IcebergTestType.parse_raw('"timestamp"') == TimestampType()


def test_str_timestamp():
    assert str(TimestampType()) == "timestamp"


def test_repr_timestamp():
    assert repr(TimestampType()) == "TimestampType()"


def test_serialization_timestamptz():
    assert TimestamptzType().json() == '"timestamptz"'


def test_deserialization_timestamptz():
    assert IcebergTestType.parse_raw('"timestamptz"') == TimestamptzType()


def test_str_timestamptz():
    assert str(TimestamptzType()) == "timestamptz"


def test_repr_timestamptz():
    assert repr(TimestamptzType()) == "TimestamptzType()"


def test_serialization_string():
    assert StringType().json() == '"string"'


def test_deserialization_string():
    assert IcebergTestType.parse_raw('"string"') == StringType()


def test_str_string():
    assert str(StringType()) == "string"


def test_repr_string():
    assert repr(StringType()) == "StringType()"


def test_serialization_uuid():
    assert UUIDType().json() == '"uuid"'


def test_deserialization_uuid():
    assert IcebergTestType.parse_raw('"uuid"') == UUIDType()


def test_str_uuid():
    assert str(UUIDType()) == "uuid"


def test_repr_uuid():
    assert repr(UUIDType()) == "UUIDType()"


def test_serialization_fixed():
    assert FixedType(22).json() == '"fixed[22]"'


def test_deserialization_fixed():
    fixed = IcebergTestType.parse_raw('"fixed[22]"')
    assert fixed == FixedType(22)

    inner = fixed.__root__
    assert isinstance(inner, FixedType)
    assert len(inner) == 22


def test_str_fixed():
    assert str(FixedType(22)) == "fixed[22]"


def test_repr_fixed():
    assert repr(FixedType(22)) == "FixedType(length=22)"


def test_serialization_binary():
    assert BinaryType().json() == '"binary"'


def test_deserialization_binary():
    assert IcebergTestType.parse_raw('"binary"') == BinaryType()


def test_str_binary():
    assert str(BinaryType()) == "binary"


def test_repr_binary():
    assert repr(BinaryType()) == "BinaryType()"


def test_serialization_decimal():
    assert DecimalType(19, 25).json() == '"decimal(19, 25)"'


def test_deserialization_decimal():
    decimal = IcebergTestType.parse_raw('"decimal(19, 25)"')
    assert decimal == DecimalType(19, 25)

    inner = decimal.__root__
    assert isinstance(inner, DecimalType)
    assert inner.precision == 19
    assert inner.scale == 25


def test_str_decimal():
    assert str(DecimalType(19, 25)) == "decimal(19, 25)"


def test_repr_decimal():
    assert repr(DecimalType(19, 25)) == "DecimalType(precision=19, scale=25)"


def test_serialization_nestedfield():
    expected = '{"id": 1, "name": "required_field", "type": "string", "required": true, "doc": "this is a doc"}'
    actual = NestedField(1, "required_field", StringType(), True, "this is a doc").json()
    assert expected == actual


def test_serialization_nestedfield_no_doc():
    expected = '{"id": 1, "name": "required_field", "type": "string", "required": true}'
    actual = NestedField(1, "required_field", StringType(), True).json()
    assert expected == actual


def test_str_nestedfield():
    assert str(NestedField(1, "required_field", StringType(), True)) == "1: required_field: required string"


def test_repr_nestedfield():
    assert (
        repr(NestedField(1, "required_field", StringType(), True))
        == "NestedField(field_id=1, name='required_field', field_type=StringType(), required=True)"
    )


def test_nestedfield_by_alias():
    # We should be able to initialize a NestedField by alias
    expected = NestedField(1, "required_field", StringType(), True, "this is a doc")
    actual = NestedField(**{"id": 1, "name": "required_field", "type": "string", "required": True, "doc": "this is a doc"})
    assert expected == actual


def test_deserialization_nestedfield():
    expected = NestedField(1, "required_field", StringType(), True, "this is a doc")
    actual = NestedField.parse_raw(
        '{"id": 1, "name": "required_field", "type": "string", "required": true, "doc": "this is a doc"}'
    )
    assert expected == actual


def test_deserialization_nestedfield_inner():
    expected = NestedField(1, "required_field", StringType(), True, "this is a doc")
    actual = IcebergTestType.parse_raw(
        '{"id": 1, "name": "required_field", "type": "string", "required": true, "doc": "this is a doc"}'
    )
    assert expected == actual.__root__


def test_serialization_struct():
    actual = StructType(
        NestedField(1, "required_field", StringType(), True, "this is a doc"), NestedField(2, "optional_field", IntegerType())
    ).json()
    expected = (
        '{"type": "struct", "fields": ['
        '{"id": 1, "name": "required_field", "type": "string", "required": true, "doc": "this is a doc"}, '
        '{"id": 2, "name": "optional_field", "type": "int", "required": true}'
        "]}"
    )
    assert actual == expected


def test_deserialization_struct():
    actual = StructType.parse_raw(
        """
    {
        "type": "struct",
        "fields": [{
                "id": 1,
                "name": "required_field",
                "type": "string",
                "required": true,
                "doc": "this is a doc"
            },
            {
                "id": 2,
                "name": "optional_field",
                "type": "int",
                "required": true
            }
        ]
    }
    """
    )

    expected = StructType(
        NestedField(1, "required_field", StringType(), True, "this is a doc"), NestedField(2, "optional_field", IntegerType())
    )

    assert actual == expected


def test_str_struct(simple_struct: StructType):
    assert str(simple_struct) == "struct<1: required_field: required string (this is a doc), 2: optional_field: required int>"


def test_repr_struct(simple_struct: StructType):
    assert (
        repr(simple_struct)
        == "StructType(fields=(NestedField(field_id=1, name='required_field', field_type=StringType(), required=True), NestedField(field_id=2, name='optional_field', field_type=IntegerType(), required=True),))"
    )


def test_serialization_list(simple_list: ListType):
    actual = simple_list.json()
    expected = '{"type": "list", "element-id": 22, "element": "string", "element-required": true}'
    assert actual == expected


def test_deserialization_list(simple_list: ListType):
    actual = ListType.parse_raw('{"type": "list", "element-id": 22, "element": "string", "element-required": true}')
    assert actual == simple_list


def test_str_list(simple_list: ListType):
    assert str(simple_list) == "list<string>"


def test_repr_list(simple_list: ListType):
    assert repr(simple_list) == "ListType(type='list', element_id=22, element_type=StringType(), element_required=True)"


def test_serialization_map(simple_map: MapType):
    actual = simple_map.json()
    expected = """{"type": "map", "key-id": 19, "key": "string", "value-id": 25, "value": "double", "value-required": false}"""

    assert actual == expected


def test_deserialization_map(simple_map: MapType):
    actual = MapType.parse_raw(
        """{"type": "map", "key-id": 19, "key": "string", "value-id": 25, "value": "double", "value-required": false}"""
    )
    assert actual == simple_map


def test_str_map(simple_map: MapType):
    assert str(simple_map) == "map<string, double>"


def test_repr_map(simple_map: MapType):
    assert (
        repr(simple_map)
        == "MapType(type='map', key_id=19, key_type=StringType(), value_id=25, value_type=DoubleType(), value_required=False)"
    )


def test_types_singleton():
    """The types are immutable so we can return the same instance multiple times"""
    assert id(BooleanType()) == id(BooleanType())
    assert id(FixedType(22)) == id(FixedType(22))
    assert id(FixedType(19)) != id(FixedType(25))

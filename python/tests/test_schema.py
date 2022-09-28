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

from textwrap import dedent
from typing import Any, Dict

import pytest

from pyiceberg import schema
from pyiceberg.avro.reader import DecimalReader
from pyiceberg.avro.resolver import resolve
from pyiceberg.exceptions import ValidationError
from pyiceberg.expressions.base import Accessor
from pyiceberg.files import StructProtocol
from pyiceberg.schema import (
    Schema,
    build_position_accessors,
    project,
    promote,
)
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


def test_schema_str(table_schema_simple: Schema):
    """Test casting a schema to a string"""
    assert str(table_schema_simple) == dedent(
        """\
    table {
      1: foo: optional string
      2: bar: required int
      3: baz: optional boolean
    }"""
    )


def test_schema_repr_single_field():
    """Test schema representation"""
    actual = repr(schema.Schema(NestedField(1, "foo", StringType()), schema_id=1))
    expected = "Schema(NestedField(field_id=1, name='foo', field_type=StringType(), required=True), schema_id=1, identifier_field_ids=[])"
    assert expected == actual


def test_schema_repr_two_fields():
    """Test schema representation"""
    actual = repr(
        schema.Schema(NestedField(1, "foo", StringType()), NestedField(2, "bar", IntegerType(), required=False), schema_id=1)
    )
    expected = "Schema(NestedField(field_id=1, name='foo', field_type=StringType(), required=True), NestedField(field_id=2, name='bar', field_type=IntegerType(), required=False), schema_id=1, identifier_field_ids=[])"
    assert expected == actual


def test_schema_raise_on_duplicate_names():
    """Test schema representation"""
    with pytest.raises(ValueError) as exc_info:
        schema.Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
            NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
            NestedField(field_id=4, name="baz", field_type=BooleanType(), required=False),
            schema_id=1,
            identifier_field_ids=[1],
        )

    assert "Invalid schema, multiple fields for name baz: 3 and 4" in str(exc_info.value)


def test_schema_index_by_id_visitor(table_schema_nested):
    """Test index_by_id visitor function"""
    index = schema.index_by_id(table_schema_nested)
    assert index == {
        1: NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        2: NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        3: NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        4: NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_required=True),
            required=True,
        ),
        5: NestedField(field_id=5, name="element", field_type=StringType(), required=True),
        6: NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(
                key_id=7,
                key_type=StringType(),
                value_id=8,
                value_type=MapType(key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True),
                value_required=True,
            ),
            required=True,
        ),
        7: NestedField(field_id=7, name="key", field_type=StringType(), required=True),
        9: NestedField(field_id=9, name="key", field_type=StringType(), required=True),
        8: NestedField(
            field_id=8,
            name="value",
            field_type=MapType(key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True),
            required=True,
        ),
        10: NestedField(field_id=10, name="value", field_type=IntegerType(), required=True),
        11: NestedField(
            field_id=11,
            name="location",
            field_type=ListType(
                element_id=12,
                element_type=StructType(
                    NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        12: NestedField(
            field_id=12,
            name="element",
            field_type=StructType(
                NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
                NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
            ),
            required=True,
        ),
        13: NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
        14: NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
        15: NestedField(
            field_id=15,
            name="person",
            field_type=StructType(
                NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
            ),
            required=False,
        ),
        16: NestedField(field_id=16, name="name", field_type=StringType(), required=False),
        17: NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
    }


def test_schema_index_by_name_visitor(table_schema_nested):
    """Test index_by_name visitor function"""
    index = schema.index_by_name(table_schema_nested)
    assert index == {
        "foo": 1,
        "bar": 2,
        "baz": 3,
        "qux": 4,
        "qux.element": 5,
        "quux": 6,
        "quux.key": 7,
        "quux.value": 8,
        "quux.value.key": 9,
        "quux.value.value": 10,
        "location": 11,
        "location.element": 12,
        "location.element.latitude": 13,
        "location.element.longitude": 14,
        "location.latitude": 13,
        "location.longitude": 14,
        "person": 15,
        "person.name": 16,
        "person.age": 17,
    }


def test_schema_find_column_name(table_schema_nested):
    """Test finding a column name using its field ID"""
    assert table_schema_nested.find_column_name(1) == "foo"
    assert table_schema_nested.find_column_name(2) == "bar"
    assert table_schema_nested.find_column_name(3) == "baz"
    assert table_schema_nested.find_column_name(4) == "qux"
    assert table_schema_nested.find_column_name(5) == "qux.element"
    assert table_schema_nested.find_column_name(6) == "quux"
    assert table_schema_nested.find_column_name(7) == "quux.key"
    assert table_schema_nested.find_column_name(8) == "quux.value"
    assert table_schema_nested.find_column_name(9) == "quux.value.key"
    assert table_schema_nested.find_column_name(10) == "quux.value.value"
    assert table_schema_nested.find_column_name(11) == "location"
    assert table_schema_nested.find_column_name(12) == "location.element"
    assert table_schema_nested.find_column_name(13) == "location.element.latitude"
    assert table_schema_nested.find_column_name(14) == "location.element.longitude"


def test_schema_find_column_name_on_id_not_found(table_schema_nested):
    """Test raising an error when a field ID cannot be found"""
    assert table_schema_nested.find_column_name(99) is None


def test_schema_find_column_name_by_id(table_schema_simple):
    """Test finding a column name given its field ID"""
    assert table_schema_simple.find_column_name(1) == "foo"
    assert table_schema_simple.find_column_name(2) == "bar"
    assert table_schema_simple.find_column_name(3) == "baz"


def test_schema_find_field_by_id(table_schema_simple):
    """Test finding a column using its field ID"""
    index = schema.index_by_id(table_schema_simple)

    column1 = index[1]
    assert isinstance(column1, NestedField)
    assert column1.field_id == 1
    assert column1.field_type == StringType()
    assert column1.required is False

    column2 = index[2]
    assert isinstance(column2, NestedField)
    assert column2.field_id == 2
    assert column2.field_type == IntegerType()
    assert column2.required is True

    column3 = index[3]
    assert isinstance(column3, NestedField)
    assert column3.field_id == 3
    assert column3.field_type == BooleanType()
    assert column3.required is False


def test_schema_find_field_by_id_raise_on_unknown_field(table_schema_simple):
    """Test raising when the field ID is not found among columns"""
    index = schema.index_by_id(table_schema_simple)
    with pytest.raises(Exception) as exc_info:
        _ = index[4]
    assert str(exc_info.value) == "4"


def test_schema_find_field_type_by_id(table_schema_simple):
    """Test retrieving a columns' type using its field ID"""
    index = schema.index_by_id(table_schema_simple)
    assert index[1] == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    assert index[2] == NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True)
    assert index[3] == NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False)


def test_index_by_id_schema_visitor(table_schema_nested):
    """Test the index_by_id function that uses the IndexById schema visitor"""
    assert schema.index_by_id(table_schema_nested) == {
        1: NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        2: NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        3: NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        4: NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_required=True),
            required=True,
        ),
        5: NestedField(field_id=5, name="element", field_type=StringType(), required=True),
        6: NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(
                key_id=7,
                key_type=StringType(),
                value_id=8,
                value_type=MapType(key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True),
                value_required=True,
            ),
            required=True,
        ),
        7: NestedField(field_id=7, name="key", field_type=StringType(), required=True),
        8: NestedField(
            field_id=8,
            name="value",
            field_type=MapType(key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True),
            required=True,
        ),
        9: NestedField(field_id=9, name="key", field_type=StringType(), required=True),
        10: NestedField(field_id=10, name="value", field_type=IntegerType(), required=True),
        11: NestedField(
            field_id=11,
            name="location",
            field_type=ListType(
                element_id=12,
                element_type=StructType(
                    NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        12: NestedField(
            field_id=12,
            name="element",
            field_type=StructType(
                NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
                NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
            ),
            required=True,
        ),
        13: NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
        14: NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
        15: NestedField(
            field_id=15,
            name="person",
            field_type=StructType(
                NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
            ),
            required=False,
        ),
        16: NestedField(field_id=16, name="name", field_type=StringType(), required=False),
        17: NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
    }


def test_index_by_id_schema_visitor_raise_on_unregistered_type():
    """Test raising a NotImplementedError when an invalid type is provided to the index_by_id function"""
    with pytest.raises(NotImplementedError) as exc_info:
        schema.index_by_id("foo")
    assert "Cannot visit non-type: foo" in str(exc_info.value)


def test_schema_find_field(table_schema_simple):
    """Test finding a field in a schema"""
    assert (
        table_schema_simple.find_field(1)
        == table_schema_simple.find_field("foo")
        == table_schema_simple.find_field("FOO", case_sensitive=False)
        == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    )
    assert (
        table_schema_simple.find_field(2)
        == table_schema_simple.find_field("bar")
        == table_schema_simple.find_field("BAR", case_sensitive=False)
        == NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True)
    )
    assert (
        table_schema_simple.find_field(3)
        == table_schema_simple.find_field("baz")
        == table_schema_simple.find_field("BAZ", case_sensitive=False)
        == NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False)
    )


def test_schema_find_type(table_schema_simple):
    """Test finding the type of a column given its field ID"""
    assert (
        table_schema_simple.find_type(1)
        == table_schema_simple.find_type("foo")
        == table_schema_simple.find_type("FOO", case_sensitive=False)
        == StringType()
    )
    assert (
        table_schema_simple.find_type(2)
        == table_schema_simple.find_type("bar")
        == table_schema_simple.find_type("BAR", case_sensitive=False)
        == IntegerType()
    )
    assert (
        table_schema_simple.find_type(3)
        == table_schema_simple.find_type("baz")
        == table_schema_simple.find_type("BAZ", case_sensitive=False)
        == BooleanType()
    )


def test_build_position_accessors(table_schema_nested):
    accessors = build_position_accessors(table_schema_nested)
    assert accessors == {
        1: Accessor(position=0, inner=None),
        2: Accessor(position=1, inner=None),
        3: Accessor(position=2, inner=None),
        4: Accessor(position=3, inner=None),
        6: Accessor(position=4, inner=None),
        11: Accessor(position=5, inner=None),
        16: Accessor(position=6, inner=Accessor(position=0, inner=None)),
        17: Accessor(position=6, inner=Accessor(position=1, inner=None)),
    }


def test_build_position_accessors_with_struct(table_schema_nested: Schema):
    class TestStruct(StructProtocol):
        def __init__(self, pos: Dict[int, Any] = EMPTY_DICT):
            self._pos: Dict[int, Any] = pos

        def set(self, pos: int, value) -> None:
            pass

        def get(self, pos: int) -> Any:
            return self._pos[pos]

    accessors = build_position_accessors(table_schema_nested)
    container = TestStruct({6: TestStruct({0: "name"})})
    inner_accessor = accessors.get(16)
    assert inner_accessor
    assert inner_accessor.get(container) == "name"


def test_serialize_schema(table_schema_simple: Schema):
    actual = table_schema_simple.json()
    expected = """{"type": "struct", "fields": [{"id": 1, "name": "foo", "type": "string", "required": false}, {"id": 2, "name": "bar", "type": "int", "required": true}, {"id": 3, "name": "baz", "type": "boolean", "required": false}], "schema-id": 1, "identifier-field-ids": [2]}"""
    assert actual == expected


def test_deserialize_schema(table_schema_simple: Schema):
    actual = Schema.parse_raw(
        """{"type": "struct", "fields": [{"id": 1, "name": "foo", "type": "string", "required": false}, {"id": 2, "name": "bar", "type": "int", "required": true}, {"id": 3, "name": "baz", "type": "boolean", "required": false}], "schema-id": 1, "identifier-field-ids": [2]}"""
    )
    expected = table_schema_simple
    assert actual == expected


def test_resolver_change_type():
    write_schema = Schema(
        NestedField(1, "properties", ListType(2, StringType())),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "properties", MapType(2, StringType(), 3, StringType())),
        schema_id=1,
    )

    with pytest.raises(ValidationError) as exc_info:
        resolve(write_schema, read_schema)

    assert "File/read schema are not aligned for list<string>, got map<string, string>" in str(exc_info.value)


def test_promote_int_to_long():
    assert promote(IntegerType(), LongType()) == LongType()


def test_promote_float_to_double():
    # We should still read floats, because it is encoded in 4 bytes
    assert promote(FloatType(), DoubleType()) == DoubleType()


def test_promote_decimal_to_decimal():
    # DecimalType(P, S) to DecimalType(P2, S) where P2 > P
    assert promote(DecimalType(19, 25), DecimalType(22, 25)) == DecimalType(22, 25)


def test_struct_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(StructType(), StringType())


def test_map_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(MapType(1, StringType(), 2, IntegerType()), StringType())


def test_primitive_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(IntegerType(), MapType(1, StringType(), 2, IntegerType()))


def test_integer_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(IntegerType(), StringType())


def test_float_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(FloatType(), StringType())


def test_string_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(StringType(), FloatType())


def test_binary_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(BinaryType(), FloatType())


def test_decimal_not_aligned():
    with pytest.raises(ValidationError):
        assert promote(DecimalType(22, 19), StringType())


def test_promote_decimal_to_decimal_reduce_precision():
    # DecimalType(P, S) to DecimalType(P2, S) where P2 > P
    with pytest.raises(ValidationError) as exc_info:
        _ = promote(DecimalType(19, 25), DecimalType(10, 25)) == DecimalReader(22, 25)

    assert "Cannot reduce precision from decimal(19, 25) to decimal(10, 25)" in str(exc_info.value)


def test_project_simple():
    projected = project(
        StructType(NestedField(1, "name", StringType(), required=True)),
        StructType(NestedField(1, "name", StringType(), required=True)),
    )

    assert projected == StructType(NestedField(1, "name", StringType(), required=True))


def test_project_simple_prune():
    projected = project(
        StructType(NestedField(1, "name", StringType(), required=True), NestedField(2, "age", IntegerType(), required=True)),
        StructType(NestedField(1, "name", StringType(), required=True)),
    )

    assert projected == StructType(NestedField(1, "name", StringType(), required=True))


def test_project_simple_id_mismatch():
    with pytest.raises(ValidationError) as exc_info:
        project(
            StructType(NestedField(1, "name", StringType(), required=True)),
            StructType(NestedField(2, "name", StringType(), required=True)),
        )

    assert "Could not find field 2: name: required string in struct<1: name: required string>" in str(exc_info.value)


def test_project_simple_rename():
    projected = project(
        StructType(NestedField(1, "name", StringType(), required=True)),
        StructType(NestedField(1, "first_name", StringType(), required=True)),
    )

    assert projected == StructType(NestedField(1, "first_name", StringType(), required=True))


def test_project_optional_to_required():
    with pytest.raises(ValidationError) as exc_info:
        project(
            StructType(NestedField(1, "name", StringType(), required=False)),
            StructType(NestedField(1, "name", StringType(), required=True)),
        )

    assert "1: name: optional string optional in the original schema, and required in the projected schema" in str(exc_info.value)


def test_project_complex_projection():
    projected = project(
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
            NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
            NestedField(
                field_id=4,
                name="qux",
                field_type=ListType(element_id=5, element_type=StringType(), element_required=True),
                required=True,
            ),
            NestedField(
                field_id=6,
                name="quux",
                field_type=MapType(
                    key_id=7,
                    key_type=StringType(),
                    value_id=8,
                    value_type=MapType(
                        key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True
                    ),
                    value_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=11,
                name="location",
                field_type=ListType(
                    element_id=12,
                    element_type=StructType(
                        NestedField(field_id=13, name="latitude", field_type=FloatType(), required=False),
                        NestedField(field_id=14, name="longitude", field_type=FloatType(), required=False),
                    ),
                    element_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=15,
                name="person",
                field_type=StructType(
                    NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                    NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
                ),
                required=False,
            ),
            schema_id=1,
            identifier_field_ids=[1],
        ),
        Schema(
            NestedField(field_id=1, name="foo", field_type=BinaryType(), required=False),  # Promoted to binary
            NestedField(field_id=2, name="bar", field_type=LongType(), required=True),  # Promoted to long
            NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
            NestedField(
                field_id=4,
                name="qux",
                field_type=ListType(element_id=5, element_type=StringType(), element_required=True),
                required=True,
            ),
            NestedField(
                field_id=6,
                name="quux",
                field_type=MapType(
                    key_id=7,
                    key_type=StringType(),
                    value_id=8,
                    value_type=MapType(
                        key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True
                    ),
                    value_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=15,
                name="person",
                field_type=StructType(
                    NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                    NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
                ),
                required=False,
            ),
            schema_id=1,
            identifier_field_ids=[1],
        ),
    )

    assert projected == StructType(
        fields=(
            NestedField(field_id=1, name="foo", field_type=BinaryType(), required=False),
            NestedField(field_id=2, name="bar", field_type=LongType(), required=True),
            NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
            NestedField(
                field_id=4,
                name="qux",
                field_type=ListType(type="list", element_id=5, element_type=StringType(), element_required=True),
                required=True,
            ),
            NestedField(
                field_id=6,
                name="quux",
                field_type=MapType(
                    type="map",
                    key_id=7,
                    key_type=StringType(),
                    value_id=8,
                    value_type=MapType(
                        type="map", key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_required=True
                    ),
                    value_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=15,
                name="person",
                field_type=StructType(
                    fields=(
                        NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                        NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
                    )
                ),
                required=False,
            ),
        )
    )

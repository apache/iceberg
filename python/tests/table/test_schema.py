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
from numpy import False_

from iceberg.table import schema
from iceberg.types import (
    BooleanType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


def test_schema_init():
    """Test initializing a schema from a list of fields"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)
    schema_struct = table_schema.struct

    assert table_schema.fields[0] == fields[0]
    assert table_schema.fields[1] == fields[1]
    assert table_schema.fields[2] == fields[2]
    assert table_schema.schema_id == 1
    assert isinstance(schema_struct, StructType)


def test_schema_str():
    """Test casting a schema to a string"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)
    assert """1: name=foo, type=string, required=True
2: name=bar, type=int, required=False
3: name=baz, type=boolean, required=True""" == str(
        table_schema
    )


@pytest.mark.parametrize(
    "schema, expected_repr",
    [
        (
            schema.Schema(fields=[NestedField(1, "foo", StringType())], schema_id=1),
            "Schema(fields=(NestedField(field_id=1, name='foo', field_type=StringType(), is_optional=True),), schema_id=1)",
        ),
        (
            schema.Schema(
                fields=[NestedField(1, "foo", StringType()), NestedField(2, "bar", IntegerType(), is_optional=False)], schema_id=2
            ),
            "Schema(fields=(NestedField(field_id=1, name='foo', field_type=StringType(), is_optional=True), NestedField(field_id=2, name='bar', field_type=IntegerType(), is_optional=False)), schema_id=2)",
        ),
    ],
)
def test_schema_repr(schema, expected_repr):
    """Test schema representation"""
    assert repr(schema) == expected_repr


def test_schema_find_field_id_case_sensitive():
    """Test case-sensitive retrieval of a field ID using the `find_field_id` method"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1, aliases={"qux": 1, "foobar": 2})
    assert table_schema.find_field_id_by_name(field_name="foo", case_sensitive=True) == 1
    assert table_schema.find_field_id_by_name(field_name="bar", case_sensitive=True) == 2
    assert table_schema.find_field_id_by_name(field_name="baz", case_sensitive=True) == 3
    assert table_schema.find_field_id_by_alias(field_alias="qux", case_sensitive=True) == 1
    assert table_schema.find_field_id_by_alias(field_alias="foobar", case_sensitive=True) == 2


def test_schema_find_field_id_case_insensitive():
    """Test case-insensitive retrieval of a field ID using the `find_field_id_by_name` and `find_field_id_by_alias methods"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1, aliases={"qux": 1, "foobar": 2})
    assert table_schema.find_field_id_by_name(field_name="fOO", case_sensitive=False) == 1
    assert table_schema.find_field_id_by_name(field_name="BAr", case_sensitive=False) == 2
    assert table_schema.find_field_id_by_name(field_name="BaZ", case_sensitive=False) == 3
    assert table_schema.find_field_id_by_alias(field_alias="qUx", case_sensitive=False) == 1
    assert table_schema.find_field_id_by_alias(field_alias="fooBAR", case_sensitive=False) == 2


def test_schema_find_field_name_by_field_id():
    """Test finding a field name using a field ID"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1, aliases={"qux": 1, "foobar": 2})
    assert table_schema.find_field_name_by_field_id(field_id=1) == "foo"
    assert table_schema.find_field_name_by_field_id(field_id=2) == "bar"
    assert table_schema.find_field_name_by_field_id(field_id=3) == "baz"


def test_schema_find_field_id_raise_on_not_found():
    """Test raising when the field ID for a given name or alias cannot be found"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1, aliases={"qux": 1, "foobar": 2})

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_name(field_name="name1")

    assert "Cannot get field ID, name not found: name1" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_name(field_name="name2", case_sensitive=False_)

    assert "Cannot get field ID, name not found: name2" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_name(
            field_name="case_insensitive_name1",
            case_sensitive=False,
        )

    assert "Cannot get field ID, name not found: case_insensitive_name1" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_name(field_name="case_insensitive_name2")

    assert "Cannot get field ID, name not found: case_insensitive_name2" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_alias(field_alias="alias1")

    assert "Cannot get field ID, alias not found: alias1" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_alias(field_alias="alias2")

    assert "Cannot get field ID, alias not found: alias2" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_alias(
            field_alias="case_insensitive_alias1",
            case_sensitive=False,
        )

    assert "Cannot get field ID, alias not found: case_insensitive_alias1" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_alias(
            field_alias="case_insensitive_alias2",
            case_sensitive=False,
        )

    assert "Cannot get field ID, alias not found: case_insensitive_alias2" in str(exc_info.value)


def test_schema_find_field_id_raise_on_multiple_case_insensitive_alias_match():
    """Test raising when a case-insensitive alias search returns multiple aliases"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1, aliases={"qux": 1, "QUX": 2})

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_id_by_alias(field_alias="qux", case_sensitive=False)

    assert "Cannot get field ID, case-insensitive alias returns multiple results: qux" in str(exc_info.value)


def test_schema_find_field_name_by_field_id_raise_on_unknown_field_id():
    """Test raising when the the field ID cannot be found while finding a field name"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1, aliases={"qux": 1, "foobar": 2})

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_name_by_field_id(field_id=4)

    assert "Cannot get field name, field ID not found: 4" in str(exc_info.value)


def test_schema_find_field_by_id():
    """Test retrieving a field using the field's ID"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)
    field1 = table_schema.find_field_by_id(field_id=1)
    field2 = table_schema.find_field_by_id(field_id=2)
    field3 = table_schema.find_field_by_id(field_id=3)

    assert isinstance(field1, NestedField)
    assert field1.field_id == 1
    assert field1.type == StringType()
    assert field1.is_optional == False
    assert isinstance(field2, NestedField)
    assert field2.field_id == 2
    assert field2.type == IntegerType()
    assert field2.is_optional == True
    assert isinstance(field3, NestedField)
    assert field3.field_id == 3
    assert field3.type == BooleanType()
    assert field3.is_optional == False


def test_schema_find_field_by_id_raise_on_unknown_field():
    """Test raising when the field ID is not found"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)

    with pytest.raises(ValueError) as exc_info:
        table_schema.find_field_by_id(field_id=4)

    assert "Cannot get field, ID does not exist: 4" in str(exc_info.value)


def test_schema_find_field_type():
    """Test retrieving a field's type using the field's ID"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)

    assert table_schema.find_field_type(field_id=1) == StringType()
    assert table_schema.find_field_type(field_id=2) == IntegerType()
    assert table_schema.find_field_type(field_id=3) == BooleanType()


def test_index_by_id_schema_visitor():
    """Test retrieving a field id to field name map using an IndexById schema visitor"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_is_optional=True),
            is_optional=False,
        ),
        NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(key_id=7, key_type=StringType(), value_id=8, value_type=IntegerType(), value_is_optional=True),
            is_optional=False,
        ),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)
    visitor = schema.IndexById()
    visitor.visit(table_schema)

    assert visitor.result == {1: "foo", 2: "bar", 3: "baz", 4: "qux", 5: "qux.element", 6: "quux", 7: "quux.key", 8: "quux.value"}


def test_index_by_name_schema_visitor():
    """Test retrieving a field name to field id map using an IndexByName schema visitor"""
    fields = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_is_optional=True),
            is_optional=False,
        ),
        NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(key_id=7, key_type=StringType(), value_id=8, value_type=IntegerType(), value_is_optional=True),
            is_optional=False,
        ),
    ]
    table_schema = schema.Schema(fields=fields, schema_id=1)
    visitor = schema.IndexByName()
    visitor.visit(table_schema)

    assert visitor.result == {"bar": 2, "baz": 3, "foo": 1, "qux": 4, "qux.element": 5, "quux": 6, "quux.key": 7, "quux.value": 8}


def test_index_by_id_schema_visitor_raise_on_unregistered_type():
    """Test raising a NotImplementedError when a type with no registered visit operation is passed to an IndexById visitor"""

    visitor = schema.IndexById()
    with pytest.raises(NotImplementedError) as exc_info:
        visitor.visit("foo")

    assert "Cannot visit node, no IndexById operation implemented for node type: <class 'str'>" in str(exc_info.value)


def test_index_by_name_schema_visitor_raise_on_unregistered_type():
    """Test raising a NotImplementedError when a type with no registered visit operation is passed to an IndexByName visitor"""

    visitor = schema.IndexByName()
    with pytest.raises(NotImplementedError) as exc_info:
        visitor.visit("foo")

    assert "Cannot visit node, no IndexByName operation implemented for node type: <class 'str'>" in str(exc_info.value)

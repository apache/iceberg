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

from iceberg.table import schema
from iceberg.types import (
    BooleanType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
)


def test_schema_str():
    """Test casting a schema to a string"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)
    assert """table { 
 1: foo: required string
 2: bar: optional int
 3: baz: required boolean
 }""" == str(
        table_schema
    )


@pytest.mark.parametrize(
    "schema, expected_repr",
    [
        (
            schema.Schema(NestedField(1, "foo", StringType())),
            "Schema(fields=(NestedField(field_id=1, name='foo', field_type=StringType(), is_optional=True),))",
        ),
        (
            schema.Schema(NestedField(1, "foo", StringType()), NestedField(2, "bar", IntegerType(), is_optional=False)),
            "Schema(fields=(NestedField(field_id=1, name='foo', field_type=StringType(), is_optional=True), NestedField(field_id=2, name='bar', field_type=IntegerType(), is_optional=False)))",
        ),
    ],
)
def test_schema_repr(schema, expected_repr):
    """Test schema representation"""
    assert repr(schema) == expected_repr


def test_schema_find_field_name_by_field_id():
    """Test finding a column name using its field ID"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)
    index = schema.index_by_id(table_schema)

    assert index[1].name == "foo"
    assert index[2].name == "bar"
    assert index[3].name == "baz"


def test_schema_find_field_by_id():
    """Test finding a column using its field ID"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)
    index = schema.index_by_id(table_schema)

    column1 = index[1]
    assert isinstance(column1, NestedField)
    assert column1.field_id == 1
    assert column1.type == StringType()
    assert column1.is_optional == False

    column2 = index[2]
    assert isinstance(column2, NestedField)
    assert column2.field_id == 2
    assert column2.type == IntegerType()
    assert column2.is_optional == True

    column3 = index[3]
    assert isinstance(column3, NestedField)
    assert column3.field_id == 3
    assert column3.type == BooleanType()
    assert column3.is_optional == False


def test_schema_find_field_by_id_raise_on_unknown_field():
    """Test raising when the field ID is not found among columns"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)
    index = schema.index_by_id(table_schema)

    with pytest.raises(Exception) as exc_info:
        index[4]

    assert str(exc_info.value) == "4"


def test_schema_find_field_type_by_id():
    """Test retrieving a columns's type using its field ID"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)
    index = schema.index_by_id(table_schema)

    assert index[1] == columns[0]
    assert index[2] == columns[1]
    assert index[3] == columns[2]


def test_index_by_id_schema_visitor():
    """Test the index_by_id function that uses the IndexById schema visitor"""
    columns = [
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
    table_schema = schema.Schema(*columns)

    assert schema.index_by_id(table_schema) == {
        1: NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        2: NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        3: NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        4: NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_is_optional=True),
            is_optional=False,
        ),
        5: NestedField(field_id=5, name="element", field_type=StringType(), is_optional=True),
        6: NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(key_id=7, key_type=StringType(), value_id=8, value_type=IntegerType(), value_is_optional=True),
            is_optional=False,
        ),
        7: NestedField(field_id=7, name="key", field_type=StringType(), is_optional=False),
        8: NestedField(field_id=8, name="value", field_type=IntegerType(), is_optional=True),
    }


def test_index_by_id_schema_visitor_raise_on_unregistered_type():
    """Test raising a NotImplementedError when an invalid type is provided to the index_by_id function"""

    with pytest.raises(NotImplementedError) as exc_info:
        schema.index_by_id("foo")

    assert "Cannot visit non-type: foo" in str(exc_info.value)


def test_schema_find_field():
    """Test finding a field in a schema"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)

    assert table_schema.find_field(1) == table_schema.find_field("foo") == columns[0]
    assert table_schema.find_field(2) == table_schema.find_field("bar") == columns[1]
    assert table_schema.find_field(3) == table_schema.find_field("baz") == columns[2]


def test_schema_find_type():
    """Test finding the type of a column given its field ID"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)

    assert table_schema.find_type(1) == table_schema.find_type("foo") == StringType()
    assert table_schema.find_type(2) == table_schema.find_type("bar") == IntegerType()
    assert table_schema.find_type(3) == table_schema.find_type("baz") == BooleanType()


def test_schema_find_column_name():
    """Test finding a column name given its field ID"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)

    assert table_schema.find_column_name(1) == "foo"
    assert table_schema.find_column_name(2) == "bar"
    assert table_schema.find_column_name(3) == "baz"


def test_schema_select():
    """Test selecting columns in a schema"""
    columns = [
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
    ]
    table_schema = schema.Schema(*columns)

    projected_schema = table_schema.select(["foo", "bar"])
    len(projected_schema.columns) == 2

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
# pylint:disable=redefined-outer-name
from typing import Dict

import pytest

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.schema import Schema, prune_columns
from pyiceberg.table import Table, UpdateSchema
from pyiceberg.types import (
    BooleanType,
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
    UUIDType, PrimitiveType, DateType, TimeType, TimestampType, TimestamptzType, BinaryType,
)


@pytest.fixture()
def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )



@pytest.fixture()
def simple_table(catalog: Catalog, table_schema_simple: Schema) -> Table:
    return _create_table_with_schema(catalog, table_schema_simple)



def test_add_column(simple_table: Table) -> None:
    update = UpdateSchema(simple_table)
    update.add_column(path="b", field_type=IntegerType())
    apply_schema: Schema = update._apply()  # pylint: disable=W0212
    assert len(apply_schema.fields) == 4

    assert apply_schema == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="b", field_type=IntegerType(), required=False),
    )
    assert apply_schema.schema_id == 0
    assert apply_schema.highest_field_id == 4


def test_add_primitive_type_column(simple_table: Table) -> None:
    primitive_type: Dict[str, PrimitiveType] = {
        "boolean": BooleanType(),
        "int": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "date": DateType(),
        "time": TimeType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "string": StringType(),
        "uuid": UUIDType(),
        "binary": BinaryType(),
    }

    for name, type_ in primitive_type.items():
        field_name = f"new_column_{name}"
        update = UpdateSchema(simple_table)
        update.add_column(path=field_name, field_type=type_, doc=f"new_column_{name}")
        new_schema = update._apply()  # pylint: disable=W0212

        field: NestedField = new_schema.find_field(field_name)
        assert field.field_type == type_
        assert field.doc == f"new_column_{name}"


def test_add_nested_type_column(simple_table: Table) -> None:
    # add struct type column
    field_name = "new_column_struct"
    update = UpdateSchema(simple_table)
    struct_ = StructType(
        NestedField(1, "lat", DoubleType()),
        NestedField(2, "long", DoubleType()),
    )
    update.add_column(path=field_name, field_type=struct_)
    schema_ = update._apply()  # pylint: disable=W0212
    field: NestedField = schema_.find_field(field_name)
    assert field.field_type == StructType(
        NestedField(5, "lat", DoubleType()),
        NestedField(6, "long", DoubleType()),
    )
    assert schema_.highest_field_id == 6


def test_add_nested_map_type_column(simple_table: Table) -> None:
    # add map type column
    field_name = "new_column_map"
    update = UpdateSchema(simple_table)
    map_ = MapType(1, StringType(), 2, IntegerType(), False)
    update.add_column(path=field_name, field_type=map_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == MapType(5, StringType(), 6, IntegerType(), False)
    assert new_schema.highest_field_id == 6


def test_add_nested_list_type_column(simple_table: Table) -> None:
    # add list type column
    field_name = "new_column_list"
    update = UpdateSchema(simple_table)
    list_ = ListType(
        element_id=101,
        element_type=StructType(
            NestedField(102, "lat", DoubleType()),
            NestedField(103, "long", DoubleType()),
        ),
        element_required=False,
    )
    update.add_column(path=field_name, field_type=list_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == ListType(
        element_id=5,
        element_type=StructType(
            NestedField(6, "lat", DoubleType()),
            NestedField(7, "long", DoubleType()),
        ),
        element_required=False,
    )
    assert new_schema.highest_field_id == 7


def test_add_field_to_map_key(catalog: Catalog, table_schema_nested_with_struct_key_map: Schema, table: Table) -> None:
    table = _create_table_with_schema(catalog, table_schema_nested_with_struct_key_map)
    with pytest.raises(ValueError) as exc_info:
        update = UpdateSchema(table)
        update.add_column(path=("location", "key", "b"), field_type=IntegerType())._apply()  # pylint: disable=W0212
    assert "Cannot add fields to map keys" in str(exc_info.value)


def _create_table_with_schema(catalog: Catalog, schema: Schema) -> Table:
    tbl_name = "default.test_schema_evolution"
    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier=tbl_name, schema=schema)


def test_add_already_exists(catalog: Catalog, table_schema_nested: Schema) -> None:
    table = _create_table_with_schema(catalog, table_schema_nested)
    update = UpdateSchema(table)

    with pytest.raises(ValueError) as exc_info:
        update.add_column("foo", IntegerType())
    assert "already exists: foo" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        update.add_column(path=("location", "latitude"), field_type=IntegerType())
    assert "already exists: location.latitude" in str(exc_info.value)


def test_ambiguous_column(catalog: Catalog, table_schema_nested: Schema) -> None:
    table = _create_table_with_schema(catalog, table_schema_nested)
    update = UpdateSchema(table)

    with pytest.raises(ValueError) as exc_info:
        update.add_column(path="location.latitude", field_type=IntegerType())
    assert "Cannot add column with ambiguous name: location.latitude, provide a tuple instead" in str(exc_info.value)


def test_add_to_non_struct_type(catalog: Catalog, table_schema_simple: Schema) -> None:
    table = _create_table_with_schema(catalog, table_schema_simple)
    update = UpdateSchema(table)
    with pytest.raises(ValueError) as exc_info:
        update.add_column(path=("foo", "lat"), field_type=IntegerType())
    assert "Cannot add column 'lat' to non-struct type" in str(exc_info.value)


def test_add_required_column(catalog: Catalog) -> None:
    schema_ = Schema(
        NestedField(field_id=1, name="a", field_type=BooleanType(), required=False), schema_id=1, identifier_field_ids=[]
    )
    table = _create_table_with_schema(catalog, schema_)
    update = UpdateSchema(table)
    with pytest.raises(ValueError) as exc_info:
        update.add_column(path="data", field_type=IntegerType(), required=True)
    assert "Incompatible change: cannot add required column: data" in str(exc_info.value)

    new_schema = (
        UpdateSchema(table)  # pylint: disable=W0212
        .allow_incompatible_changes()
        .add_column(path="data", field_type=IntegerType(), required=True)
        ._apply()
    )
    assert new_schema == Schema(
        NestedField(field_id=1, name="a", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="data", field_type=IntegerType(), required=True),
        schema_id=0,
        identifier_field_ids=[],
    )


def test_add_required_column_case_insensitive(catalog: Catalog) -> None:
    schema_ = Schema(
        NestedField(field_id=1, name="id", field_type=BooleanType(), required=False), schema_id=1, identifier_field_ids=[]
    )
    table = _create_table_with_schema(catalog, schema_)

    with pytest.raises(ValueError) as exc_info:
        update = UpdateSchema(table)
        update.allow_incompatible_changes().case_sensitive(False).add_column(path="ID", field_type=IntegerType(), required=True)
    assert "already exists: ID" in str(exc_info.value)

    new_schema = (
        UpdateSchema(table)  # pylint: disable=W0212
        .allow_incompatible_changes()
        .add_column(path="ID", field_type=IntegerType(), required=True)
        ._apply()
    )
    assert new_schema == Schema(
        NestedField(field_id=1, path="id", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="ID", field_type=IntegerType(), required=True),
        schema_id=0,
        identifier_field_ids=[],
    )


@pytest.mark.integration
def test_schema_evolution_via_transaction(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="col_uuid", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="col_fixed", field_type=FixedType(25), required=False),
    )
    tbl = _create_table_with_schema(catalog, schema)

    assert tbl.schema() == schema

    with tbl.transaction() as tx:
        tx.update_schema().add_column("col_string", StringType()).commit()

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="col_uuid", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="col_fixed", field_type=FixedType(25), required=False),
        NestedField(field_id=3, name="col_string", field_type=StringType(), required=False),
        schema_id=1,
    )

    tbl.update_schema().add_column("col_integer", IntegerType()).commit()

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="col_uuid", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="col_fixed", field_type=FixedType(25), required=False),
        NestedField(field_id=3, name="col_string", field_type=StringType(), required=False),
        NestedField(field_id=4, name="col_integer", field_type=IntegerType(), required=False),
        schema_id=1,
    )

    with pytest.raises(CommitFailedException) as exc_info:
        with tbl.transaction() as tx:
            # Start a new update
            schema_update = tx.update_schema()

            # Do a concurrent update
            tbl.update_schema().add_column("col_long", LongType()).commit()

            # stage another update in the transaction
            schema_update.add_column("col_double", DoubleType()).commit()

    assert "Requirement failed: current schema changed: expected id 2 != 3" in str(exc_info.value)

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="col_uuid", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="col_fixed", field_type=FixedType(25), required=False),
        NestedField(field_id=3, name="col_string", field_type=StringType(), required=False),
        NestedField(field_id=4, name="col_integer", field_type=IntegerType(), required=False),
        NestedField(field_id=5, name="col_long", field_type=LongType(), required=False),
        schema_id=1,
    )


@pytest.mark.integration
def test_schema_evolution_nested(catalog: Catalog) -> None:
    nested_schema = Schema(
        NestedField(
            field_id=1,
            name="location_lookup",
            field_type=MapType(
                key_id=10,
                key_type=StringType(),
                value_id=11,
                value_type=StructType(
                    NestedField(field_id=110, name="x", field_type=FloatType(), required=False),
                    NestedField(field_id=111, name="y", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=2,
            name="locations",
            field_type=ListType(
                element_id=20,
                element_type=StructType(
                    NestedField(field_id=200, name="x", field_type=FloatType(), required=False),
                    NestedField(field_id=201, name="y", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=3,
            name="person",
            field_type=StructType(
                NestedField(field_id=30, name="name", field_type=StringType(), required=False),
                NestedField(field_id=31, name="age", field_type=IntegerType(), required=True),
            ),
            required=False,
        ),
        schema_id=1,
    )

    tbl = _create_table_with_schema(catalog, nested_schema)

    assert tbl.schema().highest_field_id == 12

    with tbl.update_schema() as schema_update:
        schema_update.add_column(("location_lookup", "z"), FloatType())
        schema_update.add_column(("locations", "z"), FloatType())
        schema_update.add_column(("person", "address"), StringType())

    assert str(tbl.schema()) == str(
        Schema(
            NestedField(
                field_id=1,
                name="location_lookup",
                field_type=MapType(
                    type="map",
                    key_id=4,
                    key_type=StringType(),
                    value_id=5,
                    value_type=StructType(
                        NestedField(field_id=6, name="x", field_type=FloatType(), required=False),
                        NestedField(field_id=7, name="y", field_type=FloatType(), required=False),
                        NestedField(field_id=13, name="z", field_type=FloatType(), required=False),
                    ),
                    value_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=2,
                name="locations",
                field_type=ListType(
                    type="list",
                    element_id=8,
                    element_type=StructType(
                        NestedField(field_id=9, name="x", field_type=FloatType(), required=False),
                        NestedField(field_id=10, name="y", field_type=FloatType(), required=False),
                        NestedField(field_id=14, name="z", field_type=FloatType(), required=False),
                    ),
                    element_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=3,
                name="person",
                field_type=StructType(
                    NestedField(field_id=11, name="name", field_type=StringType(), required=False),
                    NestedField(field_id=12, name="age", field_type=IntegerType(), required=True),
                    NestedField(field_id=15, name="address", field_type=StringType(), required=False),
                ),
                required=False,
            ),
            schema_id=1,
            identifier_field_ids=[],
        )
    )

schema_nested = Schema(
    NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
    NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
    NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
    NestedField(
        field_id=4,
        name="qux",
        field_type=ListType(type="list", element_id=8, element_type=StringType(), element_required=True),
        required=True,
    ),
    NestedField(
        field_id=5,
        name="quux",
        field_type=MapType(
            type="map",
            key_id=9,
            key_type=StringType(),
            value_id=10,
            value_type=MapType(
                type="map", key_id=11, key_type=StringType(), value_id=12, value_type=IntegerType(), value_required=True
            ),
            value_required=True,
        ),
        required=True,
    ),
    NestedField(
        field_id=6,
        name="location",
        field_type=ListType(
            type="list",
            element_id=13,
            element_type=StructType(
                fields=(
                    NestedField(field_id=14, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=15, name="longitude", field_type=FloatType(), required=False),
                )
            ),
            element_required=True,
        ),
        required=True,
    ),
    NestedField(
        field_id=7,
        name="person",
        field_type=StructType(
            fields=(
                NestedField(field_id=16, name="name", field_type=StringType(), required=False),
                NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
            )
        ),
        required=False,
    ),
    schema_id=0,
    identifier_field_ids=[2],
)


@pytest.fixture()
def nested_table(catalog: Catalog) -> Table:
    return _create_table_with_schema(catalog, schema_nested)


def test_no_changes(simple_table: Table, table_schema_simple: Schema) -> None:
    with simple_table.update_schema() as _:
        pass

    assert simple_table.schema() == table_schema_simple


def test_delete_field(simple_table: Table) -> None:
    with simple_table.update_schema() as schema_update:
        schema_update.delete_column("foo")

    assert simple_table.schema() == Schema(
        # foo is missing 👍
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_delete_field_case_insensitive(simple_table: Table) -> None:
    with simple_table.update_schema(case_sensitive=False) as schema_update:
        schema_update.delete_column("FOO")

    assert simple_table.schema() == Schema(
        # foo is missing 👍
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_delete_identifier_fields(simple_table: Table) -> None:
    with pytest.raises(ValueError) as exc_info:
        with simple_table.update_schema() as schema_update:
            schema_update.delete_column("bar")

    assert str(exc_info) == "Cannot delete identifier field bar. To force deletion, update the identifier fields first."


@pytest.mark.skip(reason="REST Catalog gives an error")
def test_delete_identifier_fields_nested(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            NestedField(
                field_id=2,
                name="person",
                field_type=StructType(
                    NestedField(field_id=3, name="name", field_type=StringType(), required=True),
                    NestedField(field_id=4, name="age", field_type=IntegerType(), required=True),
                ),
                required=True,
            ),
            schema_id=1,
            identifier_field_ids=[],
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.delete_column("person")

    assert str(exc_info) == "Cannot delete field person as it will delete nested identifier field name."


@pytest.mark.parametrize(
    "field",
    [
        "foo",
        "baz",
        "qux",
        "quux",
        "location",
        "location.element.latitude",
        "location.element.longitude",
        "person",
        "person.name",
        "person.age",
    ],
)
def test_deletes(field: str, nested_table: Table) -> None:
    with nested_table.update_schema() as schema_update:
        schema_update.delete_column(field)

    selected_ids = {
        field_id
        for field_id in schema_nested.field_ids
        if not isinstance(schema_nested.find_field(field_id).field_type, (MapType, ListType))
        and not schema_nested.find_column_name(field_id).startswith(field)
    }
    expected_schema = prune_columns(schema_nested, selected_ids, select_full_types=False)

    assert expected_schema == nested_table.schema()

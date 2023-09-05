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

import pytest

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError, ValidationError
from pyiceberg.schema import Schema, prune_columns
from pyiceberg.table import Table, UpdateSchema
from pyiceberg.types import (
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
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
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


def _create_table_with_schema(catalog: Catalog, schema: Schema) -> Table:
    tbl_name = "default.test_schema_evolution"
    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier=tbl_name, schema=schema)


@pytest.mark.integration
def test_add_already_exists(catalog: Catalog, table_schema_nested: Schema) -> None:
    table = _create_table_with_schema(catalog, table_schema_nested)
    update = UpdateSchema(table)

    with pytest.raises(ValueError) as exc_info:
        update.add_column("foo", IntegerType())
    assert "already exists: foo" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        update.add_column(path=("location", "latitude"), field_type=IntegerType())
    assert "already exists: location.latitude" in str(exc_info.value)


@pytest.mark.integration
def test_add_to_non_struct_type(catalog: Catalog, table_schema_simple: Schema) -> None:
    table = _create_table_with_schema(catalog, table_schema_simple)
    update = UpdateSchema(table)
    with pytest.raises(ValueError) as exc_info:
        update.add_column(path=("foo", "lat"), field_type=IntegerType())
    assert "Cannot add column 'lat' to non-struct type: foo" in str(exc_info.value)


@pytest.mark.integration
def test_schema_evolution_nested_field(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(
            field_id=1,
            name="foo",
            field_type=StructType(NestedField(2, name="bar", field_type=StringType(), required=False)),
            required=False,
        ),
    )
    tbl = _create_table_with_schema(catalog, schema)

    assert tbl.schema() == schema

    with pytest.raises(ValidationError) as exc_info:
        with tbl.transaction() as tx:
            tx.update_schema().update_column("foo", StringType()).commit()

    assert "Cannot change column type: struct<2: bar: optional string> is not a primitive" in str(exc_info.value)


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
    )

    tbl.update_schema().add_column("col_integer", IntegerType()).commit()

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="col_uuid", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="col_fixed", field_type=FixedType(25), required=False),
        NestedField(field_id=3, name="col_string", field_type=StringType(), required=False),
        NestedField(field_id=4, name="col_integer", field_type=IntegerType(), required=False),
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
                NestedField(field_id=14, name="latitude", field_type=FloatType(), required=False),
                NestedField(field_id=15, name="longitude", field_type=FloatType(), required=False),
            ),
            element_required=True,
        ),
        required=True,
    ),
    NestedField(
        field_id=7,
        name="person",
        field_type=StructType(
            NestedField(field_id=16, name="name", field_type=StringType(), required=False),
            NestedField(field_id=17, name="age", field_type=IntegerType(), required=True),
        ),
        required=False,
    ),
    identifier_field_ids=[2],
)


@pytest.fixture()
def nested_table(catalog: Catalog) -> Table:
    return _create_table_with_schema(catalog, schema_nested)


@pytest.mark.integration
def test_no_changes(simple_table: Table, table_schema_simple: Schema) -> None:
    with simple_table.update_schema() as _:
        pass

    assert simple_table.schema() == table_schema_simple


@pytest.mark.integration
def test_no_changes_empty_commit(simple_table: Table, table_schema_simple: Schema) -> None:
    with simple_table.update_schema() as update:
        # No updates, so this should be a noop
        update.update_column(path="foo")

    assert simple_table.schema() == table_schema_simple


@pytest.mark.integration
def test_delete_field(simple_table: Table) -> None:
    with simple_table.update_schema() as schema_update:
        schema_update.delete_column("foo")

    assert simple_table.schema() == Schema(
        # foo is missing 👍
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        identifier_field_ids=[2],
    )


@pytest.mark.integration
def test_delete_field_case_insensitive(simple_table: Table) -> None:
    with simple_table.update_schema(case_sensitive=False) as schema_update:
        schema_update.delete_column("FOO")

    assert simple_table.schema() == Schema(
        # foo is missing 👍
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        identifier_field_ids=[2],
    )


@pytest.mark.integration
def test_delete_identifier_fields(simple_table: Table) -> None:
    with pytest.raises(ValueError) as exc_info:
        with simple_table.update_schema() as schema_update:
            schema_update.delete_column("bar")

    assert "Cannot find identifier field bar. In case of deletion, update the identifier fields first." in str(exc_info)


@pytest.mark.integration
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
            identifier_field_ids=[3],
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.delete_column("person")

    assert "Cannot find identifier field person.name. In case of deletion, update the identifier fields first." in str(exc_info)


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
@pytest.mark.integration
def test_deletes(field: str, nested_table: Table) -> None:
    with nested_table.update_schema() as schema_update:
        schema_update.delete_column(field)

    selected_ids = {
        field_id
        for field_id in schema_nested.field_ids
        if not isinstance(schema_nested.find_field(field_id).field_type, (MapType, ListType))
        and not schema_nested.find_column_name(field_id).startswith(field)  # type: ignore
    }
    expected_schema = prune_columns(schema_nested, selected_ids, select_full_types=False)

    assert expected_schema == nested_table.schema()


@pytest.mark.parametrize(
    "field",
    [
        "Foo",
        "Baz",
        "Qux",
        "Quux",
        "Location",
        "Location.element.latitude",
        "Location.element.longitude",
        "Person",
        "Person.name",
        "Person.age",
    ],
)
@pytest.mark.integration
def test_deletes_case_insensitive(field: str, nested_table: Table) -> None:
    with nested_table.update_schema(case_sensitive=False) as schema_update:
        schema_update.delete_column(field)

    selected_ids = {
        field_id
        for field_id in schema_nested.field_ids
        if not isinstance(schema_nested.find_field(field_id).field_type, (MapType, ListType))
        and not schema_nested.find_column_name(field_id).startswith(field.lower())  # type: ignore
    }
    expected_schema = prune_columns(schema_nested, selected_ids, select_full_types=False)

    assert expected_schema == nested_table.schema()


@pytest.mark.integration
def test_update_types(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="bar", field_type=IntegerType(), required=True),
            NestedField(
                field_id=2,
                name="location",
                field_type=ListType(
                    type="list",
                    element_id=3,
                    element_type=StructType(
                        NestedField(field_id=4, name="latitude", field_type=FloatType(), required=False),
                        NestedField(field_id=5, name="longitude", field_type=FloatType(), required=False),
                    ),
                    element_required=True,
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.update_column("bar", LongType())
        schema_update.update_column("location.latitude", DoubleType())
        schema_update.update_column("location.longitude", DoubleType())

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="bar", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="location",
            field_type=ListType(
                type="list",
                element_id=3,
                element_type=StructType(
                    NestedField(field_id=4, name="latitude", field_type=DoubleType(), required=False),
                    NestedField(field_id=5, name="longitude", field_type=DoubleType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_update_types_case_insensitive(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="bar", field_type=IntegerType(), required=True),
            NestedField(
                field_id=2,
                name="location",
                field_type=ListType(
                    type="list",
                    element_id=3,
                    element_type=StructType(
                        NestedField(field_id=4, name="latitude", field_type=FloatType(), required=False),
                        NestedField(field_id=5, name="longitude", field_type=FloatType(), required=False),
                    ),
                    element_required=True,
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema(case_sensitive=False) as schema_update:
        schema_update.update_column("baR", LongType())
        schema_update.update_column("Location.Latitude", DoubleType())
        schema_update.update_column("Location.Longitude", DoubleType())

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="bar", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="location",
            field_type=ListType(
                type="list",
                element_id=3,
                element_type=StructType(
                    NestedField(field_id=4, name="latitude", field_type=DoubleType(), required=False),
                    NestedField(field_id=5, name="longitude", field_type=DoubleType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
    )


allowed_promotions = [
    (StringType(), BinaryType()),
    (BinaryType(), StringType()),
    (IntegerType(), LongType()),
    (FloatType(), DoubleType()),
    (DecimalType(9, 2), DecimalType(18, 2)),
]


@pytest.mark.parametrize("from_type, to_type", allowed_promotions, ids=str)
@pytest.mark.integration
def test_allowed_updates(from_type: PrimitiveType, to_type: PrimitiveType, catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="bar", field_type=from_type, required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.update_column("bar", to_type)

    assert tbl.schema() == Schema(NestedField(field_id=1, name="bar", field_type=to_type, required=True))


disallowed_promotions_types = [
    BooleanType(),
    IntegerType(),
    LongType(),
    FloatType(),
    DoubleType(),
    DateType(),
    TimeType(),
    TimestampType(),
    TimestamptzType(),
    StringType(),
    UUIDType(),
    BinaryType(),
    FixedType(3),
    FixedType(4),
    # We'll just allow Decimal promotions right now
    # https://github.com/apache/iceberg/issues/8389
    # DecimalType(9, 2),
    # DecimalType(9, 3),
    DecimalType(18, 2),
]


@pytest.mark.parametrize("from_type", disallowed_promotions_types, ids=str)
@pytest.mark.parametrize("to_type", disallowed_promotions_types, ids=str)
@pytest.mark.integration
def test_disallowed_updates(from_type: PrimitiveType, to_type: PrimitiveType, catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="bar", field_type=from_type, required=True),
        ),
    )

    if from_type != to_type and (from_type, to_type) not in allowed_promotions:
        with pytest.raises(ValidationError) as exc_info:
            with tbl.update_schema() as schema_update:
                schema_update.update_column("bar", to_type)

        assert str(exc_info.value).startswith("Cannot change column type: bar:")
    else:
        with tbl.update_schema() as schema_update:
            schema_update.update_column("bar", to_type)

        assert tbl.schema() == Schema(
            NestedField(field_id=1, name="bar", field_type=to_type, required=True),
        )


@pytest.mark.integration
def test_rename_simple(simple_table: Table) -> None:
    with simple_table.update_schema() as schema_update:
        schema_update.rename_column("foo", "vo")

    assert simple_table.schema() == Schema(
        NestedField(field_id=1, name="vo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        identifier_field_ids=[2],
    )


@pytest.mark.integration
def test_rename_simple_nested(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="foo",
                field_type=StructType(NestedField(field_id=2, name="bar", field_type=StringType())),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.rename_column("foo.bar", "vo")

    assert tbl.schema() == Schema(
        NestedField(
            field_id=1,
            name="foo",
            field_type=StructType(NestedField(field_id=2, name="vo", field_type=StringType())),
            required=True,
        ),
    )


@pytest.mark.integration
def test_rename_simple_nested_with_dots(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="a.b",
                field_type=StructType(NestedField(field_id=2, name="c.d", field_type=StringType())),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.rename_column(("a.b", "c.d"), "e.f")

    assert tbl.schema() == Schema(
        NestedField(
            field_id=1,
            name="a.b",
            field_type=StructType(NestedField(field_id=2, name="e.f", field_type=StringType())),
            required=True,
        ),
    )


@pytest.mark.integration
def test_rename(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="location_lookup",
                field_type=MapType(
                    type="map",
                    key_id=5,
                    key_type=StringType(),
                    value_id=6,
                    value_type=StructType(
                        NestedField(field_id=7, name="x", field_type=FloatType(), required=False),
                        NestedField(field_id=8, name="y", field_type=FloatType(), required=False),
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
                    element_id=9,
                    element_type=StructType(
                        NestedField(field_id=10, name="x", field_type=FloatType(), required=False),
                        NestedField(field_id=11, name="y", field_type=FloatType(), required=False),
                    ),
                    element_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=3,
                name="person",
                field_type=StructType(
                    NestedField(field_id=12, name="name", field_type=StringType(), required=False),
                    NestedField(field_id=13, name="leeftijd", field_type=IntegerType(), required=True),
                ),
                required=False,
            ),
            NestedField(field_id=4, name="foo", field_type=StringType(), required=True),
            identifier_field_ids=[],
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.rename_column("foo", "bar")
        schema_update.rename_column("location_lookup.x", "latitude")
        schema_update.rename_column("locations.x", "latitude")
        schema_update.rename_column("person.leeftijd", "age")

    assert tbl.schema() == Schema(
        NestedField(
            field_id=1,
            name="location_lookup",
            field_type=MapType(
                type="map",
                key_id=5,
                key_type=StringType(),
                value_id=6,
                value_type=StructType(
                    NestedField(field_id=7, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=8, name="y", field_type=FloatType(), required=False),
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
                element_id=9,
                element_type=StructType(
                    NestedField(field_id=10, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=11, name="y", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=3,
            name="person",
            field_type=StructType(
                NestedField(field_id=12, name="name", field_type=StringType(), required=False),
                NestedField(field_id=13, name="age", field_type=IntegerType(), required=True),
            ),
            required=False,
        ),
        NestedField(field_id=4, name="bar", field_type=StringType(), required=True),
        identifier_field_ids=[],
    )


@pytest.mark.integration
def test_rename_case_insensitive(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="location_lookup",
                field_type=MapType(
                    type="map",
                    key_id=5,
                    key_type=StringType(),
                    value_id=6,
                    value_type=StructType(
                        NestedField(field_id=7, name="x", field_type=FloatType(), required=False),
                        NestedField(field_id=8, name="y", field_type=FloatType(), required=False),
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
                    element_id=9,
                    element_type=StructType(
                        NestedField(field_id=10, name="x", field_type=FloatType(), required=False),
                        NestedField(field_id=11, name="y", field_type=FloatType(), required=False),
                    ),
                    element_required=True,
                ),
                required=True,
            ),
            NestedField(
                field_id=3,
                name="person",
                field_type=StructType(
                    NestedField(field_id=12, name="name", field_type=StringType(), required=False),
                    NestedField(field_id=13, name="leeftijd", field_type=IntegerType(), required=True),
                ),
                required=True,
            ),
            NestedField(field_id=4, name="foo", field_type=StringType(), required=True),
            identifier_field_ids=[13],
        ),
    )

    with tbl.update_schema(case_sensitive=False) as schema_update:
        schema_update.rename_column("Foo", "bar")
        schema_update.rename_column("Location_lookup.X", "latitude")
        schema_update.rename_column("Locations.X", "latitude")
        schema_update.rename_column("Person.Leeftijd", "age")

    assert tbl.schema() == Schema(
        NestedField(
            field_id=1,
            name="location_lookup",
            field_type=MapType(
                type="map",
                key_id=5,
                key_type=StringType(),
                value_id=6,
                value_type=StructType(
                    NestedField(field_id=7, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=8, name="y", field_type=FloatType(), required=False),
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
                element_id=9,
                element_type=StructType(
                    NestedField(field_id=10, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=11, name="y", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=3,
            name="person",
            field_type=StructType(
                NestedField(field_id=12, name="name", field_type=StringType(), required=False),
                NestedField(field_id=13, name="age", field_type=IntegerType(), required=True),
            ),
            required=True,
        ),
        NestedField(field_id=4, name="bar", field_type=StringType(), required=True),
        identifier_field_ids=[13],
    )


@pytest.mark.integration
def test_add_struct(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    struct = StructType(
        NestedField(field_id=3, name="x", field_type=DoubleType(), required=False),
        NestedField(field_id=4, name="y", field_type=DoubleType(), required=False),
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column("location", struct)

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType()),
        NestedField(field_id=2, name="location", field_type=struct, required=False),
    )


@pytest.mark.integration
def test_add_nested_map_of_structs(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    map_type_example = MapType(
        key_id=1,
        value_id=2,
        key_type=StructType(
            NestedField(field_id=20, name="address", field_type=StringType(), required=True),
            NestedField(field_id=21, name="city", field_type=StringType(), required=True),
            NestedField(field_id=22, name="state", field_type=StringType(), required=True),
            NestedField(field_id=23, name="zip", field_type=IntegerType(), required=True),
        ),
        value_type=StructType(
            NestedField(field_id=9, name="lat", field_type=DoubleType(), required=True),
            NestedField(field_id=8, name="long", field_type=DoubleType(), required=False),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column("locations", map_type_example)

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        NestedField(
            field_id=2,
            name="locations",
            field_type=MapType(
                type="map",
                key_id=3,
                key_type=StructType(
                    NestedField(field_id=5, name="address", field_type=StringType(), required=True),
                    NestedField(field_id=6, name="city", field_type=StringType(), required=True),
                    NestedField(field_id=7, name="state", field_type=StringType(), required=True),
                    NestedField(field_id=8, name="zip", field_type=IntegerType(), required=True),
                ),
                value_id=4,
                value_type=StructType(
                    NestedField(field_id=9, name="lat", field_type=DoubleType(), required=True),
                    NestedField(field_id=10, name="long", field_type=DoubleType(), required=False),
                ),
                value_required=True,
            ),
            required=False,
        ),
    )


@pytest.mark.integration
def test_add_nested_list_of_structs(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    list_type_examples = ListType(
        element_id=1,
        element_type=StructType(
            NestedField(field_id=9, name="lat", field_type=DoubleType(), required=True),
            NestedField(field_id=10, name="long", field_type=DoubleType(), required=False),
        ),
        element_required=False,
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column("locations", list_type_examples)

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        NestedField(
            field_id=2,
            name="locations",
            field_type=ListType(
                type="list",
                element_id=3,
                element_type=StructType(
                    NestedField(field_id=4, name="lat", field_type=DoubleType(), required=True),
                    NestedField(field_id=5, name="long", field_type=DoubleType(), required=False),
                ),
                element_required=False,
            ),
            required=False,
        ),
    )


@pytest.mark.integration
def test_add_required_column(catalog: Catalog) -> None:
    schema_ = Schema(NestedField(field_id=1, name="a", field_type=BooleanType(), required=False))
    table = _create_table_with_schema(catalog, schema_)
    update = UpdateSchema(table)
    with pytest.raises(ValueError) as exc_info:
        update.add_column(path="data", field_type=IntegerType(), required=True)
    assert "Incompatible change: cannot add required column: data" in str(exc_info.value)

    new_schema = (
        UpdateSchema(table, allow_incompatible_changes=True)  # pylint: disable=W0212
        .add_column(path="data", field_type=IntegerType(), required=True)
        ._apply()
    )
    assert new_schema == Schema(
        NestedField(field_id=1, name="a", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="data", field_type=IntegerType(), required=True),
    )


@pytest.mark.integration
def test_add_required_column_case_insensitive(catalog: Catalog) -> None:
    schema_ = Schema(NestedField(field_id=1, name="id", field_type=BooleanType(), required=False))
    table = _create_table_with_schema(catalog, schema_)

    with pytest.raises(ValueError) as exc_info:
        with UpdateSchema(table, allow_incompatible_changes=True) as update:
            update.case_sensitive(False).add_column(path="ID", field_type=IntegerType(), required=True)
    assert "already exists: ID" in str(exc_info.value)

    new_schema = (
        UpdateSchema(table, allow_incompatible_changes=True)  # pylint: disable=W0212
        .add_column(path="ID", field_type=IntegerType(), required=True)
        ._apply()
    )
    assert new_schema == Schema(
        NestedField(field_id=1, name="id", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="ID", field_type=IntegerType(), required=True),
    )


@pytest.mark.integration
def test_make_column_optional(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.make_column_optional("foo")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
    )


@pytest.mark.integration
def test_mixed_changes(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=StringType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=False),
            NestedField(
                field_id=3,
                name="preferences",
                field_type=StructType(
                    NestedField(field_id=8, name="feature1", type=BooleanType(), required=True),
                    NestedField(field_id=9, name="feature2", type=BooleanType(), required=False),
                ),
                required=False,
            ),
            NestedField(
                field_id=4,
                name="locations",
                field_type=MapType(
                    key_id=10,
                    value_id=11,
                    key_type=StructType(
                        NestedField(field_id=20, name="address", field_type=StringType(), required=True),
                        NestedField(field_id=21, name="city", field_type=StringType(), required=True),
                        NestedField(field_id=22, name="state", field_type=StringType(), required=True),
                        NestedField(field_id=23, name="zip", field_type=IntegerType(), required=True),
                    ),
                    value_type=StructType(
                        NestedField(field_id=12, name="lat", field_type=DoubleType(), required=True),
                        NestedField(field_id=13, name="long", field_type=DoubleType(), required=False),
                    ),
                ),
                required=True,
            ),
            NestedField(
                field_id=5,
                name="points",
                field_type=ListType(
                    element_id=14,
                    element_type=StructType(
                        NestedField(field_id=15, name="x", field_type=LongType(), required=True),
                        NestedField(field_id=16, name="y", field_type=LongType(), required=True),
                    ),
                ),
                required=True,
                doc="2-D cartesian points",
            ),
            NestedField(field_id=6, name="doubles", field_type=ListType(element_id=17, element_type=DoubleType()), required=True),
            NestedField(
                field_id=7,
                name="properties",
                field_type=MapType(key_id=18, value_id=19, key_type=StringType(), value_type=StringType()),
                required=False,
            ),
        ),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as schema_update:
        schema_update.add_column("toplevel", field_type=DecimalType(9, 2))
        schema_update.add_column(("locations", "alt"), field_type=FloatType())
        schema_update.add_column(("points", "z"), field_type=LongType())
        schema_update.add_column(("points", "t.t"), field_type=LongType(), doc="name with '.'")
        schema_update.rename_column("data", "json")
        schema_update.rename_column("preferences", "options")
        schema_update.rename_column("preferences.feature2", "newfeature")
        schema_update.rename_column("locations.lat", "latitude")
        schema_update.rename_column("points.x", "X")
        schema_update.rename_column("points.y", "y.y")
        schema_update.update_column("id", field_type=LongType(), doc="unique id")
        schema_update.update_column("locations.lat", DoubleType())
        schema_update.update_column("locations.lat", doc="latitude")
        schema_update.delete_column("locations.long")
        schema_update.delete_column("properties")
        schema_update.make_column_optional("points.x")
        schema_update.update_column("data", required=True)
        schema_update.add_column(("locations", "description"), StringType(), doc="location description")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True, doc="unique id"),
        NestedField(field_id=2, name="json", field_type=StringType(), required=True),
        NestedField(
            field_id=3,
            name="options",
            field_type=StructType(
                NestedField(field_id=8, name="feature1", field_type=BooleanType(), required=True),
                NestedField(field_id=9, name="newfeature", field_type=BooleanType(), required=False),
            ),
            required=False,
        ),
        NestedField(
            field_id=4,
            name="locations",
            field_type=MapType(
                type="map",
                key_id=10,
                key_type=StructType(
                    NestedField(field_id=12, name="address", field_type=StringType(), required=True),
                    NestedField(field_id=13, name="city", field_type=StringType(), required=True),
                    NestedField(field_id=14, name="state", field_type=StringType(), required=True),
                    NestedField(field_id=15, name="zip", field_type=IntegerType(), required=True),
                ),
                value_id=11,
                value_type=StructType(
                    NestedField(field_id=16, name="latitude", field_type=DoubleType(), required=True, doc="latitude"),
                    NestedField(field_id=25, name="alt", field_type=FloatType(), required=False),
                    NestedField(
                        field_id=28, name="description", field_type=StringType(), required=False, doc="location description"
                    ),
                ),
                value_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=5,
            name="points",
            field_type=ListType(
                type="list",
                element_id=18,
                element_type=StructType(
                    NestedField(field_id=19, name="X", field_type=LongType(), required=False),
                    NestedField(field_id=20, name="y.y", field_type=LongType(), required=True),
                    NestedField(field_id=26, name="z", field_type=LongType(), required=False),
                    NestedField(field_id=27, name="t.t", field_type=LongType(), required=False, doc="name with '.'"),
                ),
                element_required=True,
            ),
            doc="2-D cartesian points",
            required=True,
        ),
        NestedField(
            field_id=6,
            name="doubles",
            field_type=ListType(type="list", element_id=21, element_type=DoubleType(), element_required=True),
            required=True,
        ),
        NestedField(field_id=24, name="toplevel", field_type=DecimalType(precision=9, scale=2), required=False),
    )


@pytest.mark.integration
def test_ambiguous_column(catalog: Catalog, table_schema_nested: Schema) -> None:
    table = _create_table_with_schema(catalog, table_schema_nested)
    update = UpdateSchema(table)

    with pytest.raises(ValueError) as exc_info:
        update.add_column(path="location.latitude", field_type=IntegerType())
    assert "Cannot add column with ambiguous name: location.latitude, provide a tuple instead" in str(exc_info.value)


@pytest.mark.integration
def test_delete_then_add(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.delete_column("foo")
        schema_update.add_column("foo", StringType())

    assert tbl.schema() == Schema(
        NestedField(field_id=2, name="foo", field_type=StringType(), required=False),
    )


@pytest.mark.integration
def test_delete_then_add_nested(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="preferences",
                field_type=StructType(
                    NestedField(field_id=2, name="feature1", field_type=BooleanType()),
                    NestedField(field_id=3, name="feature2", field_type=BooleanType()),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.delete_column("preferences.feature1")
        schema_update.add_column(("preferences", "feature1"), BooleanType())

    assert tbl.schema() == Schema(
        NestedField(
            field_id=1,
            name="preferences",
            field_type=StructType(
                NestedField(field_id=3, name="feature2", field_type=BooleanType()),
                NestedField(field_id=4, name="feature1", field_type=BooleanType(), required=False),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_delete_missing_column(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.delete_column("bar")

    assert "Could not find field with name bar, case_sensitive=True" in str(exc_info.value)


@pytest.mark.integration
def test_add_delete_conflict(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.add_column("bar", BooleanType())
            schema_update.delete_column("bar")
    assert "Could not find field with name bar, case_sensitive=True" in str(exc_info.value)

    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="preferences",
                field_type=StructType(
                    NestedField(field_id=2, name="feature1", field_type=BooleanType()),
                    NestedField(field_id=3, name="feature2", field_type=BooleanType()),
                ),
                required=True,
            ),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.add_column(("preferences", "feature3"), BooleanType())
            schema_update.delete_column("preferences")
    assert "Cannot delete a column that has additions: preferences" in str(exc_info.value)


@pytest.mark.integration
def test_rename_missing_column(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.rename_column("bar", "fail")

    assert "Could not find field with name bar, case_sensitive=True" in str(exc_info.value)


@pytest.mark.integration
def test_rename_missing_conflicts(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.rename_column("foo", "bar")
            schema_update.delete_column("foo")

    assert "Cannot delete a column that has updates: foo" in str(exc_info.value)

    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.rename_column("foo", "bar")
            schema_update.delete_column("bar")

    assert "Could not find field with name bar, case_sensitive=True" in str(exc_info.value)


@pytest.mark.integration
def test_update_missing_column(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.update_column("bar", DateType())

    assert "Could not find field with name bar, case_sensitive=True" in str(exc_info.value)


@pytest.mark.integration
def test_update_delete_conflict(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=IntegerType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.update_column("foo", LongType())
            schema_update.delete_column("foo")

    assert "Cannot delete a column that has updates: foo" in str(exc_info.value)


@pytest.mark.integration
def test_delete_update_conflict(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=IntegerType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.delete_column("foo")
            schema_update.update_column("foo", LongType())

    assert "Cannot update a column that will be deleted: foo" in str(exc_info.value)


@pytest.mark.integration
def test_delete_map_key(nested_table: Table) -> None:
    with pytest.raises(ValueError) as exc_info:
        with nested_table.update_schema() as schema_update:
            schema_update.delete_column("quux.key")

    assert "Cannot delete map keys" in str(exc_info.value)


@pytest.mark.integration
def test_add_field_to_map_key(nested_table: Table) -> None:
    with pytest.raises(ValueError) as exc_info:
        with nested_table.update_schema() as schema_update:
            schema_update.add_column(("quux", "key"), StringType())

    assert "Cannot add column 'key' to non-struct type: quux" in str(exc_info.value)


@pytest.mark.integration
def test_alter_map_key(nested_table: Table) -> None:
    with pytest.raises(ValueError) as exc_info:
        with nested_table.update_schema() as schema_update:
            schema_update.update_column(("quux", "key"), BinaryType())

    assert "Cannot update map keys" in str(exc_info.value)


@pytest.mark.integration
def test_update_map_key(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1, name="m", field_type=MapType(key_id=2, value_id=3, key_type=IntegerType(), value_type=DoubleType())
            )
        ),
    )
    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.update_column("m.key", LongType())

    assert "Cannot update map keys: map<int, double>" in str(exc_info.value)


@pytest.mark.integration
def test_update_added_column_doc(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.add_column("value", LongType())
            schema_update.update_column("value", doc="a value")

    assert "Could not find field with name value, case_sensitive=True" in str(exc_info.value)


@pytest.mark.integration
def test_update_deleted_column_doc(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.delete_column("foo")
            schema_update.update_column("foo", doc="a value")

    assert "Cannot update a column that will be deleted: foo" in str(exc_info.value)


@pytest.mark.integration
def test_multiple_moves(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="a", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="b", field_type=IntegerType(), required=True),
            NestedField(field_id=3, name="c", field_type=IntegerType(), required=True),
            NestedField(field_id=4, name="d", field_type=IntegerType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_first("d")
        schema_update.move_first("c")
        schema_update.move_after("b", "d")
        schema_update.move_before("d", "a")

    assert tbl.schema() == Schema(
        NestedField(field_id=3, name="c", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="b", field_type=IntegerType(), required=True),
        NestedField(field_id=4, name="d", field_type=IntegerType(), required=True),
        NestedField(field_id=1, name="a", field_type=IntegerType(), required=True),
    )


@pytest.mark.integration
def test_move_top_level_column_first(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_first("data")

    assert tbl.schema() == Schema(
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    )


@pytest.mark.integration
def test_move_top_level_column_before_first(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_before("data", "id")

    assert tbl.schema() == Schema(
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    )


@pytest.mark.integration
def test_move_top_level_column_after_last(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_after("id", "data")

    assert tbl.schema() == Schema(
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
    )


@pytest.mark.integration
def test_move_nested_field_first(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_first("struct.data")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="struct",
            field_type=StructType(
                NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                NestedField(field_id=3, name="count", field_type=LongType(), required=True),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_nested_field_before_first(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_before("struct.data", "struct.count")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="struct",
            field_type=StructType(
                NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                NestedField(field_id=3, name="count", field_type=LongType(), required=True),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_nested_field_after_first(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_before("struct.data", "struct.count")

    assert str(tbl.schema()) == str(
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                ),
                required=True,
            ),
        )
    )


@pytest.mark.integration
def test_move_nested_field_after(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                    NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_after("struct.ts", "struct.count")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="struct",
            field_type=StructType(
                NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=True),
                NestedField(field_id=4, name="data", field_type=StringType(), required=True),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_nested_field_before(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                    NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_before("struct.ts", "struct.data")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="struct",
            field_type=StructType(
                NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=True),
                NestedField(field_id=4, name="data", field_type=StringType(), required=True),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_map_value_struct_field(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="map",
                field_type=MapType(
                    key_id=3,
                    value_id=4,
                    key_type=StringType(),
                    value_type=StructType(
                        NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=True),
                        NestedField(field_id=6, name="count", field_type=LongType(), required=True),
                        NestedField(field_id=7, name="data", field_type=StringType(), required=True),
                    ),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.move_before("map.ts", "map.data")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="map",
            field_type=MapType(
                key_id=3,
                value_id=4,
                key_type=StringType(),
                value_type=StructType(
                    NestedField(field_id=6, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=True),
                    NestedField(field_id=7, name="data", field_type=StringType(), required=True),
                ),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_added_top_level_column(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column("ts", TimestamptzType())
        schema_update.move_after("ts", "id")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=3, name="ts", field_type=TimestamptzType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )


@pytest.mark.integration
def test_move_added_top_level_column_after_added_column(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column("ts", TimestamptzType())
        schema_update.add_column("count", LongType())
        schema_update.move_after("ts", "id")
        schema_update.move_after("count", "ts")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=3, name="ts", field_type=TimestamptzType(), required=False),
        NestedField(field_id=4, name="count", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )


@pytest.mark.integration
def test_move_added_nested_struct_field(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column(("struct", "ts"), TimestamptzType())
        schema_update.move_before("struct.ts", "struct.count")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="struct",
            field_type=StructType(
                NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=False),
                NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                NestedField(field_id=4, name="data", field_type=StringType(), required=True),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_added_nested_field_before_added_column(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                    NestedField(field_id=4, name="data", field_type=StringType(), required=True),
                ),
                required=True,
            ),
        ),
    )

    with tbl.update_schema() as schema_update:
        schema_update.add_column(("struct", "ts"), TimestamptzType())
        schema_update.add_column(("struct", "size"), LongType())
        schema_update.move_before("struct.ts", "struct.count")
        schema_update.move_before("struct.size", "struct.ts")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(
            field_id=2,
            name="struct",
            field_type=StructType(
                NestedField(field_id=6, name="size", field_type=LongType(), required=False),
                NestedField(field_id=5, name="ts", field_type=TimestamptzType(), required=False),
                NestedField(field_id=3, name="count", field_type=LongType(), required=True),
                NestedField(field_id=4, name="data", field_type=StringType(), required=True),
            ),
            required=True,
        ),
    )


@pytest.mark.integration
def test_move_self_reference_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("foo", "foo")
    assert "Cannot move foo before itself" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_after("foo", "foo")
    assert "Cannot move foo after itself" in str(exc_info.value)


@pytest.mark.integration
def test_move_missing_column_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_first("items")
    assert "Cannot move missing column: items" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("items", "id")
    assert "Cannot move missing column: items" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_after("items", "data")
    assert "Cannot move missing column: items" in str(exc_info.value)


@pytest.mark.integration
def test_move_before_add_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_first("ts")
            update.add_column("ts", TimestamptzType())
    assert "Cannot move missing column: ts" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("ts", "id")
            update.add_column("ts", TimestamptzType())
    assert "Cannot move missing column: ts" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_after("ts", "data")
            update.add_column("ts", TimestamptzType())
    assert "Cannot move missing column: ts" in str(exc_info.value)


@pytest.mark.integration
def test_move_missing_reference_column_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("id", "items")
    assert "Cannot move id before missing column: items" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_after("data", "items")
    assert "Cannot move data after missing column: items" in str(exc_info.value)


@pytest.mark.integration
def test_move_primitive_map_key_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
            NestedField(
                field_id=3,
                name="map",
                field_type=MapType(key_id=4, value_id=5, key_type=StringType(), value_type=StringType()),
                required=False,
            ),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("map.key", "map.value")
    assert "Cannot move fields in non-struct type: map<string, string>" in str(exc_info.value)


@pytest.mark.integration
def test_move_primitive_map_value_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
            NestedField(
                field_id=3,
                name="map",
                field_type=MapType(key_id=4, value_id=5, key_type=StringType(), value_type=StructType()),
                required=False,
            ),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("map.value", "map.key")
    assert "Cannot move fields in non-struct type: map<string, struct<>>" in str(exc_info.value)


@pytest.mark.integration
def test_move_top_level_between_structs_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="a", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="b", field_type=IntegerType(), required=True),
            NestedField(
                field_id=3,
                name="struct",
                field_type=StructType(
                    NestedField(field_id=4, name="x", field_type=IntegerType(), required=True),
                    NestedField(field_id=5, name="y", field_type=IntegerType(), required=True),
                ),
                required=False,
            ),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("a", "struct.x")
    assert "Cannot move field a to a different struct" in str(exc_info.value)


@pytest.mark.integration
def test_move_between_structs_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(
                field_id=1,
                name="s1",
                field_type=StructType(
                    NestedField(field_id=3, name="a", field_type=IntegerType(), required=True),
                    NestedField(field_id=4, name="b", field_type=IntegerType(), required=True),
                ),
                required=False,
            ),
            NestedField(
                field_id=2,
                name="s2",
                field_type=StructType(
                    NestedField(field_id=5, name="x", field_type=IntegerType(), required=True),
                    NestedField(field_id=6, name="y", field_type=IntegerType(), required=True),
                ),
                required=False,
            ),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update:
            update.move_before("s2.x", "s1.a")

    assert "Cannot move field s2.x to a different struct" in str(exc_info.value)


@pytest.mark.integration
def test_add_existing_identifier_fields(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema() as update_schema:
        update_schema.set_identifier_fields("foo")

    assert tbl.schema().identifier_field_names() == {"foo"}


@pytest.mark.integration
def test_add_new_identifiers_field_columns(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column("new_field", StringType(), required=True)
        update_schema.set_identifier_fields("foo", "new_field")

    assert tbl.schema().identifier_field_names() == {"foo", "new_field"}


@pytest.mark.integration
def test_add_new_identifiers_field_columns_out_of_order(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column("new_field", StringType(), required=True)
        update_schema.set_identifier_fields("foo", "new_field")

    assert tbl.schema().identifier_field_names() == {"foo", "new_field"}


@pytest.mark.integration
def test_add_nested_identifier_field_columns(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column(
            "required_struct", StructType(NestedField(field_id=3, name="field", type=StringType(), required=True)), required=True
        )

    with tbl.update_schema() as update_schema:
        update_schema.set_identifier_fields("required_struct.field")

    assert tbl.schema().identifier_field_names() == {"required_struct.field"}


@pytest.mark.integration
def test_add_nested_identifier_field_columns_single_transaction(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column(
            "new", StructType(NestedField(field_id=3, name="field", type=StringType(), required=True)), required=True
        )
        update_schema.set_identifier_fields("new.field")

    assert tbl.schema().identifier_field_names() == {"new.field"}


@pytest.mark.integration
def test_add_nested_nested_identifier_field_columns(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column(
            "new",
            StructType(
                NestedField(
                    field_id=3,
                    name="field",
                    type=StructType(NestedField(field_id=4, name="nested", type=StringType(), required=True)),
                    required=True,
                )
            ),
            required=True,
        )
        update_schema.set_identifier_fields("new.field.nested")

    assert tbl.schema().identifier_field_names() == {"new.field.nested"}


@pytest.mark.integration
def test_add_dotted_identifier_field_columns(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column(("dot.field",), StringType(), required=True)
        update_schema.set_identifier_fields("dot.field")

    assert tbl.schema().identifier_field_names() == {"dot.field"}


@pytest.mark.integration
def test_remove_identifier_fields(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.add_column(("new_field",), StringType(), required=True)
        update_schema.add_column(("new_field2",), StringType(), required=True)
        update_schema.set_identifier_fields("foo", "new_field", "new_field2")

    assert tbl.schema().identifier_field_names() == {"foo", "new_field", "new_field2"}

    with tbl.update_schema(allow_incompatible_changes=True) as update_schema:
        update_schema.set_identifier_fields()

    assert tbl.schema().identifier_field_names() == set()


@pytest.mark.integration
def test_set_identifier_field_fails_schema(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
            NestedField(field_id=2, name="float", field_type=FloatType(), required=True),
            NestedField(field_id=3, name="double", field_type=DoubleType(), required=True),
            identifier_field_ids=[],
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update_schema:
            update_schema.set_identifier_fields("id")

    assert "Identifier field 1 invalid: not a required field" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update_schema:
            update_schema.set_identifier_fields("float")

    assert "Identifier field 2 invalid: must not be float or double field" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update_schema:
            update_schema.set_identifier_fields("double")

    assert "Identifier field 3 invalid: must not be float or double field" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as update_schema:
            update_schema.set_identifier_fields("unknown")

    assert "Cannot find identifier field unknown. In case of deletion, update the identifier fields first." in str(exc_info.value)


@pytest.mark.integration
def test_set_identifier_field_fails(nested_table: Table) -> None:
    with pytest.raises(ValueError) as exc_info:
        with nested_table.update_schema() as update_schema:
            update_schema.set_identifier_fields("location")

    assert "Identifier field 6 invalid: not a primitive type field" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with nested_table.update_schema() as update_schema:
            update_schema.set_identifier_fields("baz")

    assert "Identifier field 3 invalid: not a required field" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        with nested_table.update_schema() as update_schema:
            update_schema.set_identifier_fields("person.name")

    assert "Identifier field 16 invalid: not a required field" in str(exc_info.value)


@pytest.mark.integration
def test_delete_identifier_field_columns(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema() as schema_update:
        schema_update.delete_column("foo")
        schema_update.set_identifier_fields()

    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema() as schema_update:
        schema_update.set_identifier_fields()
        schema_update.delete_column("foo")


@pytest.mark.integration
def test_delete_containing_nested_identifier_field_columns_fails(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema(allow_incompatible_changes=True) as schema_update:
        schema_update.add_column(
            "out", StructType(NestedField(field_id=3, name="nested", field_type=StringType(), required=True)), required=True
        )
        schema_update.set_identifier_fields("out.nested")

    assert tbl.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=True),
        NestedField(
            field_id=2,
            name="out",
            field_type=StructType(NestedField(field_id=3, name="nested", field_type=StringType(), required=True)),
            required=True,
        ),
        identifier_field_ids=[3],
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.update_schema() as schema_update:
            schema_update.delete_column("out")

    assert "Cannot find identifier field out.nested. In case of deletion, update the identifier fields first." in str(exc_info)


@pytest.mark.integration
def test_rename_identifier_fields(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(NestedField(field_id=1, name="foo", field_type=StringType(), required=True), identifier_field_ids=[1]),
    )

    with tbl.update_schema() as schema_update:
        schema_update.rename_column("foo", "bar")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"bar"}


@pytest.mark.integration
def test_move_identifier_fields(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
            identifier_field_ids=[1],
        ),
    )

    with tbl.update_schema() as update:
        update.move_before("data", "id")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"id"}

    with tbl.update_schema() as update:
        update.move_after("id", "data")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"id"}

    with tbl.update_schema() as update:
        update.move_first("data")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"id"}


@pytest.mark.integration
def test_move_identifier_fields_case_insensitive(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=True),
            identifier_field_ids=[1],
        ),
    )

    with tbl.update_schema(case_sensitive=False) as update:
        update.move_before("DATA", "ID")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"id"}

    with tbl.update_schema(case_sensitive=False) as update:
        update.move_after("ID", "DATA")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"id"}

    with tbl.update_schema(case_sensitive=False) as update:
        update.move_first("DATA")

    assert tbl.schema().identifier_field_ids == [1]
    assert tbl.schema().identifier_field_names() == {"id"}


@pytest.mark.integration
def test_two_add_schemas_in_a_single_transaction(catalog: Catalog) -> None:
    tbl = _create_table_with_schema(
        catalog,
        Schema(
            NestedField(field_id=1, name="foo", field_type=StringType()),
        ),
    )

    with pytest.raises(ValueError) as exc_info:
        with tbl.transaction() as tr:
            with tr.update_schema() as update:
                update.add_column("bar", field_type=StringType())
            with tr.update_schema() as update:
                update.add_column("baz", field_type=StringType())

    assert "Updates in a single commit need to be unique, duplicate: <class 'pyiceberg.table.AddSchemaUpdate'>" in str(
        exc_info.value
    )

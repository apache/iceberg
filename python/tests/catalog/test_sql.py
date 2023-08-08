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

import os
from pathlib import Path
from typing import Generator, List

import pytest
from pytest import TempPathFactory
from sqlalchemy.exc import ArgumentError, IntegrityError

from pyiceberg.catalog import Identifier
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import IdentityTransform


@pytest.fixture(name="warehouse", scope="session")
def fixture_warehouse(tmp_path_factory: TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("test_sql")


@pytest.fixture(name="random_identifier")
def fixture_random_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    os.makedirs(f"{warehouse}/{database_name}.db/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name


@pytest.fixture(name="another_random_identifier")
def fixture_another_random_identifier(warehouse: Path, database_name: str, table_name: str) -> Identifier:
    database_name = database_name + "_new"
    table_name = table_name + "_new"
    os.makedirs(f"{warehouse}/{database_name}.db/{table_name}/metadata/", exist_ok=True)
    return database_name, table_name


@pytest.fixture(name="test_catalog", scope="module")
def fixture_test_catalog(warehouse: Path) -> Generator[SqlCatalog, None, None]:
    props = {
        "uri": "sqlite+pysqlite:///:memory:",
        "warehouse": f"file://{warehouse}",
    }
    test_catalog = SqlCatalog("test_sql_catalog", **props)
    test_catalog.create_tables()
    yield test_catalog
    test_catalog.destroy_tables()


def test_creation_with_no_uri() -> None:
    with pytest.raises(NoSuchPropertyException):
        SqlCatalog("test_ddb_catalog", not_uri="unused")


def test_creation_with_unsupported_uri() -> None:
    with pytest.raises(ArgumentError):
        SqlCatalog("test_ddb_catalog", uri="unsupported:xxx")


def test_create_tables_idempotency(test_catalog: SqlCatalog) -> None:
    # Second initialization should not fail even if tables are already created
    test_catalog.create_tables()
    test_catalog.create_tables()


def test_create_table_default_sort_order(
    test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier
) -> None:
    database_name, _table_name = random_identifier
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(random_identifier, table_schema_nested)
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"
    test_catalog.drop_table(random_identifier)


def test_create_table_custom_sort_order(
    test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier
) -> None:
    database_name, _table_name = random_identifier
    test_catalog.create_namespace(database_name)
    order = SortOrder(SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_FIRST))
    table = test_catalog.create_table(random_identifier, table_schema_nested, sort_order=order)
    given_sort_order = table.sort_order()
    assert given_sort_order.order_id == 1, "Order ID must match"
    assert len(given_sort_order.fields) == 1, "Order must have 1 field"
    assert given_sort_order.fields[0].direction == SortDirection.ASC, "Direction must match"
    assert given_sort_order.fields[0].null_order == NullOrder.NULLS_FIRST, "Null order must match"
    assert isinstance(given_sort_order.fields[0].transform, IdentityTransform), "Transform must match"
    test_catalog.drop_table(random_identifier)


def test_create_table_with_default_warehouse_location(
    warehouse: Path, test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier
) -> None:
    database_name, _table_name = random_identifier
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(random_identifier, table_schema_nested)
    table = test_catalog.load_table(random_identifier)
    assert table.identifier == (test_catalog.name,) + random_identifier
    assert table.metadata_location.startswith(f"file://{warehouse}")
    assert os.path.exists(table.metadata_location[len("file://") :])
    test_catalog.drop_table(random_identifier)


def test_create_duplicated_table(test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(random_identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table(random_identifier, table_schema_nested)


def test_create_table_with_non_existing_namespace(test_catalog: SqlCatalog, table_schema_nested: Schema, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier, table_schema_nested)


def test_create_table_without_namespace(test_catalog: SqlCatalog, table_schema_nested: Schema, table_name: str) -> None:
    with pytest.raises(ValueError):
        test_catalog.create_table(table_name, table_schema_nested)


def test_load_table(test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(random_identifier, table_schema_nested)
    loaded_table = test_catalog.load_table(random_identifier)
    assert table.identifier == loaded_table.identifier
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


def test_drop_table(test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, _table_name = random_identifier
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (test_catalog.name,) + random_identifier
    test_catalog.drop_table(random_identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(random_identifier)


def test_drop_table_that_does_not_exist(test_catalog: SqlCatalog, random_identifier: Identifier) -> None:
    with pytest.raises(NoSuchTableError):
        test_catalog.drop_table(random_identifier)


def test_rename_table(
    test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    to_database_name, _to_table_name = another_random_identifier
    test_catalog.create_namespace(from_database_name)
    test_catalog.create_namespace(to_database_name)
    table = test_catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (test_catalog.name,) + random_identifier
    test_catalog.rename_table(random_identifier, another_random_identifier)
    new_table = test_catalog.load_table(another_random_identifier)
    assert new_table.identifier == (test_catalog.name,) + another_random_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(random_identifier)


def test_rename_table_to_existing_one(
    test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    to_database_name, _to_table_name = another_random_identifier
    test_catalog.create_namespace(from_database_name)
    test_catalog.create_namespace(to_database_name)
    table = test_catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (test_catalog.name,) + random_identifier
    new_table = test_catalog.create_table(another_random_identifier, table_schema_nested)
    assert new_table.identifier == (test_catalog.name,) + another_random_identifier
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.rename_table(random_identifier, another_random_identifier)


def test_rename_missing_table(
    test_catalog: SqlCatalog, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    to_database_name, _to_table_name = another_random_identifier
    test_catalog.create_namespace(to_database_name)
    with pytest.raises(NoSuchTableError):
        test_catalog.rename_table(random_identifier, another_random_identifier)


def test_rename_table_to_missing_namespace(
    test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    from_database_name, _from_table_name = random_identifier
    test_catalog.create_namespace(from_database_name)
    table = test_catalog.create_table(random_identifier, table_schema_nested)
    assert table.identifier == (test_catalog.name,) + random_identifier
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.rename_table(random_identifier, another_random_identifier)


def test_list_tables(
    test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier, another_random_identifier: Identifier
) -> None:
    database_name_1, _table_name_1 = random_identifier
    database_name_2, _table_name_2 = another_random_identifier
    test_catalog.create_namespace(database_name_1)
    test_catalog.create_namespace(database_name_2)
    test_catalog.create_table(random_identifier, table_schema_nested)
    test_catalog.create_table(another_random_identifier, table_schema_nested)
    identifier_list = test_catalog.list_tables(database_name_1)
    assert len(identifier_list) == 1
    assert random_identifier in identifier_list

    identifier_list = test_catalog.list_tables(database_name_2)
    assert len(identifier_list) == 1
    assert another_random_identifier in identifier_list


def test_create_namespace(test_catalog: SqlCatalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()


def test_create_duplicate_namespace(test_catalog: SqlCatalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(database_name)


def test_create_namespaces_sharing_same_prefix(test_catalog: SqlCatalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name + "_1")
    # Second namespace is a prefix of the first one, make sure it can be added.
    test_catalog.create_namespace(database_name)


def test_create_namespace_with_comment_and_location(test_catalog: SqlCatalog, database_name: str) -> None:
    test_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


def test_create_namespace_with_null_properties(test_catalog: SqlCatalog, database_name: str) -> None:
    with pytest.raises(IntegrityError):
        test_catalog.create_namespace(namespace=database_name, properties={None: "value"})  # type: ignore

    with pytest.raises(IntegrityError):
        test_catalog.create_namespace(namespace=database_name, properties={"key": None})  # type: ignore


def test_list_namespaces(test_catalog: SqlCatalog, database_list: List[str]) -> None:
    for database_name in database_list:
        test_catalog.create_namespace(database_name)
    db_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in db_list
        assert len(test_catalog.list_namespaces(database_name)) == 1


def test_list_non_existing_namespaces(test_catalog: SqlCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.list_namespaces("does_not_exist")


def test_drop_namespace(test_catalog: SqlCatalog, table_schema_nested: Schema, random_identifier: Identifier) -> None:
    database_name, table_name = random_identifier
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)
    test_catalog.drop_table((database_name, table_name))
    test_catalog.drop_namespace(database_name)
    assert (database_name,) not in test_catalog.list_namespaces()


def test_load_namespace_properties(test_catalog: SqlCatalog, database_name: str) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }

    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


def test_load_empty_namespace_properties(test_catalog: SqlCatalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {"exists": "true"}


def test_update_namespace_properties(test_catalog: SqlCatalog, database_name: str) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog.create_namespace(database_name, test_properties)
    update_report = test_catalog.update_namespace_properties(database_name, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == test_catalog.load_namespace_properties(database_name)["comment"]

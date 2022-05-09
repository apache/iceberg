#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import pytest

from iceberg.catalog.base import AlreadyExistsError, TableNotFoundError
from iceberg.catalog.in_memory import InMemoryCatalog
from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, TableSpec


@pytest.fixture
def catalog() -> InMemoryCatalog:
    return InMemoryCatalog("test.in.memory.catalog", {"test.key": "test.value"})


@pytest.fixture
def table_spec() -> TableSpec:
    return TableSpec(
        namespace="com.organization.department",
        name="my_table",
        schema=Schema(schema_id=1),
        location="protocol://some/location",
        partition_spec=PartitionSpec(),
        properties={"key1": "value1", "key2": "value2"},
    )


def test_create_table(catalog: InMemoryCatalog, table_spec: TableSpec):
    table = catalog.create_table(table_spec)
    assert table
    assert table.spec is table_spec


def test_create_table_raises_error_when_table_already_exists(catalog: InMemoryCatalog, table_spec: TableSpec):
    # Given
    catalog.create_table(table_spec)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_table(table_spec)


def test_table(catalog: InMemoryCatalog, table_spec: TableSpec):
    # Given
    catalog.create_table(table_spec)
    # When
    table = catalog.table(table_spec.namespace, table_spec.name)
    # Then
    assert table
    assert table.spec is table_spec


def test_table_raises_error_on_table_not_found(catalog: InMemoryCatalog, table_spec: TableSpec):
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_drop_table(catalog: InMemoryCatalog, table_spec: TableSpec):
    # Given
    catalog.create_table(table_spec)
    # When
    catalog.drop_table(table_spec.namespace, table_spec.name)
    # Then
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_drop_table_that_does_not_exist_raise_error(catalog: InMemoryCatalog, table_spec: TableSpec):
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_rename_table(catalog: InMemoryCatalog, table_spec: TableSpec):
    # Given
    catalog.create_table(table_spec)

    # When
    new_table = "new_table"
    new_namespace = "new.namespace"
    table = catalog.rename_table(table_spec.namespace, table_spec.name, new_namespace, new_table)

    # Then
    assert table
    assert table.spec.namespace is new_namespace
    assert table.spec.name is new_table

    # And
    table = catalog.table(new_namespace, new_table)
    assert table
    assert table.spec.namespace is new_namespace
    assert table.spec.name is new_table

    # And
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_replace_table_that_exists(catalog: InMemoryCatalog, table_spec: TableSpec):
    # Given
    catalog.create_table(table_spec)

    # When
    new_table_spec = TableSpec(
        namespace=table_spec.namespace,
        name=table_spec.name,
        schema=Schema(schema_id=2),
        location="protocol://some/location",
        partition_spec=PartitionSpec(),
        properties={"key1": "value1", "key2": "value2"},
    )
    table = catalog.replace_table(new_table_spec)

    # Then
    assert table
    assert table.spec.schema is new_table_spec.schema

    # And
    table = catalog.table(new_table_spec.namespace, new_table_spec.name)
    assert table
    assert table.spec.schema is new_table_spec.schema


def test_replace_table_that_does_not_exist_raises_error(catalog: InMemoryCatalog, table_spec: TableSpec):
    with pytest.raises(TableNotFoundError):
        catalog.replace_table(table_spec)

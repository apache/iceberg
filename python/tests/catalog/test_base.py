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

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, cast

import pytest

from iceberg.catalog.base import (
    AlreadyExistsError,
    Catalog,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    TableNotFoundError,
)
from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, Table


@dataclass(frozen=True)
class InMemoryTable(Table):
    """An in-memory table representation for testing purposes.

    Usage:
        table_spec = InMemoryTable(
            namespace = ("com", "organization", "department"),
            name = "my_table",
            schema = Schema(),
            location = "protocol://some/location",  // Optional
            partition_spec = PartitionSpec(),       // Optional
            properties = [                          // Optional
                "key1": "value1",
                "key2": "value2",
            ]
        )
    """

    namespace: Tuple[str, ...]
    name: str
    schema: Schema
    location: str
    partition_spec: PartitionSpec
    properties: Dict[str, str]


class InMemoryCatalog(Catalog):
    """An in-memory catalog implementation for testing purposes."""

    __tables: Dict[Tuple[Tuple[str, ...], str], InMemoryTable]
    __namespaces: Dict[Tuple[str, ...], Dict[str, str]]

    def __init__(self, name: str, properties: Dict[str, str]):
        super().__init__(name, properties)
        self.__tables = {}
        self.__namespaces = {}

    def create_table(
        self,
        *,
        namespace: Tuple[str, ...],
        name: str,
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:

        if (namespace, name) in self.__tables:
            raise AlreadyExistsError(name)
        else:
            if namespace not in self.__namespaces:
                self.__namespaces[namespace] = {}

            table = InMemoryTable(
                namespace=namespace,
                name=name,
                schema=schema if schema else None,
                location=location if location else None,
                partition_spec=partition_spec if partition_spec else None,
                properties=properties if properties else {},
            )
            self.__tables[(namespace, name)] = table
            return table

    def table(self, namespace: Tuple[str, ...], name: str) -> Table:
        try:
            return self.__tables[(namespace, name)]
        except KeyError:
            raise TableNotFoundError(name)

    def drop_table(self, namespace: Tuple[str, ...], name: str, purge: bool = True) -> None:
        try:
            self.__tables.pop((namespace, name))
        except KeyError:
            raise TableNotFoundError(name)

    def rename_table(self, from_namespace: Tuple[str, ...], from_name: str, to_namespace: Tuple[str, ...], to_name: str) -> Table:
        try:
            table = self.__tables.pop((from_namespace, from_name))
        except KeyError:
            raise TableNotFoundError(from_name)

        renamed_table = InMemoryTable(
            namespace=to_namespace,
            name=to_name,
            schema=table.schema,
            location=table.location,
            partition_spec=table.partition_spec,
            properties=table.properties,
        )
        if to_namespace not in self.__namespaces:
            self.__namespaces[to_namespace] = {}

        self.__tables[(to_namespace, to_name)] = renamed_table
        return renamed_table

    def replace_table(
        self,
        *,
        namespace: Tuple[str, ...],
        name: str,
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:

        try:
            table = self.__tables.pop((namespace, name))
        except KeyError:
            raise TableNotFoundError(name)

        replaced_table = InMemoryTable(
            namespace=namespace if namespace else table.namespace,
            name=name if name else table.name,
            schema=schema if schema else table.schema,
            location=location if location else table.location,
            partition_spec=partition_spec if partition_spec else table.partition_spec,
            properties={**table.properties, **properties},
        )
        self.__tables[(replaced_table.namespace, replaced_table.name)] = replaced_table
        return replaced_table

    def create_namespace(self, namespace: Tuple[str, ...], properties: Optional[Dict[str, str]] = None) -> None:
        if namespace in self.__namespaces:
            raise AlreadyExistsError(namespace)
        else:
            self.__namespaces[namespace] = properties if properties else {}

    def drop_namespace(self, namespace: Tuple[str, ...]) -> None:
        if [table_name_tuple for table_name_tuple in self.__tables.keys() if namespace in table_name_tuple]:
            raise NamespaceNotEmptyError(namespace)
        try:
            self.__namespaces.pop(namespace)
        except KeyError:
            raise NamespaceNotFoundError(namespace)

    def list_tables(self, namespace: Optional[Tuple[str, ...]] = None) -> List[Tuple[Tuple[str, ...], str]]:
        if namespace:
            list_tables = [table_name_tuple for table_name_tuple in self.__tables.keys() if namespace in table_name_tuple]
        else:
            list_tables = list(self.__tables.keys())

        # Casting to make mypy happy
        return cast(List[Tuple[Tuple[str, ...], str]], list_tables)

    def list_namespaces(self) -> List[Tuple[str, ...]]:
        return list(self.__namespaces.keys())

    def load_namespace_metadata(self, namespace: Tuple[str, ...]) -> Dict[str, str]:
        try:
            return self.__namespaces[namespace]
        except KeyError:
            raise NamespaceNotFoundError(namespace)

    def set_namespace_metadata(self, namespace: Tuple[str, ...], metadata: Dict[str, str]) -> None:
        if namespace in self.__namespaces:
            self.__namespaces[namespace] = metadata
        else:
            raise NamespaceNotFoundError(namespace)


@pytest.fixture
def catalog() -> InMemoryCatalog:
    return InMemoryCatalog("test.in.memory.catalog", {"test.key": "test.value"})


@pytest.fixture
def table_spec() -> InMemoryTable:
    return InMemoryTable(
        namespace=("com", "organization", "department"),
        name="my_table",
        schema=Schema(schema_id=1),
        location="protocol://some/location",
        partition_spec=PartitionSpec(),
        properties={"key1": "value1", "key2": "value2"},
    )


def given_catalog_has_a_table(catalog: InMemoryCatalog, table_spec: InMemoryTable) -> Table:
    return catalog.create_table(
        namespace=table_spec.namespace,
        name=table_spec.name,
        schema=table_spec.schema,
        location=table_spec.location,
        partition_spec=table_spec.partition_spec,
        properties=table_spec.properties,
    )


def test_create_table(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    table = catalog.create_table(
        namespace=table_spec.namespace,
        name=table_spec.name,
        schema=table_spec.schema,
        location=table_spec.location,
        partition_spec=table_spec.partition_spec,
        properties=table_spec.properties,
    )
    assert table == table_spec


def test_create_table_raises_error_when_table_already_exists(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_table(
            namespace=table_spec.namespace,
            name=table_spec.name,
            schema=table_spec.schema,
            location=table_spec.location,
            partition_spec=table_spec.partition_spec,
            properties=table_spec.properties,
        )


def test_table(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)
    # When
    table = catalog.table(table_spec.namespace, table_spec.name)
    # Then
    assert table == table_spec


def test_table_raises_error_on_table_not_found(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_drop_table(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)
    # When
    catalog.drop_table(table_spec.namespace, table_spec.name)
    # Then
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_drop_table_that_does_not_exist_raise_error(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_rename_table(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)

    # When
    new_table = "new_table"
    new_namespace = ("new", "namespace")
    table = catalog.rename_table(table_spec.namespace, table_spec.name, new_namespace, new_table)
    table = cast(InMemoryTable, table)

    # Then
    assert table
    assert table.namespace == new_namespace
    assert table.name == new_table

    # And
    table = catalog.table(new_namespace, new_table)
    table = cast(InMemoryTable, table)
    assert table
    assert table.namespace == new_namespace
    assert table.name == new_table

    # And
    assert new_namespace in catalog.list_namespaces()

    # And
    with pytest.raises(TableNotFoundError):
        catalog.table(table_spec.namespace, table_spec.name)


def test_replace_table_that_exists(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)

    # When
    schema = Schema(schema_id=2)
    new_properties = {"key2": "value21", "key3": "value3"}
    table = catalog.replace_table(
        namespace=table_spec.namespace,
        name=table_spec.name,
        schema=schema,
        location="protocol://some/location",
        partition_spec=PartitionSpec(),
        properties=new_properties,
    )
    table = cast(InMemoryTable, table)

    # Then
    assert table
    assert table.schema == schema
    assert table.properties == {**table_spec.properties, **new_properties}

    # And
    table = catalog.table(table_spec.namespace, table_spec.name)
    table = cast(InMemoryTable, table)
    assert table
    assert table.schema == schema


def test_replace_table_that_does_not_exist_raises_error(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    with pytest.raises(TableNotFoundError):
        catalog.replace_table(
            namespace=table_spec.namespace,
            name=table_spec.name,
            schema=table_spec.schema,
            location=table_spec.location,
            partition_spec=table_spec.partition_spec,
            properties=table_spec.properties,
        )


def test_create_namespace(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # When
    catalog.create_namespace(table_spec.namespace, table_spec.properties)

    # Then
    assert table_spec.namespace in catalog.list_namespaces()
    assert table_spec.properties == catalog.load_namespace_metadata(table_spec.namespace)


def test_create_namespace_raises_error_on_existing_namespace(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    catalog.create_namespace(table_spec.namespace, table_spec.properties)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_namespace(table_spec.namespace, table_spec.properties)


def test_get_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    with pytest.raises(NamespaceNotFoundError):
        catalog.load_namespace_metadata(table_spec.namespace)


def test_list_namespaces(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    catalog.create_namespace(table_spec.namespace, table_spec.properties)
    # When
    namespaces = catalog.list_namespaces()
    # Then
    assert table_spec.namespace in namespaces


def test_drop_namespace(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    catalog.create_namespace(table_spec.namespace, table_spec.properties)
    # When
    catalog.drop_namespace(table_spec.namespace)
    # Then
    assert table_spec.namespace not in catalog.list_namespaces()


def test_drop_namespace_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    with pytest.raises(NamespaceNotFoundError):
        catalog.drop_namespace(table_spec.namespace)


def test_drop_namespace_raises_error_when_namespace_not_empty(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)
    # When
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace(table_spec.namespace)


def test_list_tables(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)
    # When
    tables = catalog.list_tables()
    # Then
    assert tables
    assert (table_spec.namespace, table_spec.name) in tables


def test_list_tables_under_a_namespace(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    given_catalog_has_a_table(catalog, table_spec)
    new_namespace = ("new", "namespace")
    catalog.create_namespace(new_namespace)
    # When
    all_tables = catalog.list_tables()
    new_namespace_tables = catalog.list_tables(new_namespace)
    # Then
    assert all_tables
    assert (table_spec.namespace, table_spec.name) in all_tables
    assert new_namespace_tables == []


def test_set_namespace_metadata(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    # Given
    catalog.create_namespace(table_spec.namespace, table_spec.properties)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    catalog.set_namespace_metadata(table_spec.namespace, new_metadata)

    # Then
    assert table_spec.namespace in catalog.list_namespaces()
    assert catalog.load_namespace_metadata(table_spec.namespace) == new_metadata


def test_set_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog, table_spec: InMemoryTable):
    with pytest.raises(NamespaceNotFoundError):
        catalog.set_namespace_metadata(table_spec.namespace, table_spec.properties)

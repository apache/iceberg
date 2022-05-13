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

from typing import Dict, List, Optional, Tuple, cast

import pytest

from iceberg.catalog.base import Catalog
from iceberg.exceptions import (
    AlreadyExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    TableNotFoundError,
)
from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, Table


class InMemoryCatalog(Catalog):
    """An in-memory catalog implementation for testing purposes."""

    __tables: Dict[Tuple[Tuple[str, ...], str], Table]
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
            raise AlreadyExistsError(f"Table {name} already exists in namespace {namespace}")
        else:
            if namespace not in self.__namespaces:
                self.__namespaces[namespace] = {}

            table = Table()
            self.__tables[(namespace, name)] = table
            return table

    def table(self, namespace: Tuple[str, ...], name: str) -> Table:
        try:
            return self.__tables[(namespace, name)]
        except KeyError:
            raise TableNotFoundError(f"Table {name} not found in the catalog")

    def drop_table(self, namespace: Tuple[str, ...], name: str, purge: bool = True) -> None:
        try:
            self.__tables.pop((namespace, name))
        except KeyError:
            raise TableNotFoundError(f"Table {name} not found in the catalog")

    def rename_table(self, from_namespace: Tuple[str, ...], from_name: str, to_namespace: Tuple[str, ...], to_name: str) -> Table:
        try:
            self.__tables.pop((from_namespace, from_name))
        except KeyError:
            raise TableNotFoundError(f"Table {from_name} not found in the catalog")

        renamed_table = Table()
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
            self.__tables.pop((namespace, name))
        except KeyError:
            raise TableNotFoundError(f"Table {name} not found in the catalog")

        replaced_table = Table()
        self.__tables[(namespace, name)] = replaced_table
        return replaced_table

    def create_namespace(self, namespace: Tuple[str, ...], properties: Optional[Dict[str, str]] = None) -> None:
        if namespace in self.__namespaces:
            raise AlreadyExistsError(f"Namespace {namespace} already exists")
        else:
            self.__namespaces[namespace] = properties if properties else {}

    def drop_namespace(self, namespace: Tuple[str, ...]) -> None:
        if [table_name_tuple for table_name_tuple in self.__tables.keys() if namespace in table_name_tuple]:
            raise NamespaceNotEmptyError(f"Namespace {namespace} not empty")
        try:
            self.__namespaces.pop(namespace)
        except KeyError:
            raise NamespaceNotFoundError(f"Namespace {namespace} not found in the catalog")

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
            raise NamespaceNotFoundError(f"Namespace {namespace} not found in the catalog")

    def set_namespace_metadata(self, namespace: Tuple[str, ...], metadata: Dict[str, str]) -> None:
        if namespace in self.__namespaces:
            self.__namespaces[namespace] = metadata
        else:
            raise NamespaceNotFoundError(f"Namespace {namespace} not found in the catalog")


TEST_TABLE_NAMESPACE = ("com", "organization", "department")
TEST_TABLE_NAME = "my_table"
TEST_TABLE_SCHEMA = Schema(schema_id=1)
TEST_TABLE_LOCATION = "protocol://some/location"
TEST_TABLE_PARTITION_SPEC = PartitionSpec()
TEST_TABLE_PROPERTIES = {"key1": "value1", "key2": "value2"}


def given_catalog_has_a_table(catalog: InMemoryCatalog) -> Table:
    return catalog.create_table(
        namespace=TEST_TABLE_NAMESPACE,
        name=TEST_TABLE_NAME,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )


def test_create_table(catalog: InMemoryCatalog):
    table = catalog.create_table(
        namespace=TEST_TABLE_NAMESPACE,
        name=TEST_TABLE_NAME,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME) == table


def test_create_table_raises_error_when_table_already_exists(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_table(
            namespace=TEST_TABLE_NAMESPACE,
            name=TEST_TABLE_NAME,
            schema=TEST_TABLE_SCHEMA,
            location=TEST_TABLE_LOCATION,
            partition_spec=TEST_TABLE_PARTITION_SPEC,
            properties=TEST_TABLE_PROPERTIES,
        )


def test_table(catalog: InMemoryCatalog):
    # Given
    given_table = given_catalog_has_a_table(catalog)
    # When
    table = catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)
    # Then
    assert table == given_table


def test_table_raises_error_on_table_not_found(catalog: InMemoryCatalog):
    with pytest.raises(TableNotFoundError):
        catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)


def test_drop_table(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.drop_table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)
    # Then
    with pytest.raises(TableNotFoundError):
        catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)


def test_drop_table_that_does_not_exist_raise_error(catalog: InMemoryCatalog):
    with pytest.raises(TableNotFoundError):
        catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)


def test_rename_table(catalog: InMemoryCatalog):
    # Given
    given_table = given_catalog_has_a_table(catalog)

    # When
    new_table = "new_table"
    new_namespace = ("new", "namespace")
    table = catalog.rename_table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME, new_namespace, new_table)

    # Then
    assert table
    assert table is not given_table

    # And
    table = catalog.table(new_namespace, new_table)
    assert table
    assert table is not given_table

    # And
    assert new_namespace in catalog.list_namespaces()

    # And
    with pytest.raises(TableNotFoundError):
        catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)


def test_replace_table_that_exists(catalog: InMemoryCatalog):
    # Given
    given_table = given_catalog_has_a_table(catalog)

    # When
    schema = Schema(schema_id=2)
    new_properties = {"key2": "value21", "key3": "value3"}
    table = catalog.replace_table(
        namespace=TEST_TABLE_NAMESPACE,
        name=TEST_TABLE_NAME,
        schema=schema,
        location="protocol://some/location",
        partition_spec=PartitionSpec(),
        properties=new_properties,
    )

    # Then
    assert table
    assert table is not given_table

    # And
    table = catalog.table(TEST_TABLE_NAMESPACE, TEST_TABLE_NAME)
    assert table
    assert table is not given_table


def test_replace_table_that_does_not_exist_raises_error(catalog: InMemoryCatalog):
    with pytest.raises(TableNotFoundError):
        catalog.replace_table(
            namespace=TEST_TABLE_NAMESPACE,
            name=TEST_TABLE_NAME,
            schema=TEST_TABLE_SCHEMA,
            location=TEST_TABLE_LOCATION,
            partition_spec=TEST_TABLE_PARTITION_SPEC,
            properties=TEST_TABLE_PROPERTIES,
        )


def test_create_namespace(catalog: InMemoryCatalog):
    # When
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert TEST_TABLE_PROPERTIES == catalog.load_namespace_metadata(TEST_TABLE_NAMESPACE)


def test_create_namespace_raises_error_on_existing_namespace(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)


def test_get_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog):
    with pytest.raises(NamespaceNotFoundError):
        catalog.load_namespace_metadata(TEST_TABLE_NAMESPACE)


def test_list_namespaces(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    namespaces = catalog.list_namespaces()
    # Then
    assert TEST_TABLE_NAMESPACE in namespaces


def test_drop_namespace(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    catalog.drop_namespace(TEST_TABLE_NAMESPACE)
    # Then
    assert TEST_TABLE_NAMESPACE not in catalog.list_namespaces()


def test_drop_namespace_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog):
    with pytest.raises(NamespaceNotFoundError):
        catalog.drop_namespace(TEST_TABLE_NAMESPACE)


def test_drop_namespace_raises_error_when_namespace_not_empty(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace(TEST_TABLE_NAMESPACE)


def test_list_tables(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    tables = catalog.list_tables()
    # Then
    assert tables
    assert (TEST_TABLE_NAMESPACE, TEST_TABLE_NAME) in tables


def test_list_tables_under_a_namespace(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    new_namespace = ("new", "namespace")
    catalog.create_namespace(new_namespace)
    # When
    all_tables = catalog.list_tables()
    new_namespace_tables = catalog.list_tables(new_namespace)
    # Then
    assert all_tables
    assert (TEST_TABLE_NAMESPACE, TEST_TABLE_NAME) in all_tables
    assert new_namespace_tables == []


def test_set_namespace_metadata(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    catalog.set_namespace_metadata(TEST_TABLE_NAMESPACE, new_metadata)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert catalog.load_namespace_metadata(TEST_TABLE_NAMESPACE) == new_metadata


def test_set_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog):
    with pytest.raises(NamespaceNotFoundError):
        catalog.set_namespace_metadata(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

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

from typing import Dict, List, Optional, Set, Union

import pytest

from iceberg.catalog.base import Catalog, Identifier, Metadata
from iceberg.exceptions import (
    AlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from iceberg.schema import Schema
from iceberg.table.base import PartitionSpec, Table


class InMemoryCatalog(Catalog):
    """An in-memory catalog implementation for testing purposes."""

    __tables: Dict[Identifier, Table]
    __namespaces: Dict[Identifier, Metadata]

    def __init__(self, name: str, properties: Metadata):
        super().__init__(name, properties)
        self.__tables = {}
        self.__namespaces = {}

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Metadata] = None,
    ) -> Table:

        identifier = InMemoryCatalog.identifier_to_tuple(identifier)
        namespace = InMemoryCatalog.namespace_from(identifier)
        name = InMemoryCatalog.name_from(identifier)

        if identifier in self.__tables:
            raise AlreadyExistsError(f"Table {name} already exists in namespace {namespace}")
        else:
            if namespace not in self.__namespaces:
                self.__namespaces[namespace] = {}

            table = Table()
            self.__tables[identifier] = table
            return table

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        identifier = InMemoryCatalog.identifier_to_tuple(identifier)
        namespace = InMemoryCatalog.namespace_from(identifier)
        name = InMemoryCatalog.name_from(identifier)
        try:
            return self.__tables[identifier]
        except KeyError:
            raise NoSuchTableError(f"Table {name} not found in the namespace {namespace}")

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        identifier = InMemoryCatalog.identifier_to_tuple(identifier)
        namespace = InMemoryCatalog.namespace_from(identifier)
        name = InMemoryCatalog.name_from(identifier)
        try:
            self.__tables.pop(identifier)
        except KeyError:
            raise NoSuchTableError(f"Table {name} not found in the namespace {namespace}")

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        from_identifier = InMemoryCatalog.identifier_to_tuple(from_identifier)
        from_namespace = InMemoryCatalog.namespace_from(from_identifier)
        from_name = InMemoryCatalog.name_from(from_identifier)
        try:
            self.__tables.pop(from_identifier)
        except KeyError:
            raise NoSuchTableError(f"Table {from_name} not found in the namespace {from_namespace}")

        renamed_table = Table()
        to_identifier = InMemoryCatalog.identifier_to_tuple(to_identifier)
        to_namespace = InMemoryCatalog.namespace_from(to_identifier)
        if to_namespace not in self.__namespaces:
            self.__namespaces[to_namespace] = {}

        self.__tables[to_identifier] = renamed_table
        return renamed_table

    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Metadata] = None) -> None:
        namespace = InMemoryCatalog.identifier_to_tuple(namespace)
        if namespace in self.__namespaces:
            raise AlreadyExistsError(f"Namespace {namespace} already exists")
        else:
            self.__namespaces[namespace] = properties if properties else {}

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace = InMemoryCatalog.identifier_to_tuple(namespace)
        if [table_identifier for table_identifier in self.__tables.keys() if namespace == table_identifier[:-1]]:
            raise NamespaceNotEmptyError(f"Namespace {namespace} not empty")
        try:
            self.__namespaces.pop(namespace)
        except KeyError:
            raise NoSuchNamespaceError(f"Namespace {namespace} not found in the catalog")

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        if namespace:
            namespace = InMemoryCatalog.identifier_to_tuple(namespace)
            list_tables = [table_identifier for table_identifier in self.__tables.keys() if namespace == table_identifier[:-1]]
        else:
            list_tables = list(self.__tables.keys())

        # Casting to make mypy happy
        return list_tables

    def list_namespaces(self) -> List[Identifier]:
        return list(self.__namespaces.keys())

    def load_namespace(self, namespace: Union[str, Identifier]) -> Metadata:
        namespace = InMemoryCatalog.identifier_to_tuple(namespace)
        try:
            return self.__namespaces[namespace]
        except KeyError:
            raise NoSuchNamespaceError(f"Namespace {namespace} not found in the catalog")

    def update_namespace_metadata(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Metadata] = None
    ) -> None:
        namespace = InMemoryCatalog.identifier_to_tuple(namespace)
        removals = {} if not removals else removals
        updates = [] if not updates else updates
        if namespace in self.__namespaces:
            [self.__namespaces[namespace].pop(key) for key in removals]
            self.__namespaces[namespace].update(updates)
        else:
            raise NoSuchNamespaceError(f"Namespace {namespace} not found in the catalog")

    @staticmethod
    def name_from(identifier: Union[str, Identifier]) -> str:
        return InMemoryCatalog.identifier_to_tuple(identifier)[-1]

    @staticmethod
    def namespace_from(identifier: Union[str, Identifier]) -> Identifier:
        return InMemoryCatalog.identifier_to_tuple(identifier)[:-1]

    @staticmethod
    def identifier_to_tuple(identifier: Union[str, Identifier]) -> Identifier:
        return identifier if isinstance(identifier, tuple) else tuple(str.split(identifier, "."))


TEST_TABLE_IDENTIFIER = ("com", "organization", "department", "my_table")
TEST_TABLE_NAMESPACE = ("com", "organization", "department")
TEST_TABLE_NAME = "my_table"
TEST_TABLE_SCHEMA = Schema(schema_id=1)
TEST_TABLE_LOCATION = "protocol://some/location"
TEST_TABLE_PARTITION_SPEC = PartitionSpec()
TEST_TABLE_PROPERTIES = {"key1": "value1", "key2": "value2"}


def given_catalog_has_a_table(catalog: InMemoryCatalog) -> Table:
    return catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )


def test_namespace_from_tuple():
    # Given
    identifier = ("com", "organization", "department", "my_table")
    # When
    namespace_from = InMemoryCatalog.namespace_from(identifier)
    # Then
    assert namespace_from == ("com", "organization", "department")


def test_namespace_from_str():
    # Given
    identifier = "com.organization.department.my_table"
    # When
    namespace_from = InMemoryCatalog.namespace_from(identifier)
    # Then
    assert namespace_from == ("com", "organization", "department")


def test_name_from_tuple():
    # Given
    identifier = ("com", "organization", "department", "my_table")
    # When
    name_from = InMemoryCatalog.name_from(identifier)
    # Then
    assert name_from == "my_table"


def test_name_from_str():
    # Given
    identifier = "com.organization.department.my_table"
    # When
    name_from = InMemoryCatalog.name_from(identifier)
    # Then
    assert name_from == "my_table"


def test_create_table(catalog: InMemoryCatalog):
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.load_table(TEST_TABLE_IDENTIFIER) == table


def test_create_table_raises_error_when_table_already_exists(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_table(
            identifier=TEST_TABLE_IDENTIFIER,
            schema=TEST_TABLE_SCHEMA,
            location=TEST_TABLE_LOCATION,
            partition_spec=TEST_TABLE_PARTITION_SPEC,
            properties=TEST_TABLE_PROPERTIES,
        )


def test_table(catalog: InMemoryCatalog):
    # Given
    given_table = given_catalog_has_a_table(catalog)
    # When
    table = catalog.load_table(TEST_TABLE_IDENTIFIER)
    # Then
    assert table == given_table


def test_table_raises_error_on_table_not_found(catalog: InMemoryCatalog):
    with pytest.raises(NoSuchTableError):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_drop_table(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.drop_table(TEST_TABLE_IDENTIFIER)
    # Then
    with pytest.raises(NoSuchTableError):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_drop_table_that_does_not_exist_raise_error(catalog: InMemoryCatalog):
    with pytest.raises(NoSuchTableError):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_purge_table(catalog: InMemoryCatalog):
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.purge_table(TEST_TABLE_IDENTIFIER)
    # Then
    with pytest.raises(NoSuchTableError):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_rename_table(catalog: InMemoryCatalog):
    # Given
    given_table = given_catalog_has_a_table(catalog)

    # When
    new_table = "new.namespace.new_table"
    table = catalog.rename_table(TEST_TABLE_IDENTIFIER, new_table)

    # Then
    assert table
    assert table is not given_table

    # And
    table = catalog.load_table(new_table)
    assert table
    assert table is not given_table

    # And
    assert ("new", "namespace") in catalog.list_namespaces()

    # And
    with pytest.raises(NoSuchTableError):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_create_namespace(catalog: InMemoryCatalog):
    # When
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert TEST_TABLE_PROPERTIES == catalog.load_namespace(TEST_TABLE_NAMESPACE)


def test_create_namespace_raises_error_on_existing_namespace(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    with pytest.raises(AlreadyExistsError):
        catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)


def test_get_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog):
    with pytest.raises(NoSuchNamespaceError):
        catalog.load_namespace(TEST_TABLE_NAMESPACE)


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
    with pytest.raises(NoSuchNamespaceError):
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
    assert TEST_TABLE_IDENTIFIER in tables


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
    assert TEST_TABLE_IDENTIFIER in all_tables
    assert new_namespace_tables == []


def test_update_namespace_metadata(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    catalog.update_namespace_metadata(TEST_TABLE_NAMESPACE, updates=new_metadata)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert new_metadata.items() <= catalog.load_namespace(TEST_TABLE_NAMESPACE).items()


def test_update_namespace_metadata_removals(catalog: InMemoryCatalog):
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    remove_metadata = {"key1"}
    catalog.update_namespace_metadata(TEST_TABLE_NAMESPACE, remove_metadata, new_metadata)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert new_metadata.items() <= catalog.load_namespace(TEST_TABLE_NAMESPACE).items()
    assert remove_metadata.isdisjoint(catalog.load_namespace(TEST_TABLE_NAMESPACE).keys())


def test_update_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog):
    with pytest.raises(NoSuchNamespaceError):
        catalog.update_namespace_metadata(TEST_TABLE_NAMESPACE, updates=TEST_TABLE_PROPERTIES)

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

from typing import (
    Dict,
    List,
    Optional,
    Set,
    Union,
)

import pytest

from pyiceberg.catalog import (
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import EMPTY_DICT
from tests.table.test_metadata import EXAMPLE_TABLE_METADATA_V1


class InMemoryCatalog(Catalog):
    """An in-memory catalog implementation for testing purposes."""

    __tables: Dict[Identifier, Table]
    __namespaces: Dict[Identifier, Properties]

    def __init__(self, name: str, **properties: str) -> None:
        super().__init__(name, **properties)
        self.__tables = {}
        self.__namespaces = {}

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:

        identifier = Catalog.identifier_to_tuple(identifier)
        namespace = Catalog.namespace_from(identifier)

        if identifier in self.__tables:
            raise TableAlreadyExistsError(f"Table already exists: {identifier}")
        else:
            if namespace not in self.__namespaces:
                self.__namespaces[namespace] = {}

            table = Table(
                identifier=identifier,
                metadata=EXAMPLE_TABLE_METADATA_V1,
                metadata_location=f's3://warehouse/{"/".join(identifier)}/metadata/metadata.json',
                io=load_file_io(),
            )
            self.__tables[identifier] = table
            return table

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        identifier = Catalog.identifier_to_tuple(identifier)
        try:
            return self.__tables[identifier]
        except KeyError as error:
            raise NoSuchTableError(f"Table does not exist: {identifier}") from error

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        identifier = Catalog.identifier_to_tuple(identifier)
        try:
            self.__tables.pop(identifier)
        except KeyError as error:
            raise NoSuchTableError(f"Table does not exist: {identifier}") from error

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        from_identifier = Catalog.identifier_to_tuple(from_identifier)
        try:
            table = self.__tables.pop(from_identifier)
        except KeyError as error:
            raise NoSuchTableError(f"Table does not exist: {from_identifier}") from error

        to_identifier = Catalog.identifier_to_tuple(to_identifier)
        to_namespace = Catalog.namespace_from(to_identifier)
        if to_namespace not in self.__namespaces:
            self.__namespaces[to_namespace] = {}

        self.__tables[to_identifier] = Table(
            identifier=to_identifier,
            metadata=table.metadata,
            metadata_location=table.metadata_location,
            io=load_file_io(),
        )
        return self.__tables[to_identifier]

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        namespace = Catalog.identifier_to_tuple(namespace)
        if namespace in self.__namespaces:
            raise NamespaceAlreadyExistsError(f"Namespace already exists: {namespace}")
        else:
            self.__namespaces[namespace] = properties if properties else {}

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace = Catalog.identifier_to_tuple(namespace)
        if [table_identifier for table_identifier in self.__tables.keys() if namespace == table_identifier[:-1]]:
            raise NamespaceNotEmptyError(f"Namespace is not empty: {namespace}")
        try:
            self.__namespaces.pop(namespace)
        except KeyError as error:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}") from error

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        if namespace:
            namespace = Catalog.identifier_to_tuple(namespace)
            list_tables = [table_identifier for table_identifier in self.__tables.keys() if namespace == table_identifier[:-1]]
        else:
            list_tables = list(self.__tables.keys())

        return list_tables

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        return list(self.__namespaces.keys())

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace = Catalog.identifier_to_tuple(namespace)
        try:
            return self.__namespaces[namespace]
        except KeyError as error:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}") from error

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        removed: Set[str] = set()
        updated: Set[str] = set()

        namespace = Catalog.identifier_to_tuple(namespace)
        if namespace in self.__namespaces:
            if removals:
                for key in removals:
                    if key in self.__namespaces[namespace]:
                        del self.__namespaces[namespace][key]
                        removed.add(key)
            if updates:
                for key, value in updates.items():
                    self.__namespaces[namespace][key] = value
                    updated.add(key)
        else:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

        expected_to_change = removed.difference(removals or set())

        return PropertiesUpdateSummary(
            removed=list(removed or []), updated=list(updates.keys() if updates else []), missing=list(expected_to_change)
        )


TEST_TABLE_IDENTIFIER = ("com", "organization", "department", "my_table")
TEST_TABLE_NAMESPACE = ("com", "organization", "department")
TEST_TABLE_NAME = "my_table"
TEST_TABLE_SCHEMA = Schema(schema_id=1)
TEST_TABLE_LOCATION = "protocol://some/location"
TEST_TABLE_PARTITION_SPEC = PartitionSpec(PartitionField(name="x", transform=IdentityTransform(), source_id=1, field_id=1000))
TEST_TABLE_PROPERTIES = {"key1": "value1", "key2": "value2"}
NO_SUCH_TABLE_ERROR = "Table does not exist: \\('com', 'organization', 'department', 'my_table'\\)"
TABLE_ALREADY_EXISTS_ERROR = "Table already exists: \\('com', 'organization', 'department', 'my_table'\\)"
NAMESPACE_ALREADY_EXISTS_ERROR = "Namespace already exists: \\('com', 'organization', 'department'\\)"
NO_SUCH_NAMESPACE_ERROR = "Namespace does not exist: \\('com', 'organization', 'department'\\)"
NAMESPACE_NOT_EMPTY_ERROR = "Namespace is not empty: \\('com', 'organization', 'department'\\)"


def given_catalog_has_a_table(catalog: InMemoryCatalog) -> Table:
    return catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )


def test_namespace_from_tuple() -> None:
    # Given
    identifier = ("com", "organization", "department", "my_table")
    # When
    namespace_from = Catalog.namespace_from(identifier)
    # Then
    assert namespace_from == ("com", "organization", "department")


def test_namespace_from_str() -> None:
    # Given
    identifier = "com.organization.department.my_table"
    # When
    namespace_from = Catalog.namespace_from(identifier)
    # Then
    assert namespace_from == ("com", "organization", "department")


def test_name_from_tuple() -> None:
    # Given
    identifier = ("com", "organization", "department", "my_table")
    # When
    name_from = Catalog.table_name_from(identifier)
    # Then
    assert name_from == "my_table"


def test_name_from_str() -> None:
    # Given
    identifier = "com.organization.department.my_table"
    # When
    name_from = Catalog.table_name_from(identifier)
    # Then
    assert name_from == "my_table"


def test_create_table(catalog: InMemoryCatalog) -> None:
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=TEST_TABLE_LOCATION,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.load_table(TEST_TABLE_IDENTIFIER) == table


def test_create_table_raises_error_when_table_already_exists(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(TableAlreadyExistsError, match=TABLE_ALREADY_EXISTS_ERROR):
        catalog.create_table(
            identifier=TEST_TABLE_IDENTIFIER,
            schema=TEST_TABLE_SCHEMA,
        )


def test_load_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_table = given_catalog_has_a_table(catalog)
    # When
    table = catalog.load_table(TEST_TABLE_IDENTIFIER)
    # Then
    assert table == given_table


def test_table_raises_error_on_table_not_found(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_drop_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.drop_table(TEST_TABLE_IDENTIFIER)
    # Then
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_drop_table_that_does_not_exist_raise_error(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_purge_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.purge_table(TEST_TABLE_IDENTIFIER)
    # Then
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_rename_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)

    # When
    new_table = "new.namespace.new_table"
    table = catalog.rename_table(TEST_TABLE_IDENTIFIER, new_table)

    # Then
    assert table.identifier == Catalog.identifier_to_tuple(new_table)

    # And
    table = catalog.load_table(new_table)
    assert table.identifier == Catalog.identifier_to_tuple(new_table)

    # And
    assert ("new", "namespace") in catalog.list_namespaces()

    # And
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_create_namespace(catalog: InMemoryCatalog) -> None:
    # When
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert TEST_TABLE_PROPERTIES == catalog.load_namespace_properties(TEST_TABLE_NAMESPACE)


def test_create_namespace_raises_error_on_existing_namespace(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    with pytest.raises(NamespaceAlreadyExistsError, match=NAMESPACE_ALREADY_EXISTS_ERROR):
        catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)


def test_get_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError, match=NO_SUCH_NAMESPACE_ERROR):
        catalog.load_namespace_properties(TEST_TABLE_NAMESPACE)


def test_list_namespaces(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    namespaces = catalog.list_namespaces()
    # Then
    assert TEST_TABLE_NAMESPACE in namespaces


def test_drop_namespace(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    catalog.drop_namespace(TEST_TABLE_NAMESPACE)
    # Then
    assert TEST_TABLE_NAMESPACE not in catalog.list_namespaces()


def test_drop_namespace_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError, match=NO_SUCH_NAMESPACE_ERROR):
        catalog.drop_namespace(TEST_TABLE_NAMESPACE)


def test_drop_namespace_raises_error_when_namespace_not_empty(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(NamespaceNotEmptyError, match=NAMESPACE_NOT_EMPTY_ERROR):
        catalog.drop_namespace(TEST_TABLE_NAMESPACE)


def test_list_tables(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    tables = catalog.list_tables()
    # Then
    assert tables
    assert TEST_TABLE_IDENTIFIER in tables


def test_list_tables_under_a_namespace(catalog: InMemoryCatalog) -> None:
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


def test_update_namespace_metadata(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    summary = catalog.update_namespace_properties(TEST_TABLE_NAMESPACE, updates=new_metadata)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert new_metadata.items() <= catalog.load_namespace_properties(TEST_TABLE_NAMESPACE).items()
    assert summary == PropertiesUpdateSummary(removed=[], updated=["key3", "key4"], missing=[])


def test_update_namespace_metadata_removals(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    remove_metadata = {"key1"}
    summary = catalog.update_namespace_properties(TEST_TABLE_NAMESPACE, remove_metadata, new_metadata)

    # Then
    assert TEST_TABLE_NAMESPACE in catalog.list_namespaces()
    assert new_metadata.items() <= catalog.load_namespace_properties(TEST_TABLE_NAMESPACE).items()
    assert remove_metadata.isdisjoint(catalog.load_namespace_properties(TEST_TABLE_NAMESPACE).keys())
    assert summary == PropertiesUpdateSummary(removed=["key1"], updated=["key3", "key4"], missing=[])


def test_update_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError, match=NO_SUCH_NAMESPACE_ERROR):
        catalog.update_namespace_properties(TEST_TABLE_NAMESPACE, updates=TEST_TABLE_PROPERTIES)

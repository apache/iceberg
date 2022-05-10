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

from iceberg.catalog.base import (
    AlreadyExistsError,
    Catalog,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    TableNotFoundError,
)
from iceberg.table.base import Table, TableSpec


class InMemoryCatalog(Catalog):
    __tables: Dict[tuple, Table]
    __namespaces: Dict[str, Dict[str, str]]

    def __init__(self, name: str, properties: Dict[str, str]):
        super().__init__(name, properties)
        self.__tables = {}
        self.__namespaces = {}

    def create_table(self, spec: TableSpec) -> Table:
        if (spec.namespace, spec.name) in self.__tables:
            raise AlreadyExistsError(spec.name)
        else:
            if spec.namespace not in self.__namespaces:
                self.__namespaces[spec.namespace] = {}

            table = Table(spec)
            self.__tables[(spec.namespace, spec.name)] = table
            return table

    def table(self, namespace: str, name: str) -> Table:
        try:
            return self.__tables[(namespace, name)]
        except KeyError:
            raise TableNotFoundError(name)

    def drop_table(self, namespace: str, name: str, purge: bool = True) -> None:
        try:
            self.__tables.pop((namespace, name))
        except KeyError:
            raise TableNotFoundError(name)

    def rename_table(self, from_namespace: str, from_name: str, to_namespace: str, to_name: str) -> Table:
        try:
            table = self.__tables.pop((from_namespace, from_name))
        except KeyError:
            raise TableNotFoundError(from_name)

        renamed_table = Table(
            TableSpec(
                namespace=to_namespace,
                name=to_name,
                schema=table.spec.schema,
                location=table.spec.location,
                partition_spec=table.spec.partition_spec,
                properties=table.spec.properties,
            )
        )
        if to_namespace not in self.__namespaces:
            self.__namespaces[to_namespace] = {}

        self.__tables[(to_namespace, to_name)] = renamed_table
        return renamed_table

    def replace_table(self, table_spec: TableSpec) -> Table:
        try:
            table = self.__tables.pop((table_spec.namespace, table_spec.name))
        except KeyError:
            raise TableNotFoundError(table_spec.name)

        replaced_table = Table(
            TableSpec(
                namespace=table_spec.namespace if table_spec.namespace else table.spec.namespace,
                name=table_spec.name if table_spec.name else table.spec.name,
                schema=table_spec.schema if table_spec.schema else table.spec.schema,
                location=table_spec.location if table_spec.location else table.spec.location,
                partition_spec=table_spec.partition_spec if table_spec.partition_spec else table.spec.partition_spec,
                properties={**table.spec.properties, **table_spec.properties},
            )
        )
        self.__tables[(replaced_table.spec.namespace, replaced_table.spec.name)] = replaced_table
        return replaced_table

    def create_namespace(self, namespace: str, properties: Optional[Dict[str, str]] = None) -> None:
        if namespace in self.__namespaces:
            raise AlreadyExistsError(namespace)
        else:
            self.__namespaces[namespace] = properties if properties else {}

    def drop_namespace(self, namespace: str) -> None:
        if [table_name_tuple for table_name_tuple in self.__tables.keys() if namespace in table_name_tuple]:
            raise NamespaceNotEmptyError(namespace)
        try:
            self.__namespaces.pop(namespace)
        except KeyError:
            raise NamespaceNotFoundError(namespace)

    def list_tables(self, namespace: Optional[str] = None) -> List[Tuple[str, str]]:
        if namespace:
            list_tables = [table_name_tuple for table_name_tuple in self.__tables.keys() if namespace in table_name_tuple]
        else:
            list_tables = list(self.__tables.keys())

        # Casting to make mypy happy
        return cast(List[Tuple[str, str]], list_tables)

    def list_namespaces(self) -> List[str]:
        return list(self.__namespaces.keys())

    def get_namespace_metadata(self, namespace: str) -> Dict[str, str]:
        try:
            return self.__namespaces[namespace]
        except KeyError:
            raise NamespaceNotFoundError(namespace)

    def set_namespace_metadata(self, namespace: str, metadata: Dict[str, str]) -> None:
        if namespace in self.__namespaces:
            self.__namespaces[namespace] = metadata
        else:
            raise NamespaceNotFoundError(namespace)

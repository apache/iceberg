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

from typing import Dict, List, Optional

from iceberg.catalog.base import AlreadyExistsError, Catalog, TableNotFoundError
from iceberg.table.base import Table, TableSpec


class InMemoryCatalog(Catalog):

    __catalog: Dict[tuple, Table]

    def __init__(self, name: str, properties: Dict[str, str]):
        super().__init__(name, properties)
        self.__catalog = {}

    def create_table(self, spec: TableSpec) -> Table:
        if (spec.namespace, spec.name) in self.__catalog:
            raise AlreadyExistsError(spec.name)
        else:
            table = Table(spec)
            self.__catalog[(spec.namespace, spec.name)] = table
            return table

    def table(self, namespace: str, name: str) -> Table:
        try:
            return self.__catalog[(namespace, name)]
        except KeyError:
            raise TableNotFoundError(name)

    def drop_table(self, namespace: str, name: str, purge: bool = True) -> None:
        try:
            self.__catalog.pop((namespace, name))
        except KeyError:
            raise TableNotFoundError(name)

    def rename_table(self, from_namespace: str, from_name: str, to_namespace: str, to_name: str) -> Table:
        try:
            table = self.__catalog.pop((from_namespace, from_name))
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
        self.__catalog[(to_namespace, to_name)] = renamed_table
        return renamed_table

    def replace_table(self, table_spec: TableSpec) -> Table:
        try:
            table = self.__catalog.pop((table_spec.namespace, table_spec.name))
        except KeyError:
            raise TableNotFoundError(table_spec.name)

        replaced_table = Table(
            TableSpec(
                namespace=table_spec.namespace if table_spec.namespace else table.spec.namespace,
                name=table_spec.name if table_spec.name else table.spec.name,
                schema=table_spec.schema if table_spec.schema else table.spec.schema,
                location=table_spec.location if table_spec.location else table.spec.location,
                partition_spec=table_spec.partition_spec if table_spec.partition_spec else table.spec.partition_spec,
                properties=table_spec.properties if table_spec.properties else table.spec.properties,
            )
        )
        self.__catalog[(replaced_table.spec.namespace, replaced_table.spec.name)] = replaced_table
        return replaced_table

    def create_namespace(self, namespace: str, properties: Optional[Dict[str, str]] = None) -> None:
        pass

    def drop_namespace(self, namespace: str) -> None:
        pass

    def list_tables(self, namespace: Optional[str] = None) -> List[Table]:
        pass

    def list_namespaces(self) -> List[str]:
        pass

    def get_namespace_metadata(self, namespace: str) -> Dict[str, str]:
        pass

    def set_namespace_metadata(self, namespace: str, metadata: Dict[str, str]) -> None:
        pass

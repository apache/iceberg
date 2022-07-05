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
from __future__ import annotations

from typing import Dict

from iceberg.catalog import Identifier, Properties
from iceberg.catalog.base import Catalog
from iceberg.schema import Schema
from iceberg.table.base import Table, PartitionSpec

from apiclient import APIClient, endpoint
from apiclient_pydantic import serialize_all_methods


@endpoint(base_url='{scheme}://{host}/{basePath}')
class Endpoints:
    get_config: str = '/v1/config'
    list_namespaces: str = '/v1/namespaces'
    create_namespace: str = '/v1/namespaces'
    load_namespace_metadata: str = '/v1/namespaces/{namespace}'
    drop_namespace: str = '/v1/namespaces/{namespace}'
    update_properties: str = '/v1/namespaces/{namespace}/properties'
    list_tables: str = '/v1/namespaces/{namespace}/tables'
    create_table: str = '/v1/namespaces/{namespace}/tables'
    load_table: str = '/v1/namespaces/{namespace}/tables/{table}'
    update_table: str = '/v1/namespaces/{namespace}/tables/{table}'
    drop_table: str = '/v1/namespaces/{namespace}/tables/{table}'
    table_exists: str = '/v1/namespaces/{namespace}/tables/{table}'
    get_token: str = '/v1/oauth/tokens'
    rename_table: str = '/v1/tables/rename'


@serialize_all_methods()
class MyAPIClient(APIClient):


    def get_config(self) -> CatalogConfig:
        """List all catalog configuration settings"""
        return self.get(Endpoints.get_config)

    def list_namespaces(
            self, query_params: ParentQueryParams
    ) -> V1NamespacesGetResponse:
        """List namespaces, optionally providing a parent namespace to list underneath"""
        return self.get(Endpoints.list_namespaces, query_params)

    def create_namespace(
            self, body: CreateNamespaceRequest = None
    ) -> V1NamespacesPostResponse:
        """Create a namespace"""
        return self.post(Endpoints.create_namespace, body)

    def load_namespace_metadata(
            self, path_params: NamespacePathParams
    ) -> V1NamespacesNamespaceGetResponse:
        """Load the metadata properties for a namespace"""
        return self.get(Endpoints.load_namespace_metadata.format(**path_params))

    def drop_namespace(self, path_params: NamespacePathParams) -> None:
        """Drop a namespace from the catalog. Namespace must be empty."""
        self.delete(Endpoints.drop_namespace.format(**path_params))

    def update_properties(
            self,
            path_params: NamespacePathParams,
            body: UpdateNamespacePropertiesRequest = None,
    ) -> V1NamespacesNamespacePropertiesPostResponse:
        """Set or remove properties on a namespace"""
        return self.post(Endpoints.update_properties.format(**path_params), body)

    def list_tables(
            self, path_params: NamespacePathParams
    ) -> V1NamespacesNamespaceTablesGetResponse:
        """List all table identifiers underneath a given namespace"""
        return

    def create_table(
            self, path_params: NamespacePathParams, body: CreateTableRequest = None
    ) -> LoadTableResult:
        """Create a table in the given namespace"""
        return self.post(Endpoints.create_table.format(**path_params), body)

    def load_table(self, path_params: NamespaceTablePathParams) -> LoadTableResult:
        """Load a table from the catalog"""
        return self.get(Endpoints.load_table.format(**path_params))

    def update_table(
            self, path_params: NamespaceTablePathParams, body: CommitTableRequest = None
    ) -> V1NamespacesNamespaceTablesTablePostResponse:
        """Commit updates to a table"""
        return self.post(Endpoints.update_table.format(**path_params), body)

    def drop_table(
            self,
            path_params: NamespaceTablePathParams,
            query_params: PurgeRequestedQueryParams = ...,
    ) -> None:
        """Drop a table from the catalog"""
        self.delete(Endpoints.drop_table.format(**path_params), query_params)


NAMESPACE_SEPARATOR = b'\x1F'.decode('UTF-8')


class RestCatalog(APIClient, Catalog):

    def __init__(self, name: str, properties: Properties):
        super().__init__(name, properties)

        # Probably we should do something with this
        self.post(Endpoints.get_token, data={})

    def _split_namespace_and_table(self, identifier: str | Identifier) -> Dict[str, str]:
        identifier = self.identifier_to_tuple(identifier)
        return {
            'namespace': NAMESPACE_SEPARATOR.join(identifier[:-1]),
            'table': identifier[-1]
        }

    def create_table(self,
                     identifier: str | Identifier,
                     schema: Schema,
                     location: str | None = None,
                     partition_spec: PartitionSpec | None = None,
                     properties: Properties | None = None) -> Table:
        namespace_and_table = self._split_namespace_and_table(identifier)
        return self.post(Endpoints.create_table.format(namespace=namespace_and_table['namespace']), {
            'name': namespace_and_table['table'],
            'location': location,
            'partition_spec': partition_spec,
            #'write_order': sort_order,
        })

    def list_tables(self, namespace: str | Identifier | None = None) -> list[Identifier]:
        return self.get(Endpoints.list_tables.format(namespace=namespace))

    def load_table(self, identifier: str | Identifier) -> Table:
        pass

    def drop_table(self, identifier: str | Identifier, purge_requested: bool = False) -> None:
        self.delete(
            Endpoints.drop_table.format(**self._split_namespace_and_table(identifier)))

    def purge_table(self, identifier: str | Identifier) -> None:
        pass

    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        return self.post(Endpoints.rename_table, {
            "source": from_identifier,
            "destination": to_identifier
        })

    def create_namespace(self, namespace: str | Identifier, properties: Properties | None = None) -> None:
        pass

    def drop_namespace(self, namespace: str | Identifier) -> None:
        pass

    def list_namespaces(self) -> list[Identifier]:
        pass

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        pass

    def update_namespace_properties(self, namespace: str | Identifier, removals: set[str] | None = None,
                                    updates: Properties | None = None) -> None:
        pass

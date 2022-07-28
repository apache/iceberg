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
    List,
    Optional,
    Set,
    Union,
)
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hive_metastore.ThriftHiveMetastore import Client

from pyiceberg.catalog import Identifier, Properties
from pyiceberg.catalog.base import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table.base import Table
from pyiceberg.table.partitioning import PartitionSpec

_DOT = "."
THRIFT_URIS = "hive.metastore.uris"
IPROT = "iprot"
OPROT = "oprot"


class HiveCatalog(Catalog):
    def __init__(self, name: str, properties: Properties, url: str):
        super().__init__(name, properties)
        self.client = self.get_client(url)

    def get_client(self, url: str) -> Client:
        host = 'localhost'
        port = 9083

        transport = TSocket.TSocket(host, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        client = Client(protocol)
        transport.open()
        return client

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Properties] = None,
    ) -> Table:
        pass

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        pass

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        pass

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        pass

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        pass

    def create_namespace(self, namespace: Union[str, Identifier], properties: Optional[Properties] = None) -> None:
        database_name = self.identifier_to_tuple(namespace)[0]
        DatabaseBuilder(name=database_name, parameters=properties).build()
        self.client.create_database(namespace)

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        database_name = self.identifier_to_tuple(namespace)[0]
        DatabaseBuilder(name=database_name).build()
        self.client.drop_atabase(database_name, deleteData=True, cascade=True)

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        database_name = self.identifier_to_tuple(namespace)[0]
        tables = self.client.get_all_tables(db_name=database_name)
        return []

    def list_namespaces(self) -> List[Identifier]:
        return [(database,) for database in self.client.get_databases(pattern='*')]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        database_name = self.identifier_to_tuple(namespace)[0]
        vo = self.client.get_database(name=database_name)
        return {}

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Optional[Properties] = None
    ) -> None:
        return {}

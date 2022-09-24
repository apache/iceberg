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

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import SortOrder, UNSORTED_SORT_ORDER, SortDirection
from pyiceberg.typedef import Identifier, Properties, EMPTY_DICT


class GlueCatalog(Catalog):

    def __init__(self, name: str, properties: Properties):
        super().__init__(name, properties)

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        pass

    def load_table(self, identifier: str | Identifier) -> Table:
        pass

    def drop_table(self, identifier: str | Identifier) -> None:
        pass

    def purge_table(self, identifier: str | Identifier) -> None:
        pass

    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        pass

    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        pass

    def drop_namespace(self, namespace: str | Identifier) -> None:
        pass

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        pass

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        pass

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        pass

    def update_namespace_properties(
        self, namespace: str | Identifier, removals: set[str] | None = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        pass




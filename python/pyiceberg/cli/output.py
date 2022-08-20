# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
from abc import ABC, abstractmethod
from typing import Any, List, Optional
from uuid import UUID

from rich.console import Console
from rich.table import Table as RichTable
from rich.tree import Tree

from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.partitioning import PartitionSpec
from pyiceberg.typedef import Identifier, Properties


class Output(ABC):
    """Output interface for exporting"""

    @abstractmethod
    def exception(self, ex: Exception):
        ...

    @abstractmethod
    def identifiers(self, identifiers: List[Identifier]):
        ...

    @abstractmethod
    def describe_table(self, table):
        ...

    @abstractmethod
    def describe_properties(self, properties: Properties):
        ...

    @abstractmethod
    def text(self, response: str):
        ...

    @abstractmethod
    def schema(self, schema: Schema):
        ...

    @abstractmethod
    def spec(self, spec: PartitionSpec):
        ...

    @abstractmethod
    def uuid(self, uuid: Optional[UUID]):
        ...


class ConsoleOutput(Output):
    """Writes to the console"""

    def __init__(self, **properties: str):
        self.verbose = properties.get("verbose", False)

    @property
    def _table(self) -> RichTable:
        return RichTable.grid(padding=(0, 2))

    def exception(self, ex: Exception):
        if self.verbose:
            Console(stderr=True).print_exception()
        else:
            Console(stderr=True).print(ex)

    def identifiers(self, identifiers: List[Identifier]):
        table = self._table
        for identifier in identifiers:
            table.add_row(".".join(identifier))

        Console().print(table)

    def describe_table(self, table: Table):
        metadata = table.metadata
        table_properties = self._table

        for key, value in metadata.properties.items():
            table_properties.add_row(key, value)

        schema_tree = Tree("Schema")
        for field in table.schema().fields:
            schema_tree.add(str(field))

        snapshot_tree = Tree("Snapshots")
        for snapshot in metadata.snapshots:
            snapshot_tree.add(f"Snapshot {snapshot.schema_id}: {snapshot.manifest_list}")

        output_table = self._table
        output_table.add_row("Table format version", str(metadata.format_version))
        output_table.add_row("Metadata location", table.metadata_location)
        output_table.add_row("Table UUID", str(table.metadata.table_uuid))
        output_table.add_row("Last Updated", str(metadata.last_updated_ms))
        output_table.add_row("Partition spec", str(table.spec()))
        output_table.add_row("Sort order", str(table.sort_order()))
        output_table.add_row("Schema", schema_tree)
        output_table.add_row("Snapshots", snapshot_tree)
        output_table.add_row("Properties", table_properties)
        Console().print(output_table)

    def describe_properties(self, properties: Properties):
        output_table = self._table
        for k, v in properties.items():
            output_table.add_row(k, v)
        Console().print(output_table)

    def text(self, response: str):
        Console().print(response)

    def schema(self, schema: Schema):
        output_table = self._table
        for field in schema.fields:
            output_table.add_row(field.name, str(field.field_type), field.doc or "")
        Console().print(output_table)

    def spec(self, spec: PartitionSpec):
        Console().print(str(spec))

    def uuid(self, uuid: Optional[UUID]):
        Console().print(str(uuid) if uuid else "missing")


class JsonOutput(Output):
    """Writes json to stdout"""

    def _out(self, d: Any) -> None:
        print(json.dumps(d))

    def exception(self, ex: Exception):
        self._out({"type": ex.__class__.__name__, "message": str(ex)})

    def identifiers(self, identifiers: List[Identifier]):
        self._out([".".join(identifier) for identifier in identifiers])

    def describe_table(self, table: Table):
        print(table.json())

    def describe_properties(self, properties: Properties):
        self._out(properties)

    def text(self, response: str):
        print(json.dumps(response))

    def schema(self, schema: Schema):
        print(schema.json())

    def spec(self, spec: PartitionSpec):
        print(spec.json())

    def uuid(self, uuid: Optional[UUID]):
        self._out({"uuid": str(uuid) if uuid else "missing"})

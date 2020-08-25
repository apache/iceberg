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
from typing import Tuple

from . import TableOperations
from .base_table import BaseTable
from .table_metadata import TableMetadata
from ..api import PartitionSpec, Schema, Table, Tables
from ..exceptions import AlreadyExistsException, CommitFailedException, NoSuchTableException


class BaseMetastoreTables(Tables):

    def __init__(self: "BaseMetastoreTables", conf: dict) -> None:
        self.conf = conf

    def new_table_ops(self: "BaseMetastoreTables", conf: dict, database: str, table: str) -> "TableOperations":
        raise RuntimeError("Abstract Implementation")

    def load(self: "BaseMetastoreTables", table_identifier: str) -> Table:
        database, table = _parse_table_identifier(table_identifier)
        ops = self.new_table_ops(self.conf, database, table)
        if ops.current():
            return BaseTable(ops, "{}.{}".format(database, table))
        raise NoSuchTableException("Table does not exist: {}.{}".format(database, table))

    def create(self: "BaseMetastoreTables", schema: Schema, table_identifier: str, spec: PartitionSpec = None,
               properties: dict = None, location: str = None) -> Table:
        database, table = _parse_table_identifier(table_identifier)
        ops = self.new_table_ops(self.conf, database, table)
        if ops.current():  # not None check here to ensure MagicMocks aren't treated as None
            raise AlreadyExistsException("Table already exists: " + table_identifier)

        base_location = location if location else self.default_warehouse_location(self.conf, database, table)
        full_spec, properties = super(BaseMetastoreTables, self).default_args(spec, properties)
        metadata = TableMetadata.new_table_metadata(ops, schema, full_spec, base_location, properties)

        try:
            ops.commit(None, metadata)
        except CommitFailedException:
            raise AlreadyExistsException("Table was created concurrently: " + table_identifier)

        return BaseTable(ops, "{}.{}".format(database, table))

    def begin_create(self: "BaseMetastoreTables", schema: Schema, spec: PartitionSpec, database: str, table_name: str,
                     properties: dict = None):
        raise RuntimeError("Not Yet Implemented")

    def begin_replace(self: "BaseMetastoreTables", schema: Schema, spec: PartitionSpec, database: str, table: str,
                      properties: dict = None):
        raise RuntimeError("Not Yet Implemented")

    def default_warehouse_location(self: "BaseMetastoreTables", conf: dict, database: str, table: str) -> str:
        warehouse_location = conf.get("hive.metastore.warehouse.dir")
        if warehouse_location:
            return f"{warehouse_location}/{database}.db/{table}"
        raise RuntimeError("Warehouse location is not set: hive.metastore.warehouse.dir=null")


_DOT = '.'


def _parse_table_identifier(table_identifier: str) -> Tuple[str, str]:
    parts = table_identifier.rsplit(_DOT, 1)
    if len(parts) > 1:
        database = parts[0]
        table = parts[1]
    else:
        database = "default"
        table = parts[0]
    return database, table

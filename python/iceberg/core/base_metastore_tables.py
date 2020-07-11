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

from iceberg.api import Tables
from iceberg.exceptions import AlreadyExistsException, CommitFailedException, NoSuchTableException

from .base_table import BaseTable
from .table_metadata import TableMetadata


class BaseMetastoreTables(Tables):
    DOT = '.'

    def __init__(self, conf):
        self.conf = conf

    def new_table_ops(self, conf, database, table):
        raise RuntimeError("Abstract Implementation")

    def load(self, table_identifier):
        parts = table_identifier.rsplit(BaseMetastoreTables.DOT, 1)
        if len(parts) > 1:
            database = parts[0]
            table = parts[1]
        else:
            database = "default"
            table = parts[0]
        ops = self.new_table_ops(self.conf, database, table)
        if ops.current() is None:
            raise NoSuchTableException("Table does not exist: {}.{}".format(database, table))

        return BaseTable(ops, "{}.{}".format(database, table))

    def create(self, schema, table_identifier=None, spec=None, properties=None):
        database, table = table_identifier.rsplit(BaseMetastoreTables.DOT, 1)
        ops = self.new_table_ops(self.conf, database, table)
        if ops.current() is not None:
            raise AlreadyExistsException("Table already exists: " + table_identifier)

        base_location = self.default_warehouse_location(self.conf, database, table)

        metadata = TableMetadata.new_table_metadata(ops, schema, spec, base_location, dict() if properties is None else properties)

        try:
            ops.commit(None, metadata)
        except CommitFailedException:
            raise AlreadyExistsException("Table was created concurrently: " + table_identifier)

        return BaseTable(ops, "{}.{}".format(database, table))

    def begin_create(self, schema, spec, database, table_name, properties=None):
        raise RuntimeError("Not Yet Implemented")

    def begin_replace(self, schema, spec, database, table, properties=None):
        raise RuntimeError("Not Yet Implemented")

    def default_warehouse_location(self, conf, database, table):
        warehouse_location = conf.get("hive.metastore.warehouse.dir")
        if warehouse_location is None:
            raise RuntimeError("Warehouse location is not set: hive.metastore.warehouse.dir=null")

        return f"{warehouse_location}/{database}.db/{table}"

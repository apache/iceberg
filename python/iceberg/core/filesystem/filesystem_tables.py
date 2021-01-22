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

from .filesystem_table_operations import FilesystemTableOperations
from .. import TableOperations
from ..table_metadata import TableMetadata
from ...api import PartitionSpec, Schema, Table, Tables
from ...exceptions import NoSuchTableException


class FilesystemTables(Tables):

    def __init__(self: "FilesystemTables", conf: dict = None) -> None:
        self.conf = conf if conf is not None else dict()

    def load(self: "FilesystemTables", table_identifier: str) -> Table:
        from ..base_table import BaseTable
        ops = self.new_table_ops(table_identifier)
        if ops.current() is None:
            raise NoSuchTableException("Table does not exist at location: %s" % table_identifier)

        return BaseTable(ops, table_identifier)

    def create(self: "FilesystemTables", schema: Schema, table_identifier: str, spec: PartitionSpec = None,
               properties: dict = None, location: str = None) -> Table:
        """
        Create a new table on the filesystem.

        Note: it is expected that the filesystem has atomic operations to ensure consistency for metadata updates.
        Filesystems that don't have this guarantee could lead to data loss.

        Location should always be None as the table location on disk is taken from `table_identifier`
        """
        from ..base_table import BaseTable
        if location:
            raise RuntimeError("""location has to be None. Both table_identifier and location have been declared.
             table_identifier: {} and location: {}""".format(table_identifier, location))

        full_spec, properties = super(FilesystemTables, self).default_args(spec, properties)
        ops = self.new_table_ops(table_identifier)

        metadata = TableMetadata.new_table_metadata(ops, schema, full_spec, table_identifier, properties)
        ops.commit(None, metadata)

        return BaseTable(ops, table_identifier)

    def new_table_ops(self: "FilesystemTables", table_identifier: str) -> TableOperations:
        if table_identifier is None:
            raise RuntimeError("table_identifier cannot be None")

        return FilesystemTableOperations(table_identifier, self.conf)

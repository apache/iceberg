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
from iceberg.exceptions import NoSuchTableException

from .filesystem_table_operations import FilesystemTableOperations
from ..table_metadata import TableMetadata


class FilesystemTables(Tables):

    def __init__(self, conf=None):
        self.conf = conf if conf is not None else dict()

    def load(self, location):
        from ..base_table import BaseTable
        ops = self.new_table_ops(location)
        if ops.current() is None:
            raise NoSuchTableException("Table does not exist at location: %s" % location)

        return BaseTable(ops, location)

    def create(self, schema, table_identifier=None, spec=None, properties=None, location=None):
        from ..base_table import BaseTable
        spec, properties = super(FilesystemTables, self).default_args(spec, properties)
        ops = self.new_table_ops(location)

        metadata = TableMetadata.new_table_metadata(ops, schema, spec, location, properties)
        ops.commit(None, metadata)

        return BaseTable(ops, location)

    def new_table_ops(self, location):
        if location is None:
            raise RuntimeError("location cannot be None")

        return FilesystemTableOperations(location, self.conf)

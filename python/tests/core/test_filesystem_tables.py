#
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
#
import os

from iceberg.core.filesystem import FilesystemTables


def test_create_tables(base_scan_schema, base_scan_partition, tmpdir):

    conf = {"hive.metastore.uris": 'thrift://hms:port',
            "hive.metastore.warehouse.dir": tmpdir}
    tables = FilesystemTables(conf)
    table_location = os.path.join(str(tmpdir), "test", "test_123")
    tables.create(base_scan_schema, table_location, base_scan_partition)

    tables.load(table_location)

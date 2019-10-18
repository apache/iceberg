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


from iceberg.core import BaseMetastoreTableOperations


class HiveTableOperations(BaseMetastoreTableOperations):

    def __init__(self, conf, client, database, table):
        super(HiveTableOperations, self).__init__(conf)
        self._client = client
        self.database = database
        self.table = table
        self.refresh()

    def refresh(self):
        with self._client as open_client:
            tbl_info = open_client.get_table(self.database, self.table)

        table_type = tbl_info.parameters.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)

        if table_type is None or table_type.lower() != BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE:
            raise RuntimeError("Invalid table, not Iceberg: %s.%s" % (self.database,
                                                                      self.table))

        metadata_location = tbl_info.parameters.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)
        if metadata_location is None:
            raise RuntimeError("Invalid table, missing metadata_location: %s.%s" % (self.database,
                                                                                    self.table))

        self.refresh_from_metadata_location(metadata_location)

        return self.current()

    def commit(self, base, metadata):
        raise NotImplementedError()

    def io(self):
        raise NotImplementedError()

    def close(self):
        self._client.close()

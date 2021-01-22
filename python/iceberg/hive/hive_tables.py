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


from hmsclient import hmsclient
from iceberg.core import BaseMetastoreTables

from .hive_table_operations import HiveTableOperations


class HiveTables(BaseMetastoreTables):
    _DOT = "."
    THRIFT_URIS = "hive.metastore.uris"

    def __init__(self, conf):
        super(HiveTables, self).__init__(conf)

    def new_table_ops(self, conf, database, table):
        return HiveTableOperations(conf, self.get_client(), database, table)

    def drop(self, database, table):
        raise RuntimeError("Not yet implemented")

    def get_client(self):
        from urllib.parse import urlparse
        metastore_uri = urlparse(self.conf[HiveTables.THRIFT_URIS])

        client = hmsclient.HMSClient(host=metastore_uri.hostname, port=metastore_uri.port)
        return client

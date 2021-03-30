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

import logging

from multiprocessing import cpu_count
from multiprocessing.dummy import Pool
from typing import Callable

from hmsclient import HMSClient, hmsclient
from iceberg.core import BaseMetastoreTables
from iceberg.core.util import WORKER_THREAD_POOL_SIZE_PROP
from iceberg.hive import HiveTableOperations

_logger = logging.getLogger(__name__)


class HiveTables(BaseMetastoreTables):
    _DOT = "."
    THRIFT_URIS = "hive.metastore.uris"

    def __init__(self, conf):
        super(HiveTables, self).__init__(conf)

    def new_table_ops(self, conf, database, table):
        return HiveTableOperations(conf, self.get_client(), database, table)

    def drop(self, database: str, table: str, purge: bool = False) -> None:
        ops = self.new_table_ops(self.conf, database, table)
        metadata = ops.current()

        with self.get_client() as open_client:
            _logger.info("Deleting {database}.{table} from Hive Metastore".format(database=database, table=table))
            open_client.drop_table(database, table, deleteData=False)

        if purge:
            if metadata is not None:
                with Pool(self.conf.get(WORKER_THREAD_POOL_SIZE_PROP,
                                        cpu_count())) as delete_pool:
                    for s in metadata.snapshots:
                        for m in s.manifests:
                            delete_pool.map(self._delete_file(ops),
                                            (i.path() for i in s.get_filtered_manifest(m.manifest_path).iterator()))
                        delete_pool.map(self._delete_file(ops), (m.manifest_path for m in s.manifests))
                        if s.manifest_location is not None:
                            delete_pool.map(self._delete_file(ops), [s.manifest_location])
                    delete_pool.map(self._delete_file(ops), [ops.current_metadata_location])

    def get_client(self) -> HMSClient:
        from urllib.parse import urlparse
        metastore_uri = urlparse(self.conf[HiveTables.THRIFT_URIS])

        client = hmsclient.HMSClient(host=metastore_uri.hostname, port=metastore_uri.port)
        return client

    @staticmethod
    def _delete_file(ops: HiveTableOperations) -> Callable[[str], None]:
        def _delete_file_internal(path: str) -> None:
            _logger.info("Deleting file: {path}".format(path=path))
            try:
                ops.delete_file(path)
            except OSError as e:
                _logger.info("Error deleting file: {path}: {e}".format(path=path, e=e))
                pass
        return _delete_file_internal

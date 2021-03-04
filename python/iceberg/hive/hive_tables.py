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

import itertools
import logging
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool
from typing import Iterator

from hmsclient import HMSClient, hmsclient
from iceberg.core import BaseMetastoreTables, ManifestReader
from iceberg.core.filesystem import FileSystemInputFile
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

    def drop(self, database, table, purge=False) -> None:
        ops = self.new_table_ops(self.conf, database, table)
        metadata = ops.current()

        with self.get_client() as open_client:
            _logger.info("Deleting {database}.{table} from Hive Metastore".format(database=database, table=table))
            open_client.drop_table(database, table, deleteData=False)

        if purge:
            if metadata is not None:
                manifest_lists_to_delete = []
                manifests_to_delete = itertools.chain()

                for s in metadata.snapshots:
                    manifests_to_delete = itertools.chain(manifests_to_delete, (m for m in s.manifests))
                    if s.manifest_location is not None:
                        manifest_lists_to_delete.append(s.manifest_location)

                # Make a copy, as it is drained as we explore the manifest to list files.
                (manifests, manifests_to_delete) = itertools.tee(manifests_to_delete)

                with Pool(self.conf.get(WORKER_THREAD_POOL_SIZE_PROP,
                          cpu_count())) as delete_pool:
                    delete_pool.map(self._delete_file(ops), self._unique(self._get_data_files(manifests)))
                    delete_pool.map(self._delete_file(ops), self._unique(m.manifest_path for m in manifests_to_delete))
                    delete_pool.map(self._delete_file(ops), self._unique(manifest_lists_to_delete))
                    delete_pool.map(self._delete_file(ops), [ops.current_metadata_location])

    def get_client(self) -> HMSClient:
        from urllib.parse import urlparse
        metastore_uri = urlparse(self.conf[HiveTables.THRIFT_URIS])

        client = hmsclient.HMSClient(host=metastore_uri.hostname, port=metastore_uri.port)
        return client

    def _get_data_files(self, manifests) -> Iterator[str]:
        return itertools.chain.from_iterable(self._get_data_files_by_manifest(m) for m in manifests)

    def _get_data_files_by_manifest(self, manifest) -> Iterator[str]:
        file = FileSystemInputFile.from_location(manifest.manifest_path, self.conf)
        reader = ManifestReader.read(file)
        return (i.path() for i in reader.iterator())

    @staticmethod
    def _delete_file(ops):
        return lambda path: (
            _logger.info("Deleting file: {path}".format(path=path)),
            ops.delete_file(path))

    @staticmethod
    def _unique(iterable: Iterator) -> Iterator:
        seen = set()
        for item in iterable:
            if item not in seen:
                seen.add(item)
                yield item

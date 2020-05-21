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

from fastavro import parse_schema, writer
from iceberg.api import ManifestFile
from iceberg.api.io import FileAppender
from iceberg.core import GenericManifestFile
from iceberg.core.avro import IcebergToAvro


class ManifestListWriter(FileAppender):

    def __init__(self, snapshot_file, snapshot_id, parent_snapshot_id):
        self.file = snapshot_file
        self.meta = {"snapshot-id": str(snapshot_id),
                     "parent-snapshot-id": str(parent_snapshot_id)}
        tmp_schema = IcebergToAvro.type_to_schema(ManifestFile.SCHEMA.as_struct(),
                                                  "manifest_file")

        self.schema = parse_schema(json.dumps(tmp_schema))

    def add(self, d):
        writer(self.file,
               self.schema,
               d,
               metadata=self.meta)

    def add_all(self, values):
        manifest_records = [GenericManifestFile.to_avro_record_dict(value)
                            for value in values if not isinstance(value, str)]
        writer(self.file,
               self.schema,
               manifest_records)

    def close(self):
        self.writer.flush()

    def metrics(self):
        raise RuntimeError("Metrics not available")

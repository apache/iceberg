# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from iceberg.core import BaseSnapshot

from .generic_manifest_file import GenericManifestFile


class SnapshotParser(object):
    SNAPSHOT_ID = "snapshot-id"
    PARENT_SNAPSHOT_ID = "parent-snapshot-id"
    TIMESTAMP_MS = "timestamp-ms"
    MANIFESTS = "manifests"
    MANIFEST_LIST = "manifest-list"

    @staticmethod
    def to_json(snapshot, dumps=True):
        snapshot = {SnapshotParser.SNAPSHOT_ID: snapshot.snapshot_id,
                    SnapshotParser.TIMESTAMP_MS: snapshot.timestamp_millis,
                    SnapshotParser.MANIFESTS: snapshot.manifests}
        if not dumps:
            return snapshot

        return json.dumps(snapshot)

    @staticmethod
    def from_json(ops, json_obj):
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        if not isinstance(json_obj, dict):
            raise RuntimeError("Cannot parse table version from a non-object: %s" % json_obj)

        if SnapshotParser.MANIFEST_LIST in json_obj:
            # the manifest list is stored in a manifest list file
            return BaseSnapshot(ops,
                                json_obj.get(SnapshotParser.SNAPSHOT_ID),
                                json_obj.get(SnapshotParser.PARENT_SNAPSHOT_ID),
                                manifest_list=ops.new_input_file(json_obj.get(SnapshotParser.MANIFEST_LIST)),
                                timestamp_millis=json_obj.get(SnapshotParser.TIMESTAMP_MS))
        else:
            manifests = [GenericManifestFile.generic_manifest_from_file(ops.new_input_file(location))  # noqa: F841
                         for location in json_obj.get(SnapshotParser.MANIFESTS, list())]
            raise RuntimeError("Not yet implemented")

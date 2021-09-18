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

from .base_snapshot import BaseSnapshot


class SnapshotParser(object):
    SNAPSHOT_ID = "snapshot-id"
    PARENT_SNAPSHOT_ID = "parent-snapshot-id"
    TIMESTAMP_MS = "timestamp-ms"
    SUMMARY = "summary"
    OPERATION = "operation"
    MANIFESTS = "manifests"
    MANIFEST_LIST = "manifest-list"

    @staticmethod
    def to_json(snapshot):
        return json.dumps(SnapshotParser.to_dict(snapshot))

    @staticmethod
    def to_dict(snapshot):
        return {SnapshotParser.SNAPSHOT_ID: snapshot.snapshot_id,
                SnapshotParser.TIMESTAMP_MS: snapshot.timestamp_millis,
                SnapshotParser.PARENT_SNAPSHOT_ID: snapshot.parent_id,
                SnapshotParser.OPERATION: snapshot.operation,
                SnapshotParser.SUMMARY: snapshot.summary,
                SnapshotParser.MANIFESTS: [manifest.manifest_path for manifest in snapshot.manifests]}

    @staticmethod
    def from_json(ops, json_obj, spec=None):
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        if not isinstance(json_obj, dict):
            raise RuntimeError("Cannot parse table version from a non-object: %s" % json_obj)

        version_id = json_obj.get(SnapshotParser.SNAPSHOT_ID)
        parent_id = json_obj.get(SnapshotParser.PARENT_SNAPSHOT_ID)
        timestamp_millis = json_obj.get(SnapshotParser.TIMESTAMP_MS)
        operation = json_obj.get(SnapshotParser.OPERATION)
        summary = json_obj.get(SnapshotParser.SUMMARY)

        if SnapshotParser.MANIFEST_LIST in json_obj:
            # the manifest list is stored in a manifest list file
            return BaseSnapshot(ops, version_id, parent_id,
                                manifest_list=ops.new_input_file(json_obj.get(SnapshotParser.MANIFEST_LIST)),
                                timestamp_millis=timestamp_millis,
                                operation=operation,
                                summary=summary)
        else:
            manifests = json_obj.get(SnapshotParser.MANIFESTS, list())

            return BaseSnapshot(ops, version_id, parent_id,
                                manifests=manifests,
                                timestamp_millis=timestamp_millis,
                                operation=operation,
                                summary=summary)

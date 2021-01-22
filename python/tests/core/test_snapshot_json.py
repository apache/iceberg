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

from iceberg.api import DataOperations
from iceberg.core import BaseSnapshot, SnapshotParser


def test_json_conversion(ops, expected_base_snapshot):
    as_json = SnapshotParser.to_json(expected_base_snapshot)
    snapshot = SnapshotParser.from_json(ops, as_json)

    assert expected_base_snapshot.snapshot_id == snapshot.snapshot_id
    assert expected_base_snapshot.manifests == snapshot.manifests


def test_json_conversion_with_operation(ops, snapshot_manifests):
    parent_id = 1
    id = 2
    expected = BaseSnapshot(ops=ops, snapshot_id=id, parent_id=parent_id,
                            manifests=snapshot_manifests, operation=DataOperations.REPLACE,
                            summary={"files-added": 4,
                                     "files-deleted": "100"})

    json_obj = SnapshotParser.to_json(expected)
    snapshot = SnapshotParser.from_json(ops, json_obj)

    assert expected.snapshot_id == snapshot.snapshot_id
    assert expected.timestamp_millis == snapshot.timestamp_millis
    assert expected.parent_id == snapshot.parent_id
    assert expected.manifest_location == snapshot.manifest_location
    assert expected.manifests == snapshot.manifests
    assert expected.operation == snapshot.operation
    assert expected.summary == snapshot.summary

# def test_conversion_with_manifest_list(snapshot_manifests):
#     parent_id = 1
#     id = 2
#
#     with NamedTemporaryFile() as manifest_list:
#         with ManifestListWriter(Files.local_output(manifest_list), id, parent_id) as writer:
#             writer.add_all(snapshot_manifests)
#
#         with open(manifest_list) as fo:
#             print(fo.read())

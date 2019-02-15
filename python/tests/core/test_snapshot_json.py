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

from iceberg.core import SnapshotParser


def test_json_conversion(ops, expected_base_snapshot):
    as_json = SnapshotParser.to_json(expected_base_snapshot)
    snapshot = SnapshotParser.from_json(ops, as_json)

    assert expected_base_snapshot.snapshot_id == snapshot.snapshot_id
    assert expected_base_snapshot.manifests == snapshot.manifests

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
from pyiceberg.table.snapshots import Operation, Snapshot, Summary


def test_serialize_summary():
    assert Summary(Operation.APPEND).json() == """{"operation": "append"}"""


def test_serialize_summary_with_properties():
    assert Summary(Operation.APPEND, property="yes").json() == """{"operation": "append", "property": "yes"}"""


def test_serialize_snapshot():
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    assert (
        snapshot.json()
        == """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    )


def test_serialize_snapshot_without_sequence_number():
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    actual = snapshot.json()
    expected = """{"snapshot-id": 25, "parent-snapshot-id": 19, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    assert actual == expected


def test_serialize_snapshot_with_properties():
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND, property="yes"),
        schema_id=3,
    )

    assert (
        snapshot.json()
        == """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append", "property": "yes"}, "schema-id": 3}"""
    )


def test_deserialize_summary():
    summary = Summary.parse_raw("""{"operation": "append"}""")
    assert summary.operation == Operation.APPEND


def test_deserialize_summary_with_properties():
    summary = Summary.parse_raw("""{"operation": "append", "property": "yes"}""")
    assert summary.operation == Operation.APPEND
    assert summary.additional_properties == {"property": "yes"}


def test_deserialize_snapshot():
    payload = """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    snapshot = Snapshot.parse_raw(payload)
    assert snapshot == Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )


def test_deserialize_snapshot_with_properties():
    payload = """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append", "property": "yes"}, "schema-id": 3}"""
    snapshot = Snapshot.parse_raw(payload)
    assert snapshot == Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND, property="yes"),
        schema_id=3,
    )

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
# pylint:disable=redefined-outer-name,eval-used
import pytest

from pyiceberg.table.snapshots import Operation, Snapshot, Summary


@pytest.fixture
def snapshot() -> Snapshot:
    return Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )


@pytest.fixture
def snapshot_with_properties() -> Snapshot:
    return Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND, foo="bar"),
        schema_id=3,
    )


def test_serialize_summary() -> None:
    assert Summary(Operation.APPEND).model_dump_json() == """{"operation":"append"}"""


def test_serialize_summary_with_properties() -> None:
    summary = Summary(Operation.APPEND, property="yes")
    assert summary.model_dump_json() == """{"operation":"append","property":"yes"}"""


def test_serialize_snapshot(snapshot: Snapshot) -> None:
    assert (
        snapshot.model_dump_json()
        == """{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append"},"schema-id":3}"""
    )


def test_serialize_snapshot_without_sequence_number() -> None:
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    actual = snapshot.model_dump_json()
    expected = """{"snapshot-id":25,"parent-snapshot-id":19,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append"},"schema-id":3}"""
    assert actual == expected


def test_serialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    assert (
        snapshot_with_properties.model_dump_json()
        == """{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append","foo":"bar"},"schema-id":3}"""
    )


def test_deserialize_summary() -> None:
    summary = Summary.model_validate_json("""{"operation": "append"}""")
    assert summary.operation == Operation.APPEND


def test_deserialize_summary_with_properties() -> None:
    summary = Summary.model_validate_json("""{"operation": "append", "property": "yes"}""")
    assert summary.operation == Operation.APPEND
    assert summary.additional_properties == {"property": "yes"}


def test_deserialize_snapshot(snapshot: Snapshot) -> None:
    payload = """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    actual = Snapshot.model_validate_json(payload)
    assert actual == snapshot


def test_deserialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    payload = """{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append","foo":"bar"},"schema-id":3}"""
    snapshot = Snapshot.model_validate_json(payload)
    assert snapshot == snapshot_with_properties


def test_snapshot_repr(snapshot: Snapshot) -> None:
    assert (
        repr(snapshot)
        == """Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND), schema_id=3)"""
    )
    assert snapshot == eval(repr(snapshot))


def test_snapshot_with_properties_repr(snapshot_with_properties: Snapshot) -> None:
    assert (
        repr(snapshot_with_properties)
        == """Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND, **{'foo': 'bar'}), schema_id=3)"""
    )
    assert snapshot_with_properties == eval(repr(snapshot_with_properties))

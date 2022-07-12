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
from pyiceberg.table.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import BucketTransform, TruncateTransform


def test_partition_field_init():
    bucket_transform = BucketTransform(100)
    partition_field = PartitionField(3, 1000, bucket_transform, "id")

    assert partition_field.source_id == 3
    assert partition_field.field_id == 1000
    assert partition_field.transform == bucket_transform
    assert partition_field.name == "id"
    assert partition_field == partition_field
    assert str(partition_field) == "1000: id: bucket[100](3)"
    assert (
        repr(partition_field)
        == "PartitionField(source_id=3, field_id=1000, transform=BucketTransform(num_buckets=100), name='id')"
    )


def test_partition_spec_init():
    bucket_transform: BucketTransform = BucketTransform(4)

    id_field1 = PartitionField(3, 1001, bucket_transform, "id")
    partition_spec1 = PartitionSpec(0, (id_field1,), 1001)

    assert partition_spec1.spec_id == 0
    assert partition_spec1 == partition_spec1
    assert partition_spec1 != id_field1
    assert str(partition_spec1) == f"[\n  {str(id_field1)}\n]"
    assert not partition_spec1.is_unpartitioned()
    # only differ by PartitionField field_id
    id_field2 = PartitionField(3, 1002, bucket_transform, "id")
    partition_spec2 = PartitionSpec(0, (id_field2,), 1001)
    assert partition_spec1 != partition_spec2
    assert partition_spec1.compatible_with(partition_spec2)
    assert partition_spec1.fields_by_source_id(3) == [id_field1]


def test_partition_compatible_with():
    bucket_transform: BucketTransform = BucketTransform(4)
    field1 = PartitionField(3, 100, bucket_transform, "id")
    field2 = PartitionField(3, 102, bucket_transform, "id")
    lhs = PartitionSpec(0, (field1,), 1001)
    rhs = PartitionSpec(0, (field1, field2), 1001)
    assert not lhs.compatible_with(rhs)


def test_unpartitioned():
    unpartitioned = PartitionSpec(1, (), 1000)

    assert not unpartitioned.fields
    assert unpartitioned.is_unpartitioned()
    assert str(unpartitioned) == "[]"


def test_serialize_unpartition_spec():
    unpartitioned = PartitionSpec(1, (), 1000)
    assert unpartitioned.json() == """{"spec-id": 1, "fields": [], "last-assigned-field-id": 1000}"""


def test_serialize_partition_spec():
    partitioned = PartitionSpec(
        spec_id=3,
        fields=(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
            PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        ),
    )
    assert (
        partitioned.json()
        == """{"spec-id": 3, "fields": [{"source-id": 1, "field-id": 1000, "transform": "truncate[19]", "name": "str_truncate"}, {"source-id": 2, "field-id": 1001, "transform": "bucket[25]", "name": "int_bucket"}], "last-assigned-field-id": 1001}"""
    )


def test_deserialize_partition_spec():
    json_partition_spec = """{"spec-id": 3, "fields": [{"source-id": 1, "field-id": 1000, "transform": "truncate[19]", "name": "str_truncate"}, {"source-id": 2, "field-id": 1001, "transform": "bucket[25]", "name": "int_bucket"}], "last-assigned-field-id": 1001}"""

    spec = PartitionSpec.parse_raw(json_partition_spec)

    assert spec == PartitionSpec(
        spec_id=3,
        fields=(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
            PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        ),
    )

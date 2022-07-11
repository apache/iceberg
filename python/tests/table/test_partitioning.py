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

from pyiceberg.schema import Schema
from pyiceberg.table.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import BucketTransform


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


def test_partition_spec_init(table_schema_simple: Schema):
    bucket_transform: BucketTransform = BucketTransform(4)
    id_field1 = PartitionField(3, 1001, bucket_transform, "id")
    partition_spec1 = PartitionSpec(table_schema_simple, 0, (id_field1,), 1001)

    assert partition_spec1.spec_id == 0
    assert partition_spec1.schema == table_schema_simple
    assert partition_spec1 == partition_spec1
    assert partition_spec1 != id_field1
    assert str(partition_spec1) == f"[\n  {str(id_field1)}\n]"
    assert not partition_spec1.is_unpartitioned()
    # only differ by PartitionField field_id
    id_field2 = PartitionField(3, 1002, bucket_transform, "id")
    partition_spec2 = PartitionSpec(table_schema_simple, 0, (id_field2,), 1001)
    assert partition_spec1 != partition_spec2
    assert partition_spec1.compatible_with(partition_spec2)
    assert partition_spec1.fields_by_source_id(3) == [id_field1]


def test_partition_compatible_with(table_schema_simple: Schema):
    bucket_transform: BucketTransform = BucketTransform(4)
    field1 = PartitionField(3, 100, bucket_transform, "id")
    field2 = PartitionField(3, 102, bucket_transform, "id")
    lhs = PartitionSpec(table_schema_simple, 0, (field1,), 1001)
    rhs = PartitionSpec(table_schema_simple, 0, (field1, field2), 1001)
    assert not lhs.compatible_with(rhs)


def test_unpartitioned(table_schema_simple: Schema):
    unpartitioned = PartitionSpec(table_schema_simple, 1, (), 1000)

    assert not unpartitioned.fields
    assert unpartitioned.is_unpartitioned()
    assert str(unpartitioned) == "[]"

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
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, TruncateTransform
from pyiceberg.types import (
    IntegerType,
    NestedField,
    StringType,
    StructType,
)


def test_partition_field_init() -> None:
    bucket_transform = BucketTransform(100)  # type: ignore
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


def test_unpartitioned_partition_spec_repr() -> None:
    assert repr(PartitionSpec()) == "PartitionSpec(spec_id=0)"


def test_partition_spec_init() -> None:
    bucket_transform: BucketTransform = BucketTransform(4)  # type: ignore

    id_field1 = PartitionField(3, 1001, bucket_transform, "id")
    partition_spec1 = PartitionSpec(id_field1)

    assert partition_spec1.spec_id == 0
    assert partition_spec1 == partition_spec1
    assert partition_spec1 != id_field1
    assert str(partition_spec1) == f"[\n  {str(id_field1)}\n]"
    assert not partition_spec1.is_unpartitioned()
    # only differ by PartitionField field_id
    id_field2 = PartitionField(3, 1002, bucket_transform, "id")
    partition_spec2 = PartitionSpec(id_field2)
    assert partition_spec1 != partition_spec2
    assert partition_spec1.compatible_with(partition_spec2)
    assert partition_spec1.fields_by_source_id(3) == [id_field1]
    # Does not exist
    assert partition_spec1.fields_by_source_id(1925) == []


def test_partition_compatible_with() -> None:
    bucket_transform: BucketTransform = BucketTransform(4)  # type: ignore
    field1 = PartitionField(3, 100, bucket_transform, "id")
    field2 = PartitionField(3, 102, bucket_transform, "id")
    lhs = PartitionSpec(
        field1,
    )
    rhs = PartitionSpec(field1, field2)
    assert not lhs.compatible_with(rhs)


def test_unpartitioned() -> None:
    assert len(UNPARTITIONED_PARTITION_SPEC.fields) == 0
    assert UNPARTITIONED_PARTITION_SPEC.is_unpartitioned()
    assert str(UNPARTITIONED_PARTITION_SPEC) == "[]"


def test_serialize_unpartitioned_spec() -> None:
    assert UNPARTITIONED_PARTITION_SPEC.model_dump_json() == """{"spec-id":0,"fields":[]}"""


def test_serialize_partition_spec() -> None:
    partitioned = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )
    assert (
        partitioned.model_dump_json()
        == """{"spec-id":3,"fields":[{"source-id":1,"field-id":1000,"transform":"truncate[19]","name":"str_truncate"},{"source-id":2,"field-id":1001,"transform":"bucket[25]","name":"int_bucket"}]}"""
    )


def test_deserialize_unpartition_spec() -> None:
    json_partition_spec = """{"spec-id":0,"fields":[]}"""
    spec = PartitionSpec.model_validate_json(json_partition_spec)

    assert spec == PartitionSpec(spec_id=0)


def test_deserialize_partition_spec() -> None:
    json_partition_spec = """{"spec-id": 3, "fields": [{"source-id": 1, "field-id": 1000, "transform": "truncate[19]", "name": "str_truncate"}, {"source-id": 2, "field-id": 1001, "transform": "bucket[25]", "name": "int_bucket"}]}"""

    spec = PartitionSpec.model_validate_json(json_partition_spec)

    assert spec == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )


def test_partition_type(table_schema_simple: Schema) -> None:
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )

    assert spec.partition_type(table_schema_simple) == StructType(
        NestedField(field_id=1000, name="str_truncate", field_type=StringType(), required=False),
        NestedField(field_id=1001, name="int_bucket", field_type=IntegerType(), required=False),
    )

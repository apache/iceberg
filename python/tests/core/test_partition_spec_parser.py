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

from iceberg.api import PartitionSpec, Schema
from iceberg.api.types import DecimalType, IntegerType, NestedField, StringType
from iceberg.core import PartitionSpecParser
import pytest


def test_to_json_conversion():
    spec_schema = Schema(NestedField.required(1, "id", IntegerType.get()),
                         NestedField.required(2, "data", StringType.get()),
                         NestedField.required(3, "num", DecimalType.of(9, 2)))

    spec = PartitionSpec \
        .builder_for(spec_schema) \
        .identity("id") \
        .bucket("data", 16) \
        .add_without_field_id(2, "data1", "bucket[16]") \
        .add(2, 1010, "data2", "bucket[8]") \
        .bucket("num", 8) \
        .build()

    expected = '{"spec-id": 0, "fields": [' \
               '{"name": "id", "transform": "identity", "source-id": 1, "field-id": 1000}, ' \
               '{"name": "data_bucket", "transform": "bucket[16]", "source-id": 2, "field-id": 1001}, ' \
               '{"name": "data1", "transform": "bucket[16]", "source-id": 2, "field-id": 1002}, ' \
               '{"name": "data2", "transform": "bucket[8]", "source-id": 2, "field-id": 1010}, ' \
               '{"name": "num_bucket", "transform": "bucket[8]", "source-id": 3, "field-id": 1011}]}'
    assert expected == PartitionSpecParser.to_json(spec)


def test_from_json_conversion_with_field_ids():
    spec_schema = Schema(NestedField.required(1, "id", IntegerType.get()),
                         NestedField.required(2, "data", StringType.get()),
                         NestedField.required(3, "num", DecimalType.of(9, 2)))

    spec_string = '{"spec-id": 0, "fields": [' \
                  '{"name": "id", "transform": "identity", "source-id": 1, "field-id": 1000}, ' \
                  '{"name": "data_bucket", "transform": "bucket[16]", "source-id": 2, "field-id": 1001}, ' \
                  '{"name": "data1", "transform": "bucket[16]", "source-id": 2, "field-id": 1002}, ' \
                  '{"name": "data2", "transform": "bucket[8]", "source-id": 2, "field-id": 1010}, ' \
                  '{"name": "num_bucket", "transform": "bucket[8]", "source-id": 3, "field-id": 1011}]}'

    spec = PartitionSpecParser.from_json(spec_schema, spec_string)

    expected_spec = PartitionSpec \
        .builder_for(spec_schema) \
        .identity("id") \
        .bucket("data", 16) \
        .add_without_field_id(2, "data1", "bucket[16]") \
        .add(2, 1010, "data2", "bucket[8]") \
        .bucket("num", 8) \
        .build()
    assert expected_spec == spec


def test_from_json_conversion_without_field_ids():
    spec_schema = Schema(NestedField.required(1, "id", IntegerType.get()),
                         NestedField.required(2, "data", StringType.get()),
                         NestedField.required(3, "num", DecimalType.of(9, 2)))

    spec_string = '{"spec-id": 0, "fields": [' \
                  '{"name": "id", "transform": "identity", "source-id": 1}, ' \
                  '{"name": "data_bucket", "transform": "bucket[16]", "source-id": 2}, ' \
                  '{"name": "data1", "transform": "bucket[16]", "source-id": 2}, ' \
                  '{"name": "data2", "transform": "bucket[8]", "source-id": 2}, ' \
                  '{"name": "num_bucket", "transform": "bucket[8]", "source-id": 3}]}'

    spec = PartitionSpecParser.from_json(spec_schema, spec_string)

    expected_spec = PartitionSpec \
        .builder_for(spec_schema) \
        .identity("id") \
        .bucket("data", 16) \
        .add_without_field_id(2, "data1", "bucket[16]") \
        .add(2, 1003, "data2", "bucket[8]") \
        .bucket("num", 8) \
        .build()
    assert expected_spec == spec


def test_raise_exception_with_invalid_json():
    spec_schema = Schema(NestedField.required(1, "id", IntegerType.get()),
                         NestedField.required(2, "data", StringType.get()),
                         NestedField.required(3, "num", DecimalType.of(9, 2)))

    spec_string = '{"spec-id": 0, "fields": [' \
                  '{"name": "id", "transform": "identity", "source-id": 1, "field-id": 1000}, ' \
                  '{"name": "data_bucket", "transform": "bucket[16]", "source-id": 2, "field-id": 1001}, ' \
                  '{"name": "data1", "transform": "bucket[16]", "source-id": 2}, ' \
                  '{"name": "data2", "transform": "bucket[8]", "source-id": 2}, ' \
                  '{"name": "num_bucket", "transform": "bucket[8]", "source-id": 3}]}'

    with pytest.raises(RuntimeError):
        PartitionSpecParser.from_json(spec_schema, spec_string)

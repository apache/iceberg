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
from iceberg.api.types import (BinaryType,
                               DateType,
                               DecimalType,
                               FixedType,
                               IntegerType,
                               LongType,
                               NestedField,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)


def test_to_json_conversion():
    spec_schema = Schema(NestedField.required(1, "i", IntegerType.get()),
                         NestedField.required(2, "l", LongType.get()),
                         NestedField.required(3, "d", DateType.get()),
                         NestedField.required(4, "t", TimeType.get()),
                         NestedField.required(5, "ts", TimestampType.without_timezone()),
                         NestedField.required(6, "dec", DecimalType.of(9, 2)),
                         NestedField.required(7, "s", StringType.get()),
                         NestedField.required(8, "u", UUIDType.get()),
                         NestedField.required(9, "f", FixedType.of_length(3)),
                         NestedField.required(10, "b", BinaryType.get()))

    specs = [
        PartitionSpec.builder_for(spec_schema).identity("i").build(),
        PartitionSpec.builder_for(spec_schema).identity("l").build(),
        PartitionSpec.builder_for(spec_schema).identity("d").build(),
        PartitionSpec.builder_for(spec_schema).identity("t").build(),
        PartitionSpec.builder_for(spec_schema).identity("ts").build(),
        PartitionSpec.builder_for(spec_schema).identity("dec").build(),
        PartitionSpec.builder_for(spec_schema).identity("s").build(),
        PartitionSpec.builder_for(spec_schema).identity("u").build(),
        PartitionSpec.builder_for(spec_schema).identity("f").build(),
        PartitionSpec.builder_for(spec_schema).identity("b").build(),
        PartitionSpec.builder_for(spec_schema).bucket("i", 128).build(),
        PartitionSpec.builder_for(spec_schema).bucket("l", 128).build(),
        PartitionSpec.builder_for(spec_schema).bucket("d", 128).build(),
        PartitionSpec.builder_for(spec_schema).bucket("t", 128).build(),
        PartitionSpec.builder_for(spec_schema).bucket("ts", 128).build(),
        PartitionSpec.builder_for(spec_schema).bucket("dec", 128).build(),
        PartitionSpec.builder_for(spec_schema).bucket("s", 128).build(),
        PartitionSpec.builder_for(spec_schema).year("d").build(),
        PartitionSpec.builder_for(spec_schema).month("d").build(),
        PartitionSpec.builder_for(spec_schema).day("d").build(),
        PartitionSpec.builder_for(spec_schema).year("ts").build(),
        PartitionSpec.builder_for(spec_schema).month("ts").build(),
        PartitionSpec.builder_for(spec_schema).day("ts").build(),
        PartitionSpec.builder_for(spec_schema).hour("ts").build(),
        PartitionSpec.builder_for(spec_schema).truncate("i", 10).build(),
        PartitionSpec.builder_for(spec_schema).truncate("l", 10).build(),
        PartitionSpec.builder_for(spec_schema).truncate("dec", 10).build(),
        PartitionSpec.builder_for(spec_schema).truncate("s", 10).build(),
        PartitionSpec.builder_for(spec_schema).add_without_field_id(6, "dec_bucket", "bucket[16]").build(),
        PartitionSpec.builder_for(spec_schema).add(6, 1011, "dec_bucket", "bucket[16]").build(),
    ]

    expected_spec_strs = [
        "[\n 1000: i: identity(1)\n]",
        "[\n 1000: l: identity(2)\n]",
        "[\n 1000: d: identity(3)\n]",
        "[\n 1000: t: identity(4)\n]",
        "[\n 1000: ts: identity(5)\n]",
        "[\n 1000: dec: identity(6)\n]",
        "[\n 1000: s: identity(7)\n]",
        "[\n 1000: u: identity(8)\n]",
        "[\n 1000: f: identity(9)\n]",
        "[\n 1000: b: identity(10)\n]",
        "[\n 1000: i_bucket: bucket[128](1)\n]",
        "[\n 1000: l_bucket: bucket[128](2)\n]",
        "[\n 1000: d_bucket: bucket[128](3)\n]",
        "[\n 1000: t_bucket: bucket[128](4)\n]",
        "[\n 1000: ts_bucket: bucket[128](5)\n]",
        "[\n 1000: dec_bucket: bucket[128](6)\n]",
        "[\n 1000: s_bucket: bucket[128](7)\n]",
        "[\n 1000: d_year: year(3)\n]",
        "[\n 1000: d_month: month(3)\n]",
        "[\n 1000: d_day: day(3)\n]",
        "[\n 1000: ts_year: year(5)\n]",
        "[\n 1000: ts_month: month(5)\n]",
        "[\n 1000: ts_day: day(5)\n]",
        "[\n 1000: ts_hour: hour(5)\n]",
        "[\n 1000: i_truncate: truncate[10](1)\n]",
        "[\n 1000: l_truncate: truncate[10](2)\n]",
        "[\n 1000: dec_truncate: truncate[10](6)\n]",
        "[\n 1000: s_truncate: truncate[10](7)\n]",
        "[\n 1000: dec_bucket: bucket[16](6)\n]",
        "[\n 1011: dec_bucket: bucket[16](6)\n]",
    ]

    for (spec, expected_spec_str) in zip(specs, expected_spec_strs):
        assert str(spec) == expected_spec_str

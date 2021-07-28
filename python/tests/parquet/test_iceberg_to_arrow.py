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

from iceberg.parquet.iceberg_to_arrow import iceberg_to_arrow
import pyarrow as pa


def schema_equals(base: pa.Schema, other: pa.Schema):
    for i in range(len(base.names)):
        assert base.field(i) == other.field(i)

    return True


def test_convert_primitives(iceberg_primitive_schema):
    expected_arrow_schema = pa.schema([pa.field("int_col", pa.int32(), False, {b"PARQUET:field_id": b"1"}),
                                       pa.field("long_col", pa.int64(), False, {b"PARQUET:field_id": b"2"}),
                                       pa.field("float_col", pa.float32(), False, {b"PARQUET:field_id": b"3"}),
                                       pa.field("double_col", pa.float64(), False, {b"PARQUET:field_id": b"4"}),
                                       pa.field("decimal_col", pa.decimal128(38, 5), False, {b"PARQUET:field_id": b"5"}),
                                       pa.field("date_col", pa.date32(), False, {b"PARQUET:field_id": b"6"}),
                                       pa.field("string_col", pa.string(), False, {b"PARQUET:field_id": b"7"}),
                                       pa.field("time_col", pa.time64("us"), False, {b"PARQUET:field_id": b"8"}),
                                       pa.field("ts_col", pa.timestamp("us"), False, {b"PARQUET:field_id": b"9"}),
                                       pa.field("ts_w_tz_col", pa.timestamp("us", "UTC"), False,
                                                {b"PARQUET:field_id": b"10"}),
                                       pa.field("bin_col", pa.binary(), False, {b"PARQUET:field_id": b"11"}),
                                       pa.field("fixed_bin_col", pa.binary(10), False, {b"PARQUET:field_id": b"12"})
                                       ])
    converted = iceberg_to_arrow(iceberg_primitive_schema)
    assert schema_equals(expected_arrow_schema, converted)


def test_convert_struct_type(iceberg_struct_schema):

    arrow_struct = pa.struct([pa.field("a", pa.int32(), False, {b"PARQUET:field_id": b"2"}),
                              pa.field("b", pa.int32(), False, {b"PARQUET:field_id": b"3"})])
    arrow_schema = pa.schema([pa.field("struct_col", arrow_struct, False, {b"PARQUET:field_id": b"1"})])

    converted = iceberg_to_arrow(iceberg_struct_schema)
    assert schema_equals(arrow_schema, converted)


def test_convert_list(iceberg_list_schema):
    element_field = pa.field("element", pa.int32(), False, {b"PARQUET:field_id": b"2"})
    arrow_schema = pa.schema([pa.field("list_col", pa.list_(element_field), False, {b"PARQUET:field_id": b"1"})])
    converted = iceberg_to_arrow(iceberg_list_schema)

    assert schema_equals(arrow_schema, converted)


def test_primitive_nullabilty(iceberg_simple_nullability_schema):
    arrow_schema = pa.schema([pa.field("int_col", pa.int32(), False, {b"PARQUET:field_id": b"1"}),
                              pa.field("long_col", pa.int64(), True, {b"PARQUET:field_id": b"2"})])

    converted = iceberg_to_arrow(iceberg_simple_nullability_schema)
    assert schema_equals(arrow_schema, converted)


def test_nested_nullabilty(iceberg_nested_nullability_schema):

    element_field = pa.field("element", pa.int32(), True, {b"PARQUET:field_id": b"5"})
    arrow_struct = pa.struct([pa.field("a", pa.int32(), False, {b"PARQUET:field_id": b"3"}),
                              pa.field("b", pa.int32(), True, {b"PARQUET:field_id": b"4"}), ])
    arrow_schema = pa.schema([pa.field("struct_col", arrow_struct, True, {b"PARQUET:field_id": b"1"}),
                              pa.field("list_col", pa.list_(element_field), False, {b"PARQUET:field_id": b"2"})])

    converted = iceberg_to_arrow(iceberg_nested_nullability_schema)
    assert schema_equals(arrow_schema, converted)

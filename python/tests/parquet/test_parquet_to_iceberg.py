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

from iceberg.api import Schema
from iceberg.api.types import (BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FloatType,
                               IntegerType,
                               ListType,
                               LongType,
                               NestedField,
                               StringType,
                               StructType,
                               TimestampType)
from iceberg.parquet.parquet_to_iceberg import convert_parquet_to_iceberg


def compare_schema(expected_schema, converted_schema):

    assert len(expected_schema) == len(converted_schema)
    for field in expected_schema.as_struct().fields:
        converted_field = converted_schema.find_field(field.id)
        assert converted_field is not None
        assert field.name == converted_field.name
        assert field.is_optional == converted_field.is_optional
        assert field.type == converted_field.type


def test_primitive_types(primitive_type_test_parquet_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(3, "str_col", StringType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(6, "decimal_col", DecimalType.of(9, 2)),
                              NestedField.optional(7, "big_decimal_col", DecimalType.of(19, 5)),
                              NestedField.optional(8, "huge_decimal_col", DecimalType.of(38, 9)),
                              NestedField.optional(9, "date_col", DateType.get()),
                              NestedField.optional(10, "ts_col", TimestampType.without_timezone()),
                              NestedField.optional(11, "ts_wtz_col", TimestampType.with_timezone()),
                              NestedField.optional(12, "bool_col", BooleanType.get())])
    compare_schema(expected_schema, convert_parquet_to_iceberg(primitive_type_test_parquet_file))


def test_unnested_complex_types(unnested_complex_type_test_parquet_file):
    expected_schema = Schema([NestedField.optional(1, "list_int_col", ListType.of_optional(3, IntegerType.get())),
                              NestedField.optional(4, "list_str_col", ListType.of_optional(6, StringType.get())),
                              NestedField.optional(7, "struct_col",
                                                   StructType.of([NestedField.optional(8, "f1", IntegerType.get()),
                                                                  NestedField.optional(9, "f2", StringType.get())]))
                              ])
    converted_schema = convert_parquet_to_iceberg(unnested_complex_type_test_parquet_file)
    compare_schema(expected_schema, converted_schema)


def test_nested_complex_types():
    # This is a placeholder until pyarrow fully supports reading and writing nested types.
    # Following a release containing this PR:
    # https://github.com/apache/arrow/pull/8177
    pass

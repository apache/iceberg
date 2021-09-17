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
from iceberg.api.expressions import Expressions
from iceberg.api.types import (BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FloatType,
                               IntegerType,
                               LongType,
                               NestedField,
                               StringType,
                               TimestampType)
from iceberg.core.filesystem import FileSystemInputFile, get_fs
from iceberg.parquet import ParquetReader
import pyarrow as pa


def test_basic_read(primitive_type_test_file, pyarrow_primitive_array, pyarrow_schema):
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

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)

    source_table = pa.table(pyarrow_primitive_array, schema=pyarrow_schema)
    assert reader.read() == source_table


def test_projection(primitive_type_test_file, pyarrow_primitive_array, pyarrow_schema):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)

    source_table = pa.table(pyarrow_primitive_array, schema=pyarrow_schema)
    num_cols = source_table.num_columns
    for i in range(1, num_cols - 1):
        source_table = source_table.remove_column(num_cols - i)

    assert source_table == reader.read()


def test_column_rename(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(3, "string_col", StringType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)
    pyarrow_array = [pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                     pa.array([1, 2, 3, None, 5], type=pa.int64()),
                     pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64())]
    schema = pa.schema([pa.field("int_col", pa.int32(), False),
                        pa.field("bigint_col", pa.int64(), True),
                        pa.field("string_col", pa.string(), True),
                        pa.field("float_col", pa.float32(), True),
                        pa.field("dbl_col", pa.float64(), True)])

    source_table = pa.table(pyarrow_array, schema=schema)

    target_table = reader.read()
    assert source_table == target_table


def test_column_add(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(3, "string_col", StringType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(13, "int_col2", IntegerType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)
    pyarrow_array = [pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                     pa.array([1, 2, 3, None, 5], type=pa.int64()),
                     pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64()),
                     pa.array([None, None, None, None, None], type=pa.int32())]
    source_table = pa.table(pyarrow_array, schema=pa.schema([pa.field("int_col", pa.int32(), nullable=False),
                                                             pa.field("bigint_col", pa.int64(), nullable=True),
                                                             pa.field("string_col", pa.string(), nullable=True),
                                                             pa.field("float_col", pa.float32(), nullable=True),
                                                             pa.field("dbl_col", pa.float64(), nullable=True),
                                                             pa.field("int_col2", pa.int32(), nullable=True),
                                                             ]))

    target_table = reader.read()
    assert source_table == target_table


def test_decimal_column_add(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(13, "new_dec_col", DecimalType.of(38, 9))
                              ])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)
    pyarrow_array = [pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                     pa.array([1, 2, 3, None, 5], type=pa.int64()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64()),
                     pa.array([None, None, None, None, None], type=pa.decimal128(38, 9))]

    source_table = pa.table(pyarrow_array, schema=pa.schema([pa.field("int_col", pa.int32(), nullable=False),
                                                             pa.field("bigint_col", pa.int64(), nullable=True),
                                                             pa.field("float_col", pa.float32(), nullable=True),
                                                             pa.field("dbl_col", pa.float64(), nullable=True),
                                                             pa.field("new_dec_col", pa.decimal128(38, 9), nullable=True)
                                                             ]))

    target_table = reader.read()
    assert source_table == target_table


def test_column_reorder(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(3, "string_col", StringType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)
    pyarrow_array = [pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                     pa.array([1, 2, 3, None, 5], type=pa.int64()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
                     pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64()),
                     pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string())]
    source_table = pa.table(pyarrow_array, schema=pa.schema([pa.field("int_col", pa.int32(), nullable=False),
                                                             pa.field("bigint_col", pa.int64(), nullable=True),
                                                             pa.field("float_col", pa.float32(), nullable=True),
                                                             pa.field("dbl_col", pa.float64(), nullable=True),
                                                             pa.field("string_col", pa.string(), nullable=True)
                                                             ]))

    target_table = reader.read()
    assert source_table == target_table


def test_column_upcast(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", LongType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.always_true(), True)
    pyarrow_array = [pa.array([1, 2, 3, 4, 5], type=pa.int32())]
    source_table = pa.table(pyarrow_array, schema=pa.schema([pa.field("int_col", pa.int64(), nullable=False)]))

    target_table = reader.read()
    assert source_table == target_table


def test_filter(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(3, "string_col", StringType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.equal("string_col", "us"), True)
    pyarrow_array = [pa.array([1, 3, 4], type=pa.int32()),
                     pa.array([1, 3, None], type=pa.int64()),
                     pa.array([1.0, 3.0, 4.0], type=pa.float32()),
                     pa.array([1.0, 3.0, 4.0], type=pa.float64()),
                     pa.array(['us', 'us', 'us'], type=pa.string())]
    source_table = pa.table(pyarrow_array, schema=pa.schema([pa.field("int_col", pa.int32(), nullable=False),
                                                             pa.field("bigint_col", pa.int64(), nullable=True),
                                                             pa.field("float_col", pa.float32(), nullable=True),
                                                             pa.field("dbl_col", pa.float64(), nullable=True),
                                                             pa.field("string_col", pa.string(), nullable=True)
                                                             ]))

    target_table = reader.read()
    assert source_table == target_table


def test_compound_filter(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(3, "string_col", StringType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.and_(Expressions.equal("string_col", "us"),
                                                                             Expressions.equal("int_col", 1)),
                           True)
    pyarrow_array = [pa.array([1], type=pa.int32()),
                     pa.array([1], type=pa.int64()),
                     pa.array([1.0], type=pa.float32()),
                     pa.array([1.0], type=pa.float64()),
                     pa.array(['us'], type=pa.string())]

    source_table = pa.table(pyarrow_array, schema=pa.schema([pa.field("int_col", pa.int32(), nullable=False),
                                                             pa.field("bigint_col", pa.int64(), nullable=True),
                                                             pa.field("float_col", pa.float32(), nullable=True),
                                                             pa.field("dbl_col", pa.float64(), nullable=True),
                                                             pa.field("string_col", pa.string(), nullable=True)
                                                             ]))

    target_table = reader.read()
    assert source_table == target_table


def test_schema_evolution_filter(primitive_type_test_file):
    expected_schema = Schema([NestedField.required(1, "int_col", IntegerType.get()),
                              NestedField.optional(2, "bigint_col", LongType.get()),
                              NestedField.optional(16, "other_new_col", LongType.get()),
                              NestedField.optional(4, "float_col", FloatType.get()),
                              NestedField.optional(5, "dbl_col", DoubleType.get()),
                              NestedField.optional(3, "string_col", StringType.get()),
                              NestedField.optional(15, "new_col", StringType.get())])

    input_file = FileSystemInputFile(get_fs(primitive_type_test_file, conf={}), primitive_type_test_file, {})
    reader = ParquetReader(input_file, expected_schema, {}, Expressions.not_null("new_col"), True)

    schema = pa.schema([pa.field("int_col", pa.int32(), nullable=False),
                        pa.field("bigint_col", pa.int64(), nullable=True),
                        pa.field("other_new_col", pa.int64(), nullable=True),
                        pa.field("float_col", pa.float32(), nullable=True),
                        pa.field("dbl_col", pa.float64(), nullable=True),
                        pa.field("string_col", pa.string(), nullable=True),
                        pa.field("new_col", pa.string(), nullable=True)])

    pyarrow_not_null_array = [pa.array([], type=pa.int32()),
                              pa.array([], type=pa.int64()),
                              pa.array([], type=pa.int32()),
                              pa.array([], type=pa.float32()),
                              pa.array([], type=pa.float64()),
                              pa.array([], type=pa.string()),
                              pa.array([], type=pa.string())]

    not_null_table = pa.table(pyarrow_not_null_array, schema=schema)
    pyarrow_null_array = [pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                          pa.array([1, 2, 3, None, 5], type=pa.int64()),
                          pa.array([None, None, None, None, None], type=pa.int64()),
                          pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
                          pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64()),
                          pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
                          pa.array([None, None, None, None, None], type=pa.string())]
    null_table = pa.table(pyarrow_null_array, schema=schema)

    target_table = reader.read()
    assert not_null_table == target_table

    reader = ParquetReader(input_file, expected_schema, {}, Expressions.is_null("new_col"), True)
    target_table = reader.read()
    assert null_table == target_table

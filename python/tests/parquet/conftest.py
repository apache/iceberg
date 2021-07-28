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

from collections import namedtuple
from datetime import datetime
from decimal import Decimal
from tempfile import NamedTemporaryFile

from iceberg.api import Schema
from iceberg.api.types import (BinaryType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               ListType,
                               LongType,
                               MapType,
                               NestedField,
                               StringType,
                               StructType,
                               TimestampType,
                               TimeType)
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


TestRowGroupColumnStatistics = namedtuple("TestRowGroupColumnStatistics", ["min", "max", "null_count"])
TestRowGroupColumn = namedtuple("TestRowGroupColumn", ["path_in_schema",
                                                       "file_offset",
                                                       "total_compressed_size",
                                                       "statistics"])


class TestArrowParquetMetadata:
    __test__ = False

    def __init__(self, col_metadata, num_rows=100):
        self._col_metadata = col_metadata
        self._num_rows = num_rows

    @property
    def num_rows(self):
        return self._num_rows

    @property
    def num_columns(self):
        return len(self._col_metadata)

    def column(self, i):
        return self._col_metadata[i]

    def __getitem__(self, index):
        return self._col_metadata[index]


@pytest.fixture(scope="session")
def pyarrow_array():
    return [pa.array([1, 2, 3, None, 5], type=pa.int32()),
            pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
            pa.array([[0], [1, 2], [1], [1, 2, 3], None], type=pa.list_(pa.int64())),
            pa.array([True, None, False, True, True], pa.bool_())]


@pytest.fixture(scope="session")
def pytable_colnames():
    return ['int_col', 'str_col', 'list_col', 'bool_col']


@pytest.fixture(scope="session")
def rg_expected_schema():
    return Schema([NestedField.required(1, "string_col", StringType.get()),
                   NestedField.required(2, "long_col", LongType.get()),
                   NestedField.required(3, "int_col", IntegerType.get()),
                   NestedField.optional(4, "float_col", FloatType.get()),
                   NestedField.optional(5, "null_col", StringType.get()),
                   NestedField.optional(6, "missing_col", StringType.get()),
                   NestedField.optional(7, "no_stats_col", StringType.get()),
                   NestedField.optional(8, "ts_wtz_col", TimestampType.with_timezone()),
                   NestedField.optional(9, "ts_wotz_col", TimestampType.without_timezone()),
                   NestedField.optional(10, "big_decimal_type", DecimalType.of(38, 5)),
                   NestedField.optional(11, "small_decimal_type", DecimalType.of(10, 2)),
                   NestedField.optional(12, "date_type", DateType.get()),
                   ])


@pytest.fixture(scope="session")
def rg_expected_schema_map():
    return {"string_col": "string_col",
            "long_col": "long_col",
            "int_col_renamed": "int_col",
            "float_col": "float_col",
            "null_col": "null_col",
            "no_stats_col": "no_stats_col",
            "ts_wtz_col": "ts_wtz_col",
            "ts_wotz_col": "ts_wotz_col",
            "big_decimal_type": "big_decimal_type",
            "small_decimal_type": "small_decimal_type",
            "date_type": "date_type"
            }


@pytest.fixture(scope="session")
def rg_col_metadata():
    return [TestRowGroupColumn("string_col", 4, 12345, TestRowGroupColumnStatistics("b", "e", 0)),
            TestRowGroupColumn("long_col", 12349, 12345, TestRowGroupColumnStatistics(0, 1234567890123, 0)),
            TestRowGroupColumn("int_col_renamed", 24698, 12345, TestRowGroupColumnStatistics(0, 12345, 0)),
            TestRowGroupColumn("float_col", 37043, 12345, TestRowGroupColumnStatistics(0.0, 123.45, 50)),
            TestRowGroupColumn("null_col", 49388, 4, TestRowGroupColumnStatistics(None, None, 100)),
            TestRowGroupColumn("no_stats_col", 61733, 4, None),
            TestRowGroupColumn("ts_wtz_col", 74078, 4,
                               TestRowGroupColumnStatistics(datetime.strptime("2019-01-01 00:00:00-0000",
                                                                              "%Y-%m-%d %H:%M:%S%z"),
                                                            datetime.strptime("2019-12-31 00:00:00-0000",
                                                                              "%Y-%m-%d %H:%M:%S%z"),
                                                            0)),
            TestRowGroupColumn("ts_wotz_col", 86423, 4,
                               TestRowGroupColumnStatistics(datetime.strptime("2019-01-01 00:00:00-0000",
                                                                              "%Y-%m-%d %H:%M:%S%z"),
                                                            datetime.strptime("2019-12-31 00:00:00-0000",
                                                                              "%Y-%m-%d %H:%M:%S%z"),
                                                            10)),
            TestRowGroupColumn("big_decimal_type", 98768, 4,
                               # -123456789012345678.12345 to 123456789012345678.12345
                               TestRowGroupColumnStatistics(b'\xff\xff\xff\xff\xff\xff\xfdb\xbdI\xb1\x89\x8e\xbe\xeb\x07',
                                                            b'\x00\x00\x00\x00\x00\x00\x02\x9dB\xb6NvqA\x14\xf9',
                                                            10)),
            TestRowGroupColumn("small_decimal_type", 111113, 4,
                               # 0 to 123.45
                               TestRowGroupColumnStatistics(0,
                                                            12345,
                                                            10)),
            TestRowGroupColumn("date_type", 123458, 4,
                               # 2020-01-01 to 2020-12-31
                               TestRowGroupColumnStatistics(18262, 18262 + 365, 10))
            ]


@pytest.fixture(scope="session")
def pyarrow_schema():
    return pa.schema([pa.field("int_col", pa.int32(), False),
                      pa.field("bigint_col", pa.int64(), True),
                      pa.field("str_col", pa.string(), True),
                      pa.field("float_col", pa.float32(), True),
                      pa.field("dbl_col", pa.float64(), True),
                      pa.field("decimal_col",
                               pa.decimal128(9, 2), True),
                      pa.field("big_decimal_col",
                               pa.decimal128(19, 5), True),
                      pa.field("huge_decimal_col",
                               pa.decimal128(38, 9), True),
                      pa.field("date_col", pa.date32(), True),
                      pa.field("ts_col", pa.timestamp('us'), True),
                      pa.field("ts_wtz_col",
                               pa.timestamp('us',
                                            'America/New_York'),
                               True),
                      pa.field("bool_col", pa.bool_(), True)])


@pytest.fixture(scope="session")
def pyarrow_primitive_array():
    return [pa.array([1, 2, 3, 4, 5], type=pa.int32()),
            pa.array([1, 2, 3, None, 5], type=pa.int64()),
            pa.array(['us', 'can', 'us', 'us', 'can'], type=pa.string()),
            pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float32()),
            pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64()),
            pa.array([Decimal("1.0"), Decimal("2.0"), Decimal("3.0"),
                      Decimal("4.0"), Decimal("5.0")], type=pa.decimal128(9, 2)),
            pa.array([Decimal("1.0"), Decimal("2.0"), Decimal("3.0"),
                      Decimal("4.0"), Decimal("5.0")], type=pa.decimal128(19, 5)),
            pa.array([Decimal("1.0"), Decimal("2.0"), Decimal("3.0"),
                      Decimal("4.0"), Decimal("5.0")], type=pa.decimal128(38, 9)),
            pa.array([18506, 18507, 18508, 18508, 18510], type=pa.date32()),
            pa.array([1598918400000000, 1599004800000000,
                      1599091200000000, 1599177600000000, 1599264000000000],
                     type=pa.timestamp("us")),
            pa.array([1598918400000000, 1599004800000000,
                      1599091200000000, 1599177600000000, 1599264000000000],
                     type=pa.timestamp("us", 'America/New_York')),
            pa.array([True, None, False, True, True], pa.bool_())]


@pytest.fixture(scope="session")
def primitive_type_test_file(pyarrow_primitive_array, pyarrow_schema):
    with NamedTemporaryFile() as temp_file:
        pq.write_table(pa.table(pyarrow_primitive_array, schema=pyarrow_schema), temp_file.name)
        yield temp_file.name


@pytest.fixture(scope="session")
def primitive_type_test_parquet_file(primitive_type_test_file):
    yield pq.ParquetFile(primitive_type_test_file)


@pytest.fixture(scope="session")
def unnested_complex_type_test_parquet_file():
    struct_fields = [("f1", pa.int32()), ("f2", pa.string())]
    struct_type = pa.struct(struct_fields)
    pyarrow_array = [pa.array([[1, 2, 3], [4, None, 6], None, [7, 8, 9]], type=pa.list_(pa.int32())),
                     pa.array([["a", "b", "c"], ["d", None, "e"], None, ["f", "g", "h"]], type=pa.list_(pa.string())),
                     pa.array([None, None, {"f1": 3, "f2": "c"}, {"f1": 4, "f2": "d"}], type=struct_type)
                     ]
    with NamedTemporaryFile() as temp_file:
        pq.write_table(pa.table(pyarrow_array, names=["list_int_col", 'list_str_col', 'struct_col']),
                       temp_file.name)
        yield pq.ParquetFile(temp_file.name)


@pytest.fixture(scope="session")
def parquet_schema(type_test_parquet_file):
    return type_test_parquet_file.schema


@pytest.fixture(scope="session")
def arrow_schema(type_test_parquet_file):
    return type_test_parquet_file.schema_arrow


@pytest.fixture(scope="session")
def iceberg_primitive_schema():
    return Schema([NestedField.required(1, "int_col", IntegerType.get()),
                   NestedField.required(2, "long_col", LongType.get()),
                   NestedField.required(3, "float_col", FloatType.get()),
                   NestedField.required(4, "double_col", DoubleType.get()),
                   NestedField.required(5, "decimal_col", DecimalType.of(38, 5)),
                   NestedField.required(6, "date_col", DateType.get()),
                   NestedField.required(7, "string_col", StringType.get()),
                   NestedField.required(8, "time_col", TimeType.get()),
                   NestedField.required(9, "ts_col", TimestampType.without_timezone()),
                   NestedField.required(10, "ts_w_tz_col", TimestampType.with_timezone()),
                   NestedField.required(11, "bin_col", BinaryType.get()),
                   NestedField.required(12, "fixed_bin_col", FixedType.of_length(10))
                   ])


@pytest.fixture(scope="session")
def iceberg_struct_schema():
    struct_type = StructType.of([NestedField.required(2, "a", IntegerType.get()),
                                 NestedField.required(3, "b", IntegerType.get())])
    return Schema([NestedField.required(1, "struct_col", struct_type)])


@pytest.fixture(scope="session")
def iceberg_map_schema():
    map_type = MapType.of_required(2, 3, IntegerType.get(), StringType.get())
    return Schema([NestedField.required(1, "map_col", map_type)])


@pytest.fixture(scope="session")
def iceberg_simple_nullability_schema():
    return Schema([NestedField.required(1, "int_col", IntegerType.get()),
                   NestedField.optional(2, "long_col", LongType.get())])


@pytest.fixture(scope="session")
def iceberg_nested_nullability_schema():
    struct_type = StructType.of([NestedField.required(3, "a", IntegerType.get()),
                                 NestedField.optional(4, "b", IntegerType.get())])
    list_type = ListType.of_optional(5, IntegerType.get())
    return Schema([NestedField.optional(1, "struct_col", struct_type),
                   NestedField.required(2, "list_col", list_type)])


@pytest.fixture(scope="session")
def iceberg_list_schema():
    return Schema([NestedField.required(1,
                                        "list_col",
                                        ListType.of_required("2", IntegerType.get()))])

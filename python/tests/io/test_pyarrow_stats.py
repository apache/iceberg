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
# pylint: disable=protected-access,unused-argument,redefined-outer-name

import math
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.avro import (
    STRUCT_BOOL,
    STRUCT_DOUBLE,
    STRUCT_FLOAT,
    STRUCT_INT32,
    STRUCT_INT64,
)
from pyiceberg.io.pyarrow import (
    MetricModeTypes,
    MetricsMode,
    PyArrowStatisticsCollector,
    compute_statistics_plan,
    fill_parquet_file_metadata,
    match_metrics_mode,
    parquet_path_to_id_mapping,
    schema_to_pyarrow,
)
from pyiceberg.manifest import DataFile
from pyiceberg.schema import Schema, pre_order_visit
from pyiceberg.table.metadata import (
    TableMetadata,
    TableMetadataUtil,
    TableMetadataV1,
    TableMetadataV2,
)
from pyiceberg.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
)
from pyiceberg.utils.datetime import date_to_days, datetime_to_micros, time_to_micros


@dataclass(frozen=True)
class TestStruct:
    x: Optional[int]
    y: Optional[float]


def construct_test_table() -> Tuple[Any, Any, Union[TableMetadataV1, TableMetadataV2]]:
    table_metadata = {
        "format-version": 2,
        "location": "s3://bucket/test/location",
        "last-column-id": 7,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "strings", "required": False, "type": "string"},
                    {"id": 2, "name": "floats", "required": False, "type": "float"},
                    {
                        "id": 3,
                        "name": "list",
                        "required": False,
                        "type": {"type": "list", "element-id": 6, "element": "long", "element-required": False},
                    },
                    {
                        "id": 4,
                        "name": "maps",
                        "required": False,
                        "type": {
                            "type": "map",
                            "key-id": 7,
                            "key": "long",
                            "value-id": 8,
                            "value": "long",
                            "value-required": False,
                        },
                    },
                    {
                        "id": 5,
                        "name": "structs",
                        "required": False,
                        "type": {
                            "type": "struct",
                            "fields": [
                                {"id": 9, "name": "x", "required": False, "type": "long"},
                                {"id": 10, "name": "y", "required": False, "type": "float", "doc": "comment"},
                            ],
                        },
                    },
                ],
            },
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {},
    }

    table_metadata = TableMetadataUtil.parse_obj(table_metadata)
    arrow_schema = schema_to_pyarrow(table_metadata.schemas[0])

    _strings = ["zzzzzzzzzzzzzzzzzzzz", "rrrrrrrrrrrrrrrrrrrr", None, "aaaaaaaaaaaaaaaaaaaa"]

    _floats = [3.14, math.nan, 1.69, 100]

    _list = [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]]

    _maps: List[Optional[Dict[int, int]]] = [
        {1: 2, 3: 4},
        None,
        {5: 6},
        {},
    ]

    _structs = [
        asdict(TestStruct(1, 0.2)),
        asdict(TestStruct(None, -1.34)),
        None,
        asdict(TestStruct(54, None)),
    ]

    table = pa.Table.from_pydict(
        {
            "strings": _strings,
            "floats": _floats,
            "list": _list,
            "maps": _maps,
            "structs": _structs,
        },
        schema=arrow_schema,
    )
    metadata_collector: List[Any] = []

    with pa.BufferOutputStream() as f:
        with pq.ParquetWriter(f, table.schema, metadata_collector=metadata_collector) as writer:
            writer.write_table(table)

        return f.getvalue(), metadata_collector[0], table_metadata


def get_current_schema(
    table_metadata: TableMetadata,
) -> Schema:
    return next(filter(lambda s: s.schema_id == table_metadata.current_schema_id, table_metadata.schemas))


def test_record_count() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )
    assert datafile.record_count == 4


def test_file_size() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert datafile.file_size_in_bytes == len(file_bytes)


def test_value_counts() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 7
    assert datafile.value_counts[1] == 4
    assert datafile.value_counts[2] == 4
    assert datafile.value_counts[6] == 10  # 3 lists with 3 items and a None value
    assert datafile.value_counts[7] == 5
    assert datafile.value_counts[8] == 5
    assert datafile.value_counts[9] == 4
    assert datafile.value_counts[10] == 4


def test_column_sizes() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.column_sizes) == 7
    # these values are an artifact of how the write_table encodes the columns
    assert datafile.column_sizes[1] > 0
    assert datafile.column_sizes[2] > 0
    assert datafile.column_sizes[6] > 0
    assert datafile.column_sizes[7] > 0
    assert datafile.column_sizes[8] > 0


def test_null_and_nan_counts() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.null_value_counts) == 7
    assert datafile.null_value_counts[1] == 1
    assert datafile.null_value_counts[2] == 0
    assert datafile.null_value_counts[6] == 1
    assert datafile.null_value_counts[7] == 2
    assert datafile.null_value_counts[8] == 2
    assert datafile.null_value_counts[9] == 2
    assert datafile.null_value_counts[10] == 2

    # #arrow does not include this in the statistics
    # assert len(datafile.nan_value_counts)  == 3
    # assert datafile.nan_value_counts[1]    == 0
    # assert datafile.nan_value_counts[2]    == 1
    # assert datafile.nan_value_counts[3]    == 0


def test_bounds() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.lower_bounds) == 2
    assert datafile.lower_bounds[1].decode() == "aaaaaaaaaaaaaaaa"
    assert datafile.lower_bounds[2] == STRUCT_FLOAT.pack(1.69)

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1].decode() == "zzzzzzzzzzzzzzz{"
    assert datafile.upper_bounds[2] == STRUCT_FLOAT.pack(100)


def test_metrics_mode_parsing() -> None:
    assert match_metrics_mode("none") == MetricsMode(MetricModeTypes.NONE)
    assert match_metrics_mode("nOnE") == MetricsMode(MetricModeTypes.NONE)
    assert match_metrics_mode("counts") == MetricsMode(MetricModeTypes.COUNTS)
    assert match_metrics_mode("Counts") == MetricsMode(MetricModeTypes.COUNTS)
    assert match_metrics_mode("full") == MetricsMode(MetricModeTypes.FULL)
    assert match_metrics_mode("FuLl") == MetricsMode(MetricModeTypes.FULL)
    assert match_metrics_mode(" FuLl") == MetricsMode(MetricModeTypes.FULL)

    assert match_metrics_mode("truncate(16)") == MetricsMode(MetricModeTypes.TRUNCATE, 16)
    assert match_metrics_mode("trUncatE(16)") == MetricsMode(MetricModeTypes.TRUNCATE, 16)
    assert match_metrics_mode("trUncatE(7)") == MetricsMode(MetricModeTypes.TRUNCATE, 7)
    assert match_metrics_mode("trUncatE(07)") == MetricsMode(MetricModeTypes.TRUNCATE, 7)

    with pytest.raises(ValueError) as exc_info:
        match_metrics_mode("trUncatE(-7)")
    assert "Malformed truncate: trUncatE(-7)" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        match_metrics_mode("trUncatE(0)")
    assert "Truncation length must be larger than 0" in str(exc_info.value)


def test_metrics_mode_none() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "none"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 0
    assert len(datafile.null_value_counts) == 0
    assert len(datafile.nan_value_counts) == 0
    assert len(datafile.lower_bounds) == 0
    assert len(datafile.upper_bounds) == 0


def test_metrics_mode_counts() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "counts"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 7
    assert len(datafile.null_value_counts) == 7
    assert len(datafile.nan_value_counts) == 0
    assert len(datafile.lower_bounds) == 0
    assert len(datafile.upper_bounds) == 0


def test_metrics_mode_full() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "full"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 7
    assert len(datafile.null_value_counts) == 7
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 2
    assert datafile.lower_bounds[1].decode() == "aaaaaaaaaaaaaaaaaaaa"
    assert datafile.lower_bounds[2] == STRUCT_FLOAT.pack(1.69)

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1].decode() == "zzzzzzzzzzzzzzzzzzzz"
    assert datafile.upper_bounds[2] == STRUCT_FLOAT.pack(100)


def test_metrics_mode_non_default_trunc() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "truncate(2)"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 7
    assert len(datafile.null_value_counts) == 7
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 2
    assert datafile.lower_bounds[1].decode() == "aa"
    assert datafile.lower_bounds[2] == STRUCT_FLOAT.pack(1.69)

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1].decode() == "z{"
    assert datafile.upper_bounds[2] == STRUCT_FLOAT.pack(100)


def test_column_metrics_mode() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "truncate(2)"
    table_metadata.properties["write.metadata.metrics.column.strings"] = "none"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 6
    assert len(datafile.null_value_counts) == 6
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 1
    assert datafile.lower_bounds[2] == STRUCT_FLOAT.pack(1.69)
    assert 1 not in datafile.lower_bounds

    assert len(datafile.upper_bounds) == 1
    assert datafile.upper_bounds[2] == STRUCT_FLOAT.pack(100)
    assert 1 not in datafile.upper_bounds


def construct_test_table_primitive_types() -> Tuple[Any, Any, Union[TableMetadataV1, TableMetadataV2]]:
    table_metadata = {
        "format-version": 2,
        "location": "s3://bucket/test/location",
        "last-column-id": 7,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "booleans", "required": False, "type": "boolean"},
                    {"id": 2, "name": "ints", "required": False, "type": "int"},
                    {"id": 3, "name": "longs", "required": False, "type": "long"},
                    {"id": 4, "name": "floats", "required": False, "type": "float"},
                    {"id": 5, "name": "doubles", "required": False, "type": "double"},
                    {"id": 6, "name": "dates", "required": False, "type": "date"},
                    {"id": 7, "name": "times", "required": False, "type": "time"},
                    {"id": 8, "name": "timestamps", "required": False, "type": "timestamp"},
                    {"id": 9, "name": "timestamptzs", "required": False, "type": "timestamptz"},
                    {"id": 10, "name": "strings", "required": False, "type": "string"},
                    {"id": 11, "name": "uuids", "required": False, "type": "uuid"},
                    {"id": 12, "name": "binaries", "required": False, "type": "binary"},
                ],
            },
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {},
    }

    table_metadata = TableMetadataUtil.parse_obj(table_metadata)
    arrow_schema = schema_to_pyarrow(table_metadata.schemas[0])
    tz = timezone(timedelta(seconds=19800))

    booleans = [True, False]
    ints = [23, 89]
    longs = [54, 2]
    floats = [454.1223, 24342.29]
    doubles = [8542.12, -43.9]
    dates = [date(2022, 1, 2), date(2023, 2, 4)]
    times = [time(17, 30, 34), time(13, 21, 4)]
    timestamps = [datetime(2022, 1, 2, 17, 30, 34, 399), datetime(2023, 2, 4, 13, 21, 4, 354)]
    timestamptzs = [datetime(2022, 1, 2, 17, 30, 34, 399, tz), datetime(2023, 2, 4, 13, 21, 4, 354, tz)]
    strings = ["hello", "world"]
    uuids = [uuid.uuid3(uuid.NAMESPACE_DNS, "foo").bytes, uuid.uuid3(uuid.NAMESPACE_DNS, "bar").bytes]
    binaries = [b"hello", b"world"]

    table = pa.Table.from_pydict(
        {
            "booleans": booleans,
            "ints": ints,
            "longs": longs,
            "floats": floats,
            "doubles": doubles,
            "dates": dates,
            "times": times,
            "timestamps": timestamps,
            "timestamptzs": timestamptzs,
            "strings": strings,
            "uuids": uuids,
            "binaries": binaries,
        },
        schema=arrow_schema,
    )

    metadata_collector: List[Any] = []

    with pa.BufferOutputStream() as f:
        with pq.ParquetWriter(f, table.schema, metadata_collector=metadata_collector) as writer:
            writer.write_table(table)

        return f.getvalue(), metadata_collector[0], table_metadata


def test_metrics_primitive_types() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table_primitive_types()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "truncate(2)"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 12
    assert len(datafile.null_value_counts) == 12
    assert len(datafile.nan_value_counts) == 0

    tz = timezone(timedelta(seconds=19800))

    assert len(datafile.lower_bounds) == 12
    assert datafile.lower_bounds[1] == STRUCT_BOOL.pack(False)
    assert datafile.lower_bounds[2] == STRUCT_INT32.pack(23)
    assert datafile.lower_bounds[3] == STRUCT_INT64.pack(2)
    assert datafile.lower_bounds[4] == STRUCT_FLOAT.pack(454.1223)
    assert datafile.lower_bounds[5] == STRUCT_DOUBLE.pack(-43.9)
    assert datafile.lower_bounds[6] == STRUCT_INT32.pack(date_to_days(date(2022, 1, 2)))
    assert datafile.lower_bounds[7] == STRUCT_INT64.pack(time_to_micros(time(13, 21, 4)))
    assert datafile.lower_bounds[8] == STRUCT_INT64.pack(datetime_to_micros(datetime(2022, 1, 2, 17, 30, 34, 399)))
    assert datafile.lower_bounds[9] == STRUCT_INT64.pack(datetime_to_micros(datetime(2022, 1, 2, 17, 30, 34, 399, tz)))
    assert datafile.lower_bounds[10] == b"he"
    assert datafile.lower_bounds[11] == uuid.uuid3(uuid.NAMESPACE_DNS, "foo").bytes
    assert datafile.lower_bounds[12] == b"he"

    assert len(datafile.upper_bounds) == 12
    assert datafile.upper_bounds[1] == STRUCT_BOOL.pack(True)
    assert datafile.upper_bounds[2] == STRUCT_INT32.pack(89)
    assert datafile.upper_bounds[3] == STRUCT_INT64.pack(54)
    assert datafile.upper_bounds[4] == STRUCT_FLOAT.pack(24342.29)
    assert datafile.upper_bounds[5] == STRUCT_DOUBLE.pack(8542.12)
    assert datafile.upper_bounds[6] == STRUCT_INT32.pack(date_to_days(date(2023, 2, 4)))
    assert datafile.upper_bounds[7] == STRUCT_INT64.pack(time_to_micros(time(17, 30, 34)))
    assert datafile.upper_bounds[8] == STRUCT_INT64.pack(datetime_to_micros(datetime(2023, 2, 4, 13, 21, 4, 354)))
    assert datafile.upper_bounds[9] == STRUCT_INT64.pack(datetime_to_micros(datetime(2023, 2, 4, 13, 21, 4, 354, tz)))
    assert datafile.upper_bounds[10] == b"wp"
    assert datafile.upper_bounds[11] == uuid.uuid3(uuid.NAMESPACE_DNS, "bar").bytes
    assert datafile.upper_bounds[12] == b"wp"


def construct_test_table_invalid_upper_bound() -> Tuple[Any, Any, Union[TableMetadataV1, TableMetadataV2]]:
    table_metadata = {
        "format-version": 2,
        "location": "s3://bucket/test/location",
        "last-column-id": 7,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "valid_upper_binary", "required": False, "type": "binary"},
                    {"id": 2, "name": "invalid_upper_binary", "required": False, "type": "binary"},
                    {"id": 3, "name": "valid_upper_string", "required": False, "type": "string"},
                    {"id": 4, "name": "invalid_upper_string", "required": False, "type": "string"},
                ],
            },
        ],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "properties": {},
    }

    table_metadata = TableMetadataUtil.parse_obj(table_metadata)
    arrow_schema = schema_to_pyarrow(table_metadata.schemas[0])

    valid_binaries = [b"\x00\x00\x00", b"\xff\xfe\x00"]
    invalid_binaries = [b"\x00\x00\x00", b"\xff\xff\x00"]

    valid_strings = ["\x00\x00\x00", "".join([chr(0x10FFFF), chr(0x10FFFE), chr(0x0)])]
    invalid_strings = ["\x00\x00\x00", "".join([chr(0x10FFFF), chr(0x10FFFF), chr(0x0)])]

    table = pa.Table.from_pydict(
        {
            "valid_upper_binary": valid_binaries,
            "invalid_upper_binary": invalid_binaries,
            "valid_upper_string": valid_strings,
            "invalid_upper_string": invalid_strings,
        },
        schema=arrow_schema,
    )

    metadata_collector: List[Any] = []

    with pa.BufferOutputStream() as f:
        with pq.ParquetWriter(f, table.schema, metadata_collector=metadata_collector) as writer:
            writer.write_table(table)

        return f.getvalue(), metadata_collector[0], table_metadata


def test_metrics_invalid_upper_bound() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table_invalid_upper_bound()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    table_metadata.properties["write.metadata.metrics.default"] = "truncate(2)"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert len(datafile.value_counts) == 4
    assert len(datafile.null_value_counts) == 4
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 4
    assert datafile.lower_bounds[1] == b"\x00\x00"
    assert datafile.lower_bounds[2] == b"\x00\x00"
    assert datafile.lower_bounds[3] == b"\x00\x00"
    assert datafile.lower_bounds[4] == b"\x00\x00"

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1] == b"\xff\xff"
    assert datafile.upper_bounds[3] == "".join([chr(0x10FFFF), chr(0x10FFFF)]).encode()


def test_offsets() -> None:
    (file_bytes, metadata, table_metadata) = construct_test_table()

    schema = get_current_schema(table_metadata)
    datafile = DataFile()
    fill_parquet_file_metadata(
        datafile,
        metadata,
        len(file_bytes),
        compute_statistics_plan(schema, table_metadata.properties),
        parquet_path_to_id_mapping(schema),
    )

    assert datafile.split_offsets is not None
    assert len(datafile.split_offsets) == 1
    assert datafile.split_offsets[0] == 4


def test_write_and_read_stats_schema(table_schema_nested: Schema) -> None:
    tbl = pa.Table.from_pydict(
        {
            "foo": ["a", "b"],
            "bar": [1, 2],
            "baz": [False, True],
            "qux": [["a", "b"], ["c", "d"]],
            "quux": [[("a", (("aa", 1), ("ab", 2)))], [("b", (("ba", 3), ("bb", 4)))]],
            "location": [[(52.377956, 4.897070), (4.897070, -122.431297)], [(43.618881, -116.215019), (41.881832, -87.623177)]],
            "person": [("Fokko", 33), ("Max", 42)],  # Possible data quality issue
        },
        schema=schema_to_pyarrow(table_schema_nested),
    )
    stats_columns = pre_order_visit(table_schema_nested, PyArrowStatisticsCollector(table_schema_nested, {}))

    visited_paths = []

    def file_visitor(written_file: Any) -> None:
        visited_paths.append(written_file)

    with tempfile.TemporaryDirectory() as tmpdir:
        pq.write_to_dataset(tbl, tmpdir, file_visitor=file_visitor)

    assert visited_paths[0].metadata.num_columns == len(stats_columns)


def test_stats_types(table_schema_nested: Schema) -> None:
    stats_columns = pre_order_visit(table_schema_nested, PyArrowStatisticsCollector(table_schema_nested, {}))

    # the field-ids should be sorted
    assert all(stats_columns[i].field_id <= stats_columns[i + 1].field_id for i in range(len(stats_columns) - 1))
    assert [col.iceberg_type for col in stats_columns] == [
        StringType(),
        IntegerType(),
        BooleanType(),
        StringType(),
        StringType(),
        StringType(),
        IntegerType(),
        FloatType(),
        FloatType(),
        StringType(),
        IntegerType(),
    ]


# This is commented out for now because write_to_dataset drops the partition
# columns making it harder to calculate the mapping from the column index to
# datatype id
#
# def test_dataset() -> pa.Buffer:

#     table_metadata = {
#         "format-version": 2,
#         "location": "s3://bucket/test/location",
#         "last-column-id": 7,
#         "current-schema-id": 0,
#         "schemas": [
#             {
#                 "type": "struct",
#                 "schema-id": 0,
#                 "fields": [
#                     {"id": 1, "name": "ints", "required": False, "type": "long"},
#                     {"id": 2, "name": "even", "required": False, "type": "boolean"},
#                 ],
#             },
#         ],
#         "default-spec-id": 0,
#         "partition-specs": [{"spec-id": 0, "fields": []}],
#         "properties": {},
#     }

#     table_metadata = TableMetadataUtil.parse_obj(table_metadata)
#     schema = schema_to_pyarrow(table_metadata.schemas[0])

#     _ints = [0, 2, 4, 8, 1, 3, 5, 7]
#     parity = [True, True, True, True, False, False, False, False]

#     table = pa.Table.from_pydict({"ints": _ints, "even": parity}, schema=schema)

#     visited_paths = []

#     def file_visitor(written_file: Any) -> None:
#         visited_paths.append(written_file)

#     with TemporaryDirectory() as tmpdir:
#         pq.write_to_dataset(table, tmpdir, partition_cols=["even"], file_visitor=file_visitor)

#     even = None
#     odd = None

#     assert len(visited_paths) == 2

#     for written_file in visited_paths:
#         df = DataFile()

#         fill_parquet_file_metadata(df, written_file.metadata, written_file.size, table_metadata)

#         if "even=true" in written_file.path:
#             even = df

#         if "even=false" in written_file.path:
#             odd = df

#     assert even is not None
#     assert odd is not None

#     assert len(even.value_counts) == 1
#     assert even.value_counts[1] == 4
#     assert len(even.lower_bounds) == 1
#     assert even.lower_bounds[1] == STRUCT_INT64.pack(0)
#     assert len(even.upper_bounds) == 1
#     assert even.upper_bounds[1] == STRUCT_INT64.pack(8)

#     assert len(odd.value_counts) == 1
#     assert odd.value_counts[1] == 4
#     assert len(odd.lower_bounds) == 1
#     assert odd.lower_bounds[1] == STRUCT_INT64.pack(1)
#     assert len(odd.upper_bounds) == 1
#     assert odd.upper_bounds[1] == STRUCT_INT64.pack(7)

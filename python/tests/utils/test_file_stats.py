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


import math
import struct
from tempfile import TemporaryDirectory
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.manifest import DataFile
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.utils.file_stats import BOUND_TRUNCATED_LENGHT, fill_parquet_file_metadata, parquet_schema_to_ids


def construct_test_table() -> pa.Buffer:
    schema = pa.schema(
        [pa.field("strings", pa.string()), pa.field("floats", pa.float64()), pa.field("list", pa.list_(pa.int64()))]
    )

    _strings = ["zzzzzzzzzzzzzzzzzzzz", "rrrrrrrrrrrrrrrrrrrr", None, "aaaaaaaaaaaaaaaaaaaa"]

    _floats = [3.14, math.nan, 1.69, 100]

    _list = [[1, 2, 3], [4, 5, 6], None, [7, 8, 9]]

    table = pa.Table.from_pydict({"strings": _strings, "floats": _floats, "list": _list}, schema=schema)
    f = pa.BufferOutputStream()

    metadata_collector: List[Any] = []
    writer = pq.ParquetWriter(f, table.schema, metadata_collector=metadata_collector)

    writer.write_table(table)
    writer.close()

    print(writer.writer)
    print(writer.writer.metadata)

    return f.getvalue(), metadata_collector[0]


def test_record_count() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert datafile.record_count == 4


def test_file_size() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert datafile.file_size_in_bytes == len(file_bytes)


def test_value_counts() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert len(datafile.value_counts) == 3
    assert datafile.value_counts[1] == 4
    assert datafile.value_counts[2] == 4
    assert datafile.value_counts[3] == 10  # 3 lists with 3 items and a None value


def test_column_sizes() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert len(datafile.column_sizes) == 3
    # these values are an artifact of how the write_table encodes the columns
    assert datafile.column_sizes[1] == 116
    assert datafile.column_sizes[2] == 119
    assert datafile.column_sizes[3] == 151


def test_null_and_nan_counts() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert len(datafile.null_value_counts) == 3
    assert datafile.null_value_counts[1] == 1
    assert datafile.null_value_counts[2] == 0
    assert datafile.null_value_counts[3] == 1

    # #arrow does not include this in the statistics
    # assert len(datafile.nan_value_counts)  == 3
    # assert datafile.nan_value_counts[1]    == 0
    # assert datafile.nan_value_counts[2]    == 1
    # assert datafile.nan_value_counts[3]    == 0


def test_bounds() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert len(datafile.lower_bounds) == 2
    assert datafile.lower_bounds[1].decode() == "aaaaaaaaaaaaaaaaaaaa"[:BOUND_TRUNCATED_LENGHT]
    assert datafile.lower_bounds[2] == struct.pack("<d", 1.69)

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1].decode() == "zzzzzzzzzzzzzzzzzzzz"[:BOUND_TRUNCATED_LENGHT]
    assert datafile.upper_bounds[2] == struct.pack("<d", 100)


def test_metrics_mode_none(example_table_metadata_v2: Dict[str, Any]) -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    table_metadata = TableMetadataUtil.parse_obj(example_table_metadata_v2)
    table_metadata.properties["write.metadata.metrics.default"] = "none"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        {"strings": 1, "floats": 2, "list.list.item": 3},
        len(file_bytes),
        table_metadata,
    )

    assert len(datafile.value_counts) == 0
    assert len(datafile.null_value_counts) == 0
    assert len(datafile.nan_value_counts) == 0
    assert len(datafile.lower_bounds) == 0
    assert len(datafile.upper_bounds) == 0


def test_metrics_mode_counts(example_table_metadata_v2: Dict[str, Any]) -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    table_metadata = TableMetadataUtil.parse_obj(example_table_metadata_v2)
    table_metadata.properties["write.metadata.metrics.default"] = "counts"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        {"strings": 1, "floats": 2, "list.list.item": 3},
        len(file_bytes),
        table_metadata,
    )

    assert len(datafile.value_counts) == 3
    assert len(datafile.null_value_counts) == 3
    assert len(datafile.nan_value_counts) == 0
    assert len(datafile.lower_bounds) == 0
    assert len(datafile.upper_bounds) == 0


def test_metrics_mode_full(example_table_metadata_v2: Dict[str, Any]) -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    table_metadata = TableMetadataUtil.parse_obj(example_table_metadata_v2)
    table_metadata.properties["write.metadata.metrics.default"] = "full"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        {"strings": 1, "floats": 2, "list.list.item": 3},
        len(file_bytes),
        table_metadata,
    )

    assert len(datafile.value_counts) == 3
    assert len(datafile.null_value_counts) == 3
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 2
    assert datafile.lower_bounds[1].decode() == "aaaaaaaaaaaaaaaaaaaa"
    assert datafile.lower_bounds[2] == struct.pack("<d", 1.69)

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1].decode() == "zzzzzzzzzzzzzzzzzzzz"
    assert datafile.upper_bounds[2] == struct.pack("<d", 100)


def test_metrics_mode_non_default_trunc(example_table_metadata_v2: Dict[str, Any]) -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    table_metadata = TableMetadataUtil.parse_obj(example_table_metadata_v2)
    table_metadata.properties["write.metadata.metrics.default"] = "truncate(2)"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        {"strings": 1, "floats": 2, "list.list.item": 3},
        len(file_bytes),
        table_metadata,
    )

    assert len(datafile.value_counts) == 3
    assert len(datafile.null_value_counts) == 3
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 2
    assert datafile.lower_bounds[1].decode() == "aa"
    assert datafile.lower_bounds[2] == struct.pack("<d", 1.69)[:2]

    assert len(datafile.upper_bounds) == 2
    assert datafile.upper_bounds[1].decode() == "zz"
    assert datafile.upper_bounds[2] == struct.pack("<d", 100)[:2]


def test_column_metrics_mode(example_table_metadata_v2: Dict[str, Any]) -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    table_metadata = TableMetadataUtil.parse_obj(example_table_metadata_v2)
    table_metadata.properties["write.metadata.metrics.default"] = "truncate(2)"
    table_metadata.properties["write.metadata.metrics.column.strings"] = "none"
    fill_parquet_file_metadata(
        datafile,
        metadata,
        {"strings": 1, "floats": 2, "list.list.item": 3},
        len(file_bytes),
        table_metadata,
    )

    assert len(datafile.value_counts) == 2
    assert len(datafile.null_value_counts) == 2
    assert len(datafile.nan_value_counts) == 0

    assert len(datafile.lower_bounds) == 1
    assert datafile.lower_bounds[2] == struct.pack("<d", 1.69)[:2]

    assert len(datafile.upper_bounds) == 1
    assert datafile.upper_bounds[2] == struct.pack("<d", 100)[:2]


def test_offsets() -> None:
    (file_bytes, metadata) = construct_test_table()

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, metadata, {"strings": 1, "floats": 2, "list.list.item": 3}, len(file_bytes))

    assert datafile.split_offsets is not None
    assert len(datafile.split_offsets) == 1
    assert datafile.split_offsets[0] == 4


def test_dataset() -> pa.Buffer:
    schema = pa.schema([pa.field("ints", pa.int64()), pa.field("even", pa.bool_())])

    _ints = [0, 2, 4, 8, 1, 3, 5, 7]
    parity = [True, True, True, True, False, False, False, False]

    table = pa.Table.from_pydict({"ints": _ints, "even": parity}, schema=schema)

    visited_paths = []

    def file_visitor(written_file: Any) -> None:
        visited_paths.append(written_file)

    with TemporaryDirectory() as tmpdir:
        pq.write_to_dataset(table, tmpdir, partition_cols=["even"], file_visitor=file_visitor)

    even = None
    odd = None

    assert len(visited_paths) == 2

    for written_file in visited_paths:
        df = DataFile()

        fill_parquet_file_metadata(df, written_file.metadata, {"ints": 1, "even": 2}, written_file.size)

        if "even=true" in written_file.path:
            even = df

        if "even=false" in written_file.path:
            odd = df

    assert even is not None
    assert odd is not None

    assert len(even.value_counts) == 1
    assert even.value_counts[1] == 4
    assert len(even.lower_bounds) == 1
    assert even.lower_bounds[1] == struct.pack("<q", 0)
    assert len(even.upper_bounds) == 1
    assert even.upper_bounds[1] == struct.pack("<q", 8)

    assert len(odd.value_counts) == 1
    assert odd.value_counts[1] == 4
    assert len(odd.lower_bounds) == 1
    assert odd.lower_bounds[1] == struct.pack("<q", 1)
    assert len(odd.upper_bounds) == 1
    assert odd.upper_bounds[1] == struct.pack("<q", 7)


def test_schema_mapping() -> None:
    json_schema = """
    {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {
        "id": 1,
        "name": "_8bit",
        "required": false,
        "type": "int"
        },
        {
        "id": 2,
        "name": "_16bit",
        "required": false,
        "type": "int"
        },
        {
        "id": 3,
        "name": "_32bit",
        "required": false,
        "type": "int"
        },
        {
        "id": 4,
        "name": "_64bit",
        "required": false,
        "type": "long"
        },
        {
        "id": 5,
        "name": "_float",
        "required": false,
        "type": "float"
        },
        {
        "id": 6,
        "name": "_double",
        "required": false,
        "type": "double"
        },
        {
        "id": 7,
        "name": "_decimal",
        "required": false,
        "type": "decimal(10, 0)"
        },
        {
        "id": 8,
        "name": "_timestamp",
        "required": false,
        "type": "timestamptz"
        },
        {
        "id": 9,
        "name": "_date",
        "required": false,
        "type": "date"
        },
        {
        "id": 10,
        "name": "_string",
        "required": false,
        "type": "string"
        },
        {
        "id": 11,
        "name": "_varchar",
        "required": false,
        "type": "string"
        },
        {
        "id": 12,
        "name": "_char",
        "required": false,
        "type": "string"
        },
        {
        "id": 13,
        "name": "_boolean",
        "required": false,
        "type": "boolean"
        },
        {
        "id": 14,
        "name": "_binary",
        "required": false,
        "type": "binary"
        },
        {
        "id": 15,
        "name": "_array",
        "required": false,
        "type": {
            "type": "list",
            "element-id": 18,
            "element": "int",
            "element-required": false
        }
        },
        {
        "id": 16,
        "name": "_map",
        "required": false,
        "type": {
            "type": "map",
            "key-id": 19,
            "key": "int",
            "value-id": 20,
            "value": "int",
            "value-required": false
        }
        },
        {
        "id": 17,
        "name": "_struct",
        "required": false,
        "type": {
            "type": "struct",
            "fields": [
            {
                "id": 21,
                "name": "_field1",
                "required": false,
                "type": "int"
            },
            {
                "id": 22,
                "name": "_field2",
                "required": false,
                "type": "int"
            }
            ]
        }
        }
    ]
    }
    """

    schema = Schema.parse_raw(json_schema)

    mapping = parquet_schema_to_ids(schema)

    print(mapping)

    assert mapping == {
        "_8bit": 1,
        "_16bit": 2,
        "_32bit": 3,
        "_64bit": 4,
        "_float": 5,
        "_double": 6,
        "_decimal": 7,
        "_timestamp": 8,
        "_date": 9,
        "_string": 10,
        "_varchar": 11,
        "_char": 12,
        "_boolean": 13,
        "_binary": 14,
        "_array.list.element": 18,
        "_map.key_value.key": 19,
        "_map.key_value.value": 20,
        "_struct._field1": 21,
        "_struct._field2": 22,
    }

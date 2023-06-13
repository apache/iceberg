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


import pyarrow as pa
import pyarrow.parquet as pq
import math

from pyiceberg.utils.file_stats import fill_parquet_file_metadata, serialize_to_binary, BOUND_TRUNCATED_LENGHT
from pyiceberg.manifest import DataFile
import datetime
import binascii
import struct

def test_boolean_scalar():
    scalar1 = pa.scalar(True)
    assert serialize_to_binary(scalar1) == struct.pack('?', True)

    scalar2 = pa.scalar(False)
    assert serialize_to_binary(scalar2) == struct.pack('?', False)

def test_int8_scalar():
    scalar = pa.scalar(123, type=pa.int8())
    assert serialize_to_binary(scalar) == struct.pack('<b', 123)

def test_int32_scalar():
    scalar = pa.scalar(123456789, type=pa.int32())
    assert serialize_to_binary(scalar) == struct.pack('<i', 123456789)

def test_int64_scalar():
    scalar = pa.scalar(1234567891011121314, type=pa.int64())
    assert serialize_to_binary(scalar) == struct.pack('<q', 1234567891011121314)

def test_float_scalar():
    scalar = pa.scalar(123.456, type=pa.float32())
    assert serialize_to_binary(scalar) == struct.pack('<f', 123.456)

def test_double_scalar():
    scalar = pa.scalar(123.456, type=pa.float64())
    assert serialize_to_binary(scalar) == struct.pack('<d', 123.456)

def test_string_scalar():
    scalar = pa.scalar('abc')
    assert serialize_to_binary(scalar) == 'abc'.encode()

def test_date32_scalar():
    scalar = pa.scalar(datetime.date(1970, 1, 2), type=pa.date32())
    reference = (datetime.date(1970, 1, 2) - datetime.date(1970, 1, 1)).days
    assert serialize_to_binary(scalar) == struct.pack('<i', reference)

def test_time32_scalar():
    scalar = pa.scalar(datetime.time(1, 2, 3, 456000), type=pa.time64('us'))
    assert serialize_to_binary(scalar) == struct.pack('<q', int(1*60*60*1e6 + 2*60*1e6 + 3* 1e6 + 456000))

def test_timestamp_scalar():
    scalar = pa.scalar(datetime.datetime(2023, 1, 1, 1, 2, 3), type=pa.timestamp('us'))
    reference = int((datetime.datetime(2023, 1, 1, 1,2,3) - datetime.datetime(1970, 1, 1, 0,0,0)).total_seconds()*1e6)
    assert serialize_to_binary(scalar) == struct.pack('<q', reference)

def construct_test_table() -> pa.Buffer:

    schema = pa.schema([
        pa.field("strings", pa.string()),
        pa.field('floats',  pa.float64()),
        pa.field('list',    pa.list_(pa.int64()))
    ])
        
    _strings = [
        "zzzzzzzzzzzzzzzzzzzz",
        "rrrrrrrrrrrrrrrrrrrr",
        None,
        "aaaaaaaaaaaaaaaaaaaa"
    ]

    _floats = [
        3.14,
        math.nan,
        1.69,
        100
    ]

    _list = [
        [ 1, 2, 3],
        [ 4, 5, 6],
        None,
        [ 7, 8, 9]
    ]
        
    table = pa.Table.from_pydict({"strings": _strings, "floats": _floats, "list": _list}, schema=schema)
    f = pa.BufferOutputStream()

    pq.write_table(table, f)

    return f.getvalue()

def test_record_count() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert datafile.record_count   == 4


def test_file_size() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert datafile.file_size_in_bytes   == 1558


def test_value_counts() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert len(datafile.value_counts) == 3
    assert datafile.value_counts[1]   == 4
    assert datafile.value_counts[2]   == 4
    assert datafile.value_counts[3]   == 10 # 3 lists with 3 items and a None value


def test_column_sizes() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert len(datafile.column_sizes) == 3
    # these values are an artifact of how the write_table encodes the columns
    assert datafile.column_sizes[1]   == 116
    assert datafile.column_sizes[2]   == 119
    assert datafile.column_sizes[3]   == 151


def test_null_and_nan_counts() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert len(datafile.null_value_counts) == 3
    assert datafile.null_value_counts[1]   == 1
    assert datafile.null_value_counts[2]   == 0
    assert datafile.null_value_counts[3]   == 1

    assert len(datafile.nan_value_counts)  == 3
    assert datafile.nan_value_counts[1]    == 0
    assert datafile.nan_value_counts[2]    == 1
    assert datafile.nan_value_counts[3]    == 0


def test_bounds() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert len(datafile.lower_bounds)          == 2
    assert datafile.lower_bounds[1].decode()   == "aaaaaaaaaaaaaaaaaaaa"[:BOUND_TRUNCATED_LENGHT]
    assert datafile.lower_bounds[2]            == struct.pack('<d', 1.69)

    assert len(datafile.upper_bounds)          == 2
    assert datafile.upper_bounds[1].decode()   == "zzzzzzzzzzzzzzzzzzzz"[:BOUND_TRUNCATED_LENGHT]
    assert datafile.upper_bounds[2]            == struct.pack('<d', 100)


def test_offsets() -> None:

    file_obj = pa.BufferReader(construct_test_table())

    datafile = DataFile()
    fill_parquet_file_metadata(datafile, file_obj)

    assert len(datafile.split_offsets) == 1
    assert datafile.split_offsets[0]   == 4
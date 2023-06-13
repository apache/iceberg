#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from pyiceberg.manifest import DataFile, FileFormat
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
import struct
import datetime

BOUND_TRUNCATED_LENGHT = 16

# Serialization rules: https://iceberg.apache.org/spec/#binary-single-value-serialization
#
# Type      Binary serialization
# boolean   0x00 for false, non-zero byte for true
# int       Stored as 4-byte little-endian
# long      Stored as 8-byte little-endian
# float     Stored as 4-byte little-endian
# double    Stored as 8-byte little-endian
# date      Stores days from the 1970-01-01 in an 4-byte little-endian int
# time      Stores microseconds from midnight in an 8-byte little-endian long
# timestamp without zone	Stores microseconds from 1970-01-01 00:00:00.000000 in an 8-byte little-endian long
# timestamp with zone	Stores microseconds from 1970-01-01 00:00:00.000000 UTC in an 8-byte little-endian long
# string    UTF-8 bytes (without length)
# uuid      16-byte big-endian value, see example in Appendix B
# fixed(L)  Binary value
# binary    Binary value (without length)
#
def serialize_to_binary(scalar: pa.Scalar) -> bytes:
    value = scalar.as_py()
    if isinstance(scalar, pa.BooleanScalar):
        return struct.pack('?', value)
    elif isinstance(scalar, (pa.Int8Scalar, pa.UInt8Scalar)):
        return struct.pack('<b', value)
    elif isinstance(scalar, (pa.Int16Scalar, pa.UInt16Scalar)):
        return struct.pack('<h', value)
    elif isinstance(scalar, (pa.Int32Scalar, pa.UInt32Scalar)):
        return struct.pack('<i', value)
    elif isinstance(scalar, (pa.Int64Scalar, pa.UInt64Scalar)):
        return struct.pack('<q', value)
    elif isinstance(scalar, pa.FloatScalar):
        return struct.pack('<f', value)
    elif isinstance(scalar, pa.DoubleScalar):
        return struct.pack('<d', value)
    elif isinstance(scalar, pa.StringScalar):
        return value.encode('utf-8')
    elif isinstance(scalar, pa.BinaryScalar):
        return value
    elif isinstance(scalar, (pa.Date32Scalar, pa.Date64Scalar)):
        epoch = datetime.date(1970, 1, 1)
        days = (value - epoch).days
        return struct.pack('<i', days)
    elif isinstance(scalar, (pa.Time32Scalar, pa.Time64Scalar)):
        microseconds = int(value.hour * 60 * 60 * 1e6 +
                        value.minute * 60 * 1e6 +
                        value.second * 1e6 +
                        value.microsecond)
        return struct.pack('<q', microseconds)
    elif isinstance(scalar, pa.TimestampScalar):
        epoch = datetime.datetime(1970, 1, 1)
        microseconds = int((value - epoch).total_seconds() * 1e6)
        return struct.pack('<q', microseconds)
    else:
        raise TypeError('Unsupported type: {}'.format(type(scalar)))


def fill_parquet_file_metadata(df: DataFile, file_object: pa.NativeFile) -> DataFile:
    
    parquet_file = pq.ParquetFile(file_object)
    metadata = parquet_file.metadata

    column_sizes = {}
    value_counts = {}

    for r in range(metadata.num_row_groups):
        for c in range(metadata.num_columns):
            column_sizes[c+1] = column_sizes.get(c+1, 0) + metadata.row_group(r).column(c).total_compressed_size
            value_counts[c+1] = value_counts.get(c+1, 0) + metadata.row_group(r).column(c).num_values


    # References:
    # https://github.com/apache/iceberg/blob/fc381a81a1fdb8f51a0637ca27cd30673bd7aad3/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L232
    # https://github.com/apache/parquet-mr/blob/ac29db4611f86a07cc6877b416aa4b183e09b353/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/metadata/ColumnChunkMetaData.java#L184
    split_offsets = []
    for r in range(metadata.num_row_groups):
        data_offset       = metadata.row_group(r).column(0).data_page_offset
        dictionary_offset = metadata.row_group(r).column(0).dictionary_page_offset
        if metadata.row_group(r).column(0).has_dictionary_page and dictionary_offset < data_offset:
            split_offsets.append(dictionary_offset)
        else:
            split_offsets.append(data_offset)

    split_offsets.sort()

    pf = pa.parquet.read_table(file_object)

    null_value_counts = {}
    nan_value_counts  = {}
    lower_bounds      = {}
    upper_bounds      = {}

    for c in range(metadata.num_columns):
        null_value_counts[c+1] = pf.filter(pc.field(c).is_null(nan_is_null=False)).num_rows
        nan_value_counts[c+1]  = pf.filter(pc.field(c).is_null(nan_is_null=True)).num_rows - null_value_counts[c+1]

        try:
            lower = pc.min(pf[c])
            upper = pc.max(pf[c])

            lower_bounds[c+1] = serialize_to_binary(lower)[:BOUND_TRUNCATED_LENGHT]
            upper_bounds[c+1] = serialize_to_binary(upper)[:BOUND_TRUNCATED_LENGHT]
        except pa.lib.ArrowNotImplementedError:
            # skip bound detection for types such as lists
            pass


    df.file_format        = FileFormat.PARQUET
    df.record_count       = parquet_file.metadata.num_rows
    df.file_size_in_bytes = file_object.size()
    df.column_sizes       = column_sizes
    df.value_counts       = value_counts
    df.null_value_counts  = null_value_counts
    df.nan_value_counts   = nan_value_counts
    df.lower_bounds       = lower_bounds
    df.upper_bounds       = upper_bounds
    df.split_offsets      = split_offsets
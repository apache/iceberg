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

import struct
from typing import (
    Any,
    Dict,
    List,
    Union,
)

import pyarrow.lib
import pyarrow.parquet as pq

from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema, SchemaVisitor, visit
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)

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


def bool_to_avro(value: bool) -> bytes:
    return struct.pack("?", value)


def int32_to_avro(value: int) -> bytes:
    return struct.pack("<i", value)


def int64_to_avro(value: int) -> bytes:
    return struct.pack("<q", value)


def float_to_avro(value: float) -> bytes:
    return struct.pack("<f", value)


def double_to_avro(value: float) -> bytes:
    return struct.pack("<d", value)


def bytes_to_avro(value: Union[bytes, str]) -> bytes:
    if type(value) == str:
        return value.encode()
    else:
        assert isinstance(value, bytes)  # appeases mypy
        return value


class StatsAggregator:
    def __init__(self, type_string: str):
        self.current_min: Any = None
        self.current_max: Any = None
        self.serialize: Any = None

        if type_string == "BOOLEAN":
            self.serialize = bool_to_avro
        elif type_string == "INT32":
            self.serialize = int32_to_avro
        elif type_string == "INT64":
            self.serialize = int64_to_avro
        elif type_string == "INT96":
            raise NotImplementedError("Statistics not implemented for INT96 physical type")
        elif type_string == "FLOAT":
            self.serialize = float_to_avro
        elif type_string == "DOUBLE":
            self.serialize = double_to_avro
        elif type_string == "BYTE_ARRAY":
            self.serialize = bytes_to_avro
        elif type_string == "FIXED_LEN_BYTE_ARRAY":
            self.serialize = bytes_to_avro
        else:
            raise AssertionError(f"Unknown physical type {type_string}")

    def add_min(self, val: bytes) -> None:
        if not self.current_min:
            self.current_min = val
        elif val < self.current_min:
            self.current_min = val

    def add_max(self, val: bytes) -> None:
        if not self.current_max:
            self.current_max = val
        elif self.current_max < val:
            self.current_max = val

    def get_min(self) -> bytes:
        return self.serialize(self.current_min)[:BOUND_TRUNCATED_LENGHT]

    def get_max(self) -> bytes:
        return self.serialize(self.current_max)[:BOUND_TRUNCATED_LENGHT]


def fill_parquet_file_metadata(
    df: DataFile, metadata: pq.FileMetaData, col_path_2_iceberg_id: Dict[str, int], file_size: int
) -> None:
    """
    Computes and fills the following fields of the DataFile object.

    - file_format
    - record_count
    - file_size_in_bytes
    - column_sizes
    - value_counts
    - null_value_counts
    - nan_value_counts
    - lower_bounds
    - upper_bounds
    - split_offsets

    Args:
        df (DataFile): A DataFile object representing the Parquet file for which metadata is to be filled.
        metadata (pyarrow.parquet.FileMetaData): A pyarrow metadata object.
        col_path_2_iceberg_id: A mapping of column paths as in the `path_in_schema` attribute of the colum
            metadata to iceberg schema IDs. For scalar columns this will be the column name. For complex types
            it could be something like `my_map.key_value.value`
        file_size (int): The total compressed file size cannot be retrieved from the metadata and hence has to
            be passed here. Depending on the kind of file system and pyarrow library call used, different
            ways to obtain this value might be appropriate.
    """
    col_index_2_id = {}

    col_names = set(metadata.schema.names)

    first_group = metadata.row_group(0)

    for c in range(metadata.num_columns):
        column = first_group.column(c)
        col_path = column.path_in_schema

        if col_path in col_path_2_iceberg_id:
            col_index_2_id[c] = col_path_2_iceberg_id[col_path]
        else:
            raise AssertionError(f"Column path {col_path} couldn't be mapped to an iceberg ID")

    column_sizes: Dict[int, int] = {}
    value_counts: Dict[int, int] = {}
    split_offsets: List[int] = []

    null_value_counts: Dict[int, int] = {}
    nan_value_counts: Dict[int, int] = {}

    col_aggs = {}

    for r in range(metadata.num_row_groups):
        # References:
        # https://github.com/apache/iceberg/blob/fc381a81a1fdb8f51a0637ca27cd30673bd7aad3/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L232
        # https://github.com/apache/parquet-mr/blob/ac29db4611f86a07cc6877b416aa4b183e09b353/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/metadata/ColumnChunkMetaData.java#L184

        row_group = metadata.row_group(r)

        data_offset = row_group.column(0).data_page_offset
        dictionary_offset = row_group.column(0).dictionary_page_offset

        if row_group.column(0).has_dictionary_page and dictionary_offset < data_offset:
            split_offsets.append(dictionary_offset)
        else:
            split_offsets.append(data_offset)

        for c in range(metadata.num_columns):
            col_id = col_index_2_id[c]

            column = row_group.column(c)

            column_sizes[col_id] = column_sizes.get(col_id, 0) + column.total_compressed_size
            value_counts[col_id] = value_counts.get(col_id, 0) + column.num_values

            if column.is_stats_set:
                try:
                    statistics = column.statistics

                    null_value_counts[col_id] = null_value_counts.get(col_id, 0) + statistics.null_count

                    if column.path_in_schema in col_names:
                        # Iceberg seems to only have statistics for scalar columns

                        if col_id not in col_aggs:
                            col_aggs[col_id] = StatsAggregator(statistics.physical_type)

                        col_aggs[col_id].add_min(statistics.min)
                        col_aggs[col_id].add_max(statistics.max)

                except pyarrow.lib.ArrowNotImplementedError:
                    pass

    split_offsets.sort()

    lower_bounds = {}
    upper_bounds = {}

    for k, agg in col_aggs.items():
        lower_bounds[k] = agg.get_min()
        upper_bounds[k] = agg.get_max()

    df.file_format = FileFormat.PARQUET
    df.record_count = metadata.num_rows
    df.file_size_in_bytes = file_size
    df.column_sizes = column_sizes
    df.value_counts = value_counts
    df.null_value_counts = null_value_counts
    df.nan_value_counts = nan_value_counts
    df.lower_bounds = lower_bounds
    df.upper_bounds = upper_bounds
    df.split_offsets = split_offsets


class _IndexByParquetPath(SchemaVisitor[Dict[str, int]]):
    """A schema visitor for generating a parquet path to field ID index."""

    def __init__(self) -> None:
        self._index: Dict[str, int] = {}
        self._field_names: List[str] = []

    def before_list_element(self, element: NestedField) -> None:
        """Short field names omit element when the element is a StructType."""
        self._field_names.append(element.name)

    def after_list_element(self, element: NestedField) -> None:
        self._field_names.pop()

    def before_field(self, field: NestedField) -> None:
        """Store the field name."""
        self._field_names.append(field.name)

    def after_field(self, field: NestedField) -> None:
        """Remove the last field name stored."""
        self._field_names.pop()

    def schema(self, schema: Schema, struct_result: Dict[str, int]) -> Dict[str, int]:
        return self._index

    def struct(self, _struct: StructType, field_results: List[Dict[str, int]]) -> Dict[str, int]:
        return self._index

    def field(self, field: NestedField, field_result: Dict[str, int]) -> Dict[str, int]:
        """Add the field name to the index."""
        if isinstance(field.field_type, PrimitiveType):
            self._add_field(field.name, field.field_id)
        return self._index

    def list(self, list_type: ListType, element_result: Dict[str, int]) -> Dict[str, int]:
        """Add the list element name to the index."""
        self._add_field("list.element", list_type.element_field.field_id)
        return self._index

    def map(self, map_type: MapType, key_result: Dict[str, int], value_result: Dict[str, int]) -> Dict[str, int]:
        """Add the key name and value name as individual items in the index."""
        self._add_field("key_value.key", map_type.key_field.field_id)
        self._add_field("key_value.value", map_type.value_field.field_id)
        return self._index

    def primitive(self, primitive: PrimitiveType) -> Dict[str, int]:
        return self._index

    def _add_field(self, name: str, field_id: int) -> None:
        """Add a field name to the index, mapping its full name to its field ID.

        Args:
            name (str): The field name.
            field_id (int): The field ID.

        Raises:
            ValueError: If the field name is already contained in the index.
        """
        full_name = name

        if self._field_names:
            full_name = ".".join([".".join(self._field_names), name])

        if full_name in self._index:
            raise ValueError(f"Invalid schema, multiple fields for name {full_name}: {self._index[full_name]} and {field_id}")
        self._index[full_name] = field_id

    def by_path(self) -> Dict[str, int]:
        return self._index


def parquet_schema_to_ids(schema_or_type: Union[Schema, IcebergType]) -> Dict[str, int]:
    """Generate an index of field parquet paths to field IDs.

    It can be used to compute the mapping pass to `fill_parquet_file_metadata`.

    Args:
        schema_or_type (Union[Schema, IcebergType]): A schema or type to index.

    Returns:
        Dict[str, int]: An index of field names to field IDs.
    """
    if len(schema_or_type.fields) > 0:
        indexer = _IndexByParquetPath()
        visit(schema_or_type, indexer)
        return indexer.by_path()
    else:
        return {}

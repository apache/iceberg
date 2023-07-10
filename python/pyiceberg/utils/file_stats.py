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

import logging
import re
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import pyarrow.lib
import pyarrow.parquet as pq

from pyiceberg.avro import (
    STRUCT_BOOL,
    STRUCT_DOUBLE,
    STRUCT_FLOAT,
    STRUCT_INT32,
    STRUCT_INT64,
)
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema, SchemaVisitor, visit
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)

BOUND_TRUNCATED_LENGHT = 16
TRUNCATION_EXPR = r"^truncate\((\d+)\)$"

logger = logging.getLogger(__name__)

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
    return STRUCT_BOOL.pack(value)


def int32_to_avro(value: int) -> bytes:
    return STRUCT_INT32.pack(value)


def int64_to_avro(value: int) -> bytes:
    return STRUCT_INT64.pack(value)


def float_to_avro(value: float) -> bytes:
    return STRUCT_FLOAT.pack(value)


def double_to_avro(value: float) -> bytes:
    return STRUCT_DOUBLE.pack(value)


def bytes_to_avro(value: Union[bytes, str]) -> bytes:
    if type(value) == str:
        return value.encode()
    else:
        assert isinstance(value, bytes)  # appeases mypy
        return value


class StatsAggregator:
    def __init__(self, type_string: str, trunc_length: Optional[int] = None):
        self.current_min: Any = None
        self.current_max: Any = None
        self.serialize: Any = None
        self.trunc_lenght = trunc_length

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
        return self.serialize(self.current_min)[: self.trunc_lenght]

    def get_max(self) -> bytes:
        return self.serialize(self.current_max)[: self.trunc_lenght]


class MetricsMode(Enum):
    NONE = 0
    COUNTS = 1
    TRUNC = 2
    FULL = 3


def match_metrics_mode(mode: str) -> Tuple[MetricsMode, Optional[int]]:
    m = re.match(TRUNCATION_EXPR, mode)

    if m:
        return MetricsMode.TRUNC, int(m[1])
    elif mode == "none":
        return MetricsMode.NONE, None
    elif mode == "counts":
        return MetricsMode.COUNTS, None
    elif mode == "full":
        return MetricsMode.FULL, None
    else:
        raise AssertionError(f"Unsupported metrics mode {mode}")


def fill_parquet_file_metadata(
    df: DataFile,
    metadata: pq.FileMetaData,
    col_path_2_iceberg_id: Dict[str, int],
    file_size: int,
    table_metadata: Optional[TableMetadata] = None,
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

    col_names = {p.split(".")[0] for p in col_path_2_iceberg_id.keys()}

    metrics_modes = {n: MetricsMode.TRUNC for n in col_names}
    trunc_lengths: Dict[str, Optional[int]] = {n: BOUND_TRUNCATED_LENGHT for n in col_names}

    if table_metadata:
        default_mode = table_metadata.properties.get("write.metadata.metrics.default")

        if default_mode:
            m, t = match_metrics_mode(default_mode)

            metrics_modes = {n: m for n in col_names}
            trunc_lengths = {n: t for n in col_names}

        for col_name in col_names:
            col_mode = table_metadata.properties.get(f"write.metadata.metrics.column.{col_name}")

            if col_mode:
                metrics_modes[col_name], trunc_lengths[col_name] = match_metrics_mode(col_mode)

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

            top_level_col_name = column.path_in_schema.split(".")[0]

            metrics_mode = metrics_modes[top_level_col_name]

            if metrics_mode == MetricsMode.NONE:
                continue

            value_counts[col_id] = value_counts.get(col_id, 0) + column.num_values

            if column.is_stats_set:
                try:
                    statistics = column.statistics

                    null_value_counts[col_id] = null_value_counts.get(col_id, 0) + statistics.null_count

                    if metrics_mode == MetricsMode.COUNTS:
                        continue

                    if column.path_in_schema in col_names:
                        # Iceberg seems to only have statistics for scalar columns

                        if col_id not in col_aggs:
                            trunc_length = trunc_lengths[top_level_col_name]
                            col_aggs[col_id] = StatsAggregator(statistics.physical_type, trunc_length)

                        col_aggs[col_id].add_min(statistics.min)
                        col_aggs[col_id].add_max(statistics.max)

                except pyarrow.lib.ArrowNotImplementedError as e:
                    logger.warning(e)

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

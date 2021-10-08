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


from datetime import datetime
import decimal
import logging
import typing

from iceberg.api import Schema
from iceberg.api.expressions import Expression
from iceberg.api.io import InputFile
from iceberg.api.types import NestedField, Type, TypeID
from iceberg.core.filesystem import FileSystem, LocalFileSystem, S3FileSystem
from iceberg.core.util.profile import profile
from iceberg.exceptions import FileSystemNotFound, InvalidCastException
import numpy as np
import pyarrow as pa
from pyarrow import fs
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .dataset_utils import get_dataset_filter
from .parquet_schema_utils import prune_columns
from .parquet_to_iceberg import convert_parquet_to_iceberg

_logger = logging.getLogger(__name__)

DTYPE_MAP: typing.Dict[TypeID,
                       typing.Callable[[NestedField], typing.Tuple[pa.Field, typing.Any]]] = \
    {TypeID.BINARY: lambda field: pa.binary(),
     TypeID.BOOLEAN: lambda field: (pa.bool_(), False),
     TypeID.DATE: lambda field: (pa.date32(), datetime.now()),
     TypeID.DECIMAL: lambda field: (pa.decimal128(field.type.precision, field.type.scale),
                                    decimal.Decimal()),
     TypeID.DOUBLE: lambda field: (pa.float64(), np.nan),
     TypeID.FIXED: lambda field: pa.binary(field.length),
     TypeID.FLOAT: lambda field: (pa.float32(), np.nan),
     TypeID.INTEGER: lambda field: (pa.int32(), np.nan),
     TypeID.LIST: lambda field: (pa.list_(pa.field("element",
                                                   DTYPE_MAP[field.type.element_type.type_id](field.type)[0])),
                                 None),
     TypeID.LONG: lambda field: (pa.int64(), np.nan),
     # To-Do: update to support reading map fields
     # TypeID.MAP: lambda field: (,),
     TypeID.STRING: lambda field: (pa.string(), ""),
     TypeID.STRUCT: lambda field: (pa.struct([(nested_field.name,
                                               DTYPE_MAP[nested_field.type.type_id](nested_field.type)[0])
                                              for nested_field in field.type.fields]), {}),
     TypeID.TIMESTAMP: lambda field: (pa.timestamp("us"), datetime.now()),
     # not used in SPARK, so not implementing for now
     # TypeID.TIME: pa.time64(None)
     }

FS_MAP: typing.Dict[typing.Type[FileSystem], typing.Type[fs.FileSystem]] = {LocalFileSystem: fs.LocalFileSystem}

try:
    FS_MAP[S3FileSystem] = fs.S3FileSystem

except ImportError:
    _logger.warning("Mapped filesystem not available")


class ParquetReader(object):

    def __init__(self, input: InputFile, expected_schema: Schema, options, filter_expr: Expression,
                 case_sensitive: bool, start: int = None, end: int = None):
        self._stats: typing.Dict[str, int] = dict()

        self._input = input
        self._input_fo = input.new_fo()

        self._arrow_file = pq.ParquetFile(self._input_fo)
        self._file_schema = convert_parquet_to_iceberg(self._arrow_file)
        self._expected_schema = expected_schema
        self._file_to_expected_name_map = ParquetReader.get_field_map(self._file_schema,
                                                                      self._expected_schema)
        self._options = options
        self._filter = get_dataset_filter(filter_expr, ParquetReader.get_reverse_field_map(self._file_schema,
                                                                                           self._expected_schema))

        self._case_sensitive = case_sensitive
        if start is not None or end is not None:
            raise NotImplementedError("Partial file reads are not yet supported")
            # self.start = start
            # self.end = end

        self.materialized_table = False
        self._table = None

        _logger.debug("Reader initialized for %s" % self._input.path)

    @property
    def stats(self) -> typing.Dict[str, int]:
        return dict(self._stats)

    def read(self, force=False) -> pa.Table:
        if not self.materialized_table or force:
            self._read_data()

        return self._table

    def _read_data(self) -> None:
        _logger.debug("Starting data read")

        # only scan the columns projected and in our file
        cols_to_read = prune_columns(self._file_schema, self._expected_schema)

        with profile("read data", self._stats):
            try:
                read_fs = FS_MAP[type(self._input.fs)]
            except KeyError:
                raise FileSystemNotFound(f"No mapped filesystem found for {type(self._input.fs)}")

            arrow_dataset = ds.FileSystemDataset.from_paths([self._input.location()],
                                                            schema=self._arrow_file.schema_arrow,
                                                            format=ds.ParquetFileFormat(),
                                                            filesystem=read_fs())

            arrow_table = arrow_dataset.to_table(columns=cols_to_read, filter=self._filter)

        # process schema evolution if needed
        with profile("schema_evol_proc", self._stats):
            processed_tbl = self.migrate_schema(arrow_table)
            for i, field in self.get_missing_fields():
                dtype_func = DTYPE_MAP.get(field.type.type_id)
                if dtype_func is None:
                    raise RuntimeError("Unable to create null column for type %s" % field.type.type_id)

                dtype = dtype_func(field)
                processed_tbl = (processed_tbl.add_column(i,
                                                          pa.field(field.name, dtype[0], True, None),
                                                          ParquetReader.create_null_column(processed_tbl[0],
                                                                                           dtype)))
        self._table = processed_tbl
        self.materialized_table = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _logger.debug(self._stats)
        self.close()

    def close(self):
        self._input_fo.close()

    def get_missing_fields(self) -> typing.List[typing.Tuple[int, NestedField]]:
        return [(i, field) for i, field in enumerate(self._expected_schema.as_struct().fields)
                if self._file_schema.find_field(field.id) is None]

    @staticmethod
    def get_field_map(file_schema, expected_schema) -> typing.Dict[str, str]:
        return {file_schema.find_field(field.id).name: field.name
                for field in expected_schema.as_struct().fields
                if file_schema.find_field(field.id) is not None}

    @staticmethod
    def get_reverse_field_map(file_schema, expected_schema) -> typing.Dict[str, str]:
        return {expected_schema.find_field(field.id).name: field.name
                for field in file_schema.as_struct().fields
                if expected_schema.find_field(field.id) is not None}

    def migrate_schema(self, table: pa.Table) -> pa.Table:
        data_arrays: typing.List[pa.ChunkedArray] = []
        schema: typing.List[pa.Field] = []
        for key, value in self._file_to_expected_name_map.items():
            column_idx: int = table.schema.get_field_index(key)
            column_field: pa.Field = table.schema[column_idx]
            column_arrow_type: pa.DataType = column_field.type
            column_data: pa.ChunkedArray = table[column_idx]

            iceberg_field: NestedField = self._expected_schema.find_field(value)
            converted_field: NestedField = self._file_schema.find_field(key)

            if iceberg_field.type != converted_field.type:
                if not ParquetReader.is_supported_cast(converted_field.type, iceberg_field.type):
                    _logger.error(f"unsupported cast {converted_field.type} -> {iceberg_field.type}")
                    raise InvalidCastException("")
                try:
                    column_arrow_type = DTYPE_MAP[iceberg_field.type.type_id](iceberg_field)[0]
                    column_data = column_data.cast(column_arrow_type)
                except KeyError:
                    _logger.error(f"Unable to map {iceberg_field.type} to an arrow type")
                    raise

            data_arrays.append(column_data)
            schema.append(pa.field(value, column_arrow_type, column_field.nullable, column_field.metadata))

        return pa.table(data_arrays, schema=pa.schema(schema))

    @staticmethod
    def create_null_column(reference_column: pa.ChunkedArray, dtype_tuple: typing.Tuple[pa.DataType, typing.Any]) -> pa.ChunkedArray:
        dtype, init_val = dtype_tuple
        return pa.chunked_array([pa.array(np.full(len(c), init_val),
                                          type=dtype,
                                          mask=np.array([True] * len(reference_column.chunks[0]), dtype="bool"))
                                 for c in reference_column.chunks], type=dtype)

    @staticmethod
    def is_supported_cast(old_type: Type, new_type: Type) -> bool:
        if old_type.type_id == TypeID.INTEGER and new_type.type_id == TypeID.LONG:
            return True
        elif old_type.type_id == TypeID.FLOAT and new_type.type_id == TypeID.DOUBLE:
            return True
        elif old_type.type_id == TypeID.DECIMAL and new_type.type_id == TypeID.DECIMAL \
                and old_type.precision < new_type.precision \
                and old_type.scale == new_type.scale:
            return True
        return False

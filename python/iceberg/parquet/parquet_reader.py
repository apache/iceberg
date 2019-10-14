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

from collections import OrderedDict
from datetime import datetime
import decimal
import logging

import fastparquet
from iceberg.api.expressions import Expressions
from iceberg.api.types import TypeID
from iceberg.core.util import SCAN_THREAD_POOL_ENABLED
import numpy as np
import pyarrow as pa
import pyarrow.lib as lib
import pyarrow.parquet as pq

from .arrow_table_utils import apply_iceberg_filter
from .parquet_rowgroup_evaluator import ParquetRowgroupEvaluator
from .parquet_schema_utils import prune_columns
from .parquet_to_iceberg import convert_parquet_to_iceberg

_logger = logging.getLogger(__name__)


USE_RG_FILTERING_PROP = "use-row-group-filtering"

DTYPE_MAP = {TypeID.BINARY: lambda field: pa.binary(),
             TypeID.BOOLEAN: lambda field: (pa.bool_(), False),
             TypeID.DATE: lambda field: (pa.date32(), datetime.now()),
             TypeID.DECIMAL: lambda field: (pa.decimal128(field.type.precision, field.type.scale),
                                            decimal.Decimal()),
             TypeID.DOUBLE: lambda field: (pa.float64(), np.nan),
             TypeID.FIXED: lambda field: pa.binary(field.length()),
             TypeID.FLOAT: lambda field: (pa.float32(), np.nan),
             TypeID.INTEGER: lambda field: (pa.int32(), np.nan),
             TypeID.LIST: lambda field: (pa.list_(pa.field("element",
                                                           DTYPE_MAP.get(field.type.element_type.type_id)(field.type)[0])),
                                         None),
             TypeID.LONG: lambda field: (pa.int64(), np.nan),
             # pyarrow doesn't currently support reading parquet map types
             # TypeID.MAP: lambda field: (,),
             TypeID.STRING: lambda field: (pa.string(), ""),
             TypeID.STRUCT: lambda field: (pa.struct([(nested_field.name,
                                                       DTYPE_MAP.get(nested_field.type.type_id)(nested_field.type)[0])
                                                     for nested_field in field.type.fields]), {}),
             TypeID.TIMESTAMP: lambda field: (pa.timestamp("us"), datetime.now()),
             # Not implemented yet, since Spark doesn't support time type
             # TypeID.TIME: pa.time64(None)
             }


class ParquetReader(object):
    """
    Handles Reading a parquet file for a given start to end range.  Also, enforces
    the projected iceberg schema on the read data.  This may involve selectively reading
    columns, remapping column names, re-arranging the projection ordering, and possibly
    adding null columns

    Parameters
    ----------
    input : iceberg.api.io.InputFile
        An iceberg InputFile that is pointing to the parquet file that is to be read.
    expected_schema : iceberg.api.Schema
        An iceberg schema with the schema to project in the output
    options : dict
        A dict of options to pass to the reader
    filter : iceberg.api.expressions.Expression
        A row-level filtering expression to apply to the parquet row groups
        and to the final output table
    start : int, default None
        The start byte range for this reader.This is passed to the ParquetRowGroupEvaluator
    end : int, default None
        The end byte range for this reader. This is passed to the ParquetRowGroupEvaluator
    """
    def __init__(self, input, expected_schema, options, filter,
                 case_sensitive, start=None, end=None):
        self._input = input
        self._input_fo = input.new_fo()

        # TODO: Remove usage of fastparquet
        self._pq_file = fastparquet.ParquetFile(self._input_fo)
        self._arrow_file = pq.ParquetFile(self._input_fo)
        self._file_schema = convert_parquet_to_iceberg(self._pq_file.schema)
        self._expected_schema = expected_schema
        self._file_to_expected_name_map = ParquetReader.get_field_map(self._file_schema,
                                                                      self._expected_schema)

        self._options = options
        self.use_threads = self._options.get(SCAN_THREAD_POOL_ENABLED, False)

        _logger.debug("Starting Parquet Reader with %s" %
                      "threaded scan enabled" if self.use_threads else "threaded scan disabled")
        _logger.debug("Reading %s" % self._input.path)

        self._filter = filter if filter != Expressions.always_true() else None
        self._case_sensitive = case_sensitive
        self.start = start
        self.end = end

        self.materialized_table = False
        self.curr_iterator = None
        self._table = None

    def to_pandas(self):
        """
        Return a pandas dataframe for the given file and filter

        Returns
        -------
        pandas.core.frame.DataFrame
            A pandas dataframe with the data from the file, projected to the specified schema and
            with the filter expression applied

        """
        if not self.materialized_table:
            self._read_data()

        return self._table.to_pandas(use_threads=self.use_threads)

    def to_arrow_table(self):
        """
        Return a pyarrow Table given file and filter

        Returns
        -------
        pyarrow.Table
            A pyarrow Table with the data from the file, projected to the specified schema and
            with the filter expression applied
        """
        if not self.materialized_table:
            self._read_data()

        return self._table

    def _read_data(self):
        """
        Private function to perform the read and schema evolution

        """
        # only scan the columns projected and in our file
        cols_to_read = prune_columns(self._file_schema, self._expected_schema)

        if not self._options.get(USE_RG_FILTERING_PROP, True):
            _logger.debug("Reading Full file")
            arrow_tbls = [pq.ParquetFile(self._input.new_fo()).read(columns=cols_to_read,
                                                                    use_threads=self.use_threads)]
        else:
            _logger.debug("Using row group filtering")
            # filter out row groups that cannot contain our filter expressions
            rg_evaluator = ParquetRowgroupEvaluator(self._file_schema, self._filter,
                                                    self.start, self.end)

            read_row_groups = [i for i, row_group in enumerate(self._pq_file.row_groups)
                               if rg_evaluator.eval(row_group)]

            if len(read_row_groups) == 0:
                self._table = None
                self.materialized_table = True
                return

            arrow_tbls = [self.read_rgs(i, cols_to_read) for i in read_row_groups]

        _logger.debug("data fetch done")
        processed_tbl = ParquetReader.migrate_schema(self._file_to_expected_name_map,
                                                     lib.concat_tables(arrow_tbls))
        for i, field in self.get_missing_fields():
            dtype_func = DTYPE_MAP.get(field.type.type_id)
            if dtype_func is None:
                raise RuntimeError("Unable to create null column for type %s" % field.type.type_id)

            processed_tbl = (processed_tbl
                             .add_column(i, ParquetReader.create_null_column(processed_tbl[0],
                                                                             field.name,
                                                                             dtype_func(field))))
        else:
            processed_tbl = lib.concat_tables(arrow_tbls)

        self._table = processed_tbl
        self.materialized_table = True

    def read_rgs(self, i, cols_to_read):
        """
        Handles Reading a single row group and performing the row-level filtering.

        Parameters
        ----------
        i : int
            The row group number to read
        cols_to_read : list
            A list of columns to read, must be using the names from the parquet schema

        Returns
        -------
        pyarrow.Table
            A pyarrow Table with the data from the row group with the filter expression applied
        """
        with self._input.new_fo() as thread_file:
            arrow_tbl = (pq.ParquetFile(thread_file)
                         .read_row_group(i, columns=cols_to_read))
        filtered_table = apply_iceberg_filter(arrow_tbl,
                                              ParquetReader.get_reverse_field_map(self._file_schema,
                                                                                  self._expected_schema),
                                              self._filter)

        return filtered_table

    def close(self):
        """
        Function to close out the file-like object that was opened at object creation

        """
        self._input_fo.close()

    def get_missing_fields(self):
        """
        Get a list of the fields in the expected schema that don't exist in the file schema

        Returns
        -------
        pyarrow.Table
            A pyarrow Table with the data from the row group with the filter expression applied
        """
        return [(i, field) for i, field in enumerate(self._expected_schema.as_struct().fields)
                if self._file_schema.find_field(field.id) is None]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def get_field_map(file_schema, expected_schema):
        """
        Get a map that maps the field name from the file schema to the expected schema field name
        based on their id's

        Parameters
        ----------
        file_schema : iceberg.api.Schema
            The iceberg schema of the parquet file
        expected_schema : iceberg.api.Schema
            The iceberg schema of the expected output

        Returns
        -------
        collections.OrderedDict
            map that maps the file field name to the expected field name

        """
        return OrderedDict({file_schema.find_field(field.id).name: field.name
                            for field in expected_schema.as_struct().fields
                            if file_schema.find_field(field.id) is not None})

    @staticmethod
    def get_reverse_field_map(file_schema, expected_schema):
        """
        Get a map that maps the field name from the expected schema to the file schema field name
        based on their id's

        Parameters
        ----------
        file_schema : iceberg.api.Schema
            The iceberg schema of the parquet file
        expected_schema : iceberg.api.Schema
            The iceberg schema of the expected output

        Returns
        -------
        map
            map that maps the expected field name to the file field name
        """
        return {expected_schema.find_field(field.id).name: field.name
                for field in file_schema.as_struct().fields
                if expected_schema.find_field(field.id) is not None}

    @staticmethod
    def migrate_schema(mapping, table):
        """
        Re-order the columns in the pyarrow.Table to the expected schema output ordering

        Parameters
        ----------
        mapping : collections.OrderedDict
            An OrderedDict that has the desired ordering and mapping of columns
        table : pyarrow.Table
            The pyarrow.Table to be rearranged

        Returns
        -------
        pyarrow.Table
            A pyarrow.Table rearranged to match the naming and ordering is specified by mapping

        """
        new_columns = []
        for key, value in mapping.items():
            column_idx = table.schema.get_field_index(key)
            column = table[column_idx]
            new_columns.append(pa.Column.from_array(value, column.data))

        return pa.Table.from_arrays(new_columns)

    @staticmethod
    def create_null_column(reference_column, name, dtype_tuple):
        """
        Create a filled column of Null values

        Parameters
        ----------
        reference_column : pyarrow.Column
            A column to use as the reference for the chunked arrays
        name : str
            The column_name
        dtype_tuple : tuple
            A tuple that contains the pyarrow datatype and the init value to use for the array
        Returns
        -------
        pyarrow.Table
            A pyarrow.Table rearranged to match the naming and ordering is specified by mapping

        """
        dtype, init_val = dtype_tuple
        chunk = pa.chunked_array([pa.array(np.full(len(c), init_val), type=dtype, mask=[True] * len(c))
                                  for c in reference_column.data.chunks], type=dtype)

        return pa.Column.from_array(name, chunk)

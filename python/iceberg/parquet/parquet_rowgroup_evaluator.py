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

# inspiration drawn from the predicate accepts function in Kartothek
# https://github.com/JDASoftwareGroup/kartothek/blob/master/kartothek/serialization/_parquet.py#L406

from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from iceberg.api import Schema
from iceberg.api.expressions import (Binder,
                                     BoundPredicate,
                                     BoundReference,
                                     Expressions,
                                     ExpressionVisitors,
                                     UnboundPredicate)
from iceberg.api.expressions.literals import BaseLiteral
from iceberg.api.types import (DecimalType, StructType, Type, TypeID)
from pyarrow._parquet import ColumnChunkMetaData, RowGroupMetaData

MICROSECOND_CONVERSION = 1000000


class ParquetRowgroupEvalVisitor(ExpressionVisitors.BoundExpressionVisitor):
    ROWS_MIGHT_MATCH = True
    ROWS_CANNOT_MATCH = False

    def __init__(self, expr: BoundPredicate, schema: Schema, field_name_map: dict, struct: StructType,
                 start: int, end: int) -> None:
        self.expr = expr
        self.schema = schema
        self.field_name_map = field_name_map
        self.struct = struct
        self.start = start
        self.end = end

        # row-group stats info
        self.lower_bounds: Dict[int, Union[Decimal, int, float, str]] = dict()
        self.upper_bounds: Dict[int, Union[Decimal, int, float, str]] = dict()
        self.nulls: Dict[int, int] = dict()
        self.num_rows: int = 0
        self.midpoint: int = -1
        self.parquet_cols: List[int] = []

    def eval(self, row_group: RowGroupMetaData) -> bool:
        """
        Returns a boolean that determines if the given row-group may contain rows
        for the assigned predicate and read-range[start, end]

        Parameters
        ----------
        row_group : pyarrow._parquet.RowGroupMetaData
            The pyarrow parquet row-group metadata being evaluated
        Returns
        -------
        bool
            True if rows for the current evaluator might exist in the row group, false otherwise
        """
        self.num_rows = row_group.num_rows
        if self.num_rows is None or self.num_rows <= 0:
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        self.get_stats(row_group)

        # if the mid-point of the row-group is not contained by the
        # start-end range we don't read it
        if self.start is not None and self.end is not None \
                and not (self.start <= self.midpoint <= self.end):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        if self.expr is None:
            return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

        return ExpressionVisitors.visit(self.expr, self)

    def always_true(self) -> bool:
        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def always_false(self) -> bool:
        return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

    def not_(self, result: bool) -> bool:
        return not result

    def and_(self, left_result: bool, right_result: bool) -> bool:
        return left_result and right_result

    def or_(self, left_result: bool, right_result: bool) -> bool:
        return left_result or right_result

    def is_null(self, ref: BoundReference) -> bool:
        field_id = ref.field_id

        if not self.is_col_in_row_group(field_id) or self.nulls.get(field_id) is None or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

    def not_null(self, ref: BoundReference) -> bool:
        field_id = ref.field_id

        if not self.is_col_in_row_group(field_id) or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def lt(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        field_id = ref.field_id

        if not self.is_col_in_row_group(field_id) or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=field_id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(field_id))

        if self.lower_bounds is not None and field_id in self.lower_bounds:
            if self.lower_bounds.get(field_id) >= lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def lt_eq(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        field_id = ref.field_id

        if not self.is_col_in_row_group(field_id) or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=field_id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(field_id))

        if self.lower_bounds is not None and field_id in self.lower_bounds:
            if self.lower_bounds.get(field_id) > lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def gt(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        field_id = ref.field_id
        if not self.is_col_in_row_group(field_id) or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=field_id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(field_id))

        if self.upper_bounds is not None and field_id in self.upper_bounds:
            if self.upper_bounds.get(field_id) <= lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def gt_eq(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        field_id = ref.field_id
        if not self.is_col_in_row_group(field_id) or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=field_id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(field_id))

        if self.upper_bounds is not None and field_id in self.upper_bounds:
            if self.upper_bounds.get(field_id) < lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def eq(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        field_id = ref.field_id
        if not self.is_col_in_row_group(field_id) or self.is_col_all_null(field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        field = self.struct.field(id=field_id)

        if field is None:
            raise RuntimeError("Cannot filter by nested column: %s" % self.schema.find_field(field_id))

        if self.lower_bounds is not None and field_id in self.lower_bounds:
            if self.lower_bounds.get(field_id) > lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        if self.upper_bounds is not None and field_id in self.upper_bounds:
            if self.upper_bounds.get(field_id) < lit.value:
                return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return ParquetRowgroupEvalVisitor.ROWS_MIGHT_MATCH

    def not_eq(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        if not self.is_col_in_row_group(ref.field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return not self.is_col_all_null(ref.field_id)

    def in_(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        if not self.is_col_in_row_group(ref.field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return not self.is_col_all_null(ref.field_id)

    def not_in(self, ref: BoundReference, lit: BaseLiteral) -> bool:
        if not self.is_col_in_row_group(ref.field_id):
            return ParquetRowgroupEvalVisitor.ROWS_CANNOT_MATCH

        return not self.is_col_all_null(ref.field_id)

    def is_col_in_row_group(self, field_id: int) -> bool:
        """
        Check if the column exists in the row group

        Parameters
        ----------
        field_id : int
            The field_id in the iceberg schema for the column being evaluated
        Returns
        -------
        bool
            Returns true if the column is found in the row group metadata
        """

        return field_id in self.parquet_cols

    def is_col_all_null(self, field_id: int) -> bool:
        """
        Check if all values for the column will be null.

        Parameters
        ----------
        field_id : int
            The field_id in the iceberg schema for the column being evaluated
        Returns
        -------
        bool
            Returns true if the column is all null values otherwise returns false
        """
        return self.nulls.get(field_id) == self.num_rows

    def get_stats(self, row_group: RowGroupMetaData) -> None:
        """
        Summarizes the row group statistics for upper and lower bounds.  Also calculates
        the mid-point of the row-group for determining start <= midpoint <= end

        Parameters
        ----------
        row_group : pyarrow._parquet.ColumnChunkMetaData
            The pyarrow parquet row-group metadata being evaluated
        """

        self.lower_bounds = {}
        self.upper_bounds = {}
        self.nulls = {}
        self.parquet_cols = []

        start = -1
        size = 0

        for i in range(row_group.num_columns):
            column = row_group.column(i)
            if start < 0:
                start = column.file_offset
            size += column.total_compressed_size

            id = self.schema.lazy_name_to_id().get(self.field_name_map.get(column.path_in_schema))
            iceberg_type = self.schema.columns()[id - 1].type

            if id is not None:
                self.parquet_cols.append(id)
                if column.statistics is not None:
                    if self.set_min_max(id, iceberg_type, column):
                        self.nulls[id] = column.statistics.null_count

        self.midpoint = int(size / 2 + start)

    def set_min_max(self, field_id: int, iceberg_type: Type, column: ColumnChunkMetaData) -> bool:
        """
        If the type is supported coerce the min and max values into a type that can be compared in the
        operator functions. If the type is not supported, or is otherwise not accurate for usage
        return without setting any stats

        Parameters
        ----------
        field_id : int
            The field_id in the expected iceberg schema
        iceberg_type: Type
            The Iceberg type value for this field which is used to get the TypeID to determine support and
            may be used to get additional info needed to interpret the stat
        column: ColumnChunkMetaData
            The parquet metadata for the column that corresponds to the column in the iceberg schema with id
            equal to field_id
        Returns
        -------
        bool
            Returns True if the min/max was set
        """
        type_id: Any = iceberg_type.type_id
        if not ParquetRowgroupEvalVisitor.is_supported_type(type_id):
            return False
        min_value = column.statistics.min
        max_value = column.statistics.max

        # checking for int overflow, stats are stored as signed int
        if type_id in (TypeID.INTEGER, TypeID.LONG) and max_value < min_value:
            return False
        elif type_id == TypeID.TIMESTAMP:
            min_value = int(min_value.timestamp() * MICROSECOND_CONVERSION)
            max_value = int(max_value.timestamp() * MICROSECOND_CONVERSION)
        elif type_id == TypeID.DECIMAL and isinstance(iceberg_type, DecimalType):
            min_value = ParquetRowgroupEvalVisitor.get_decimal_stat_value(min_value, iceberg_type)
            max_value = ParquetRowgroupEvalVisitor.get_decimal_stat_value(max_value, iceberg_type)
        elif type_id == TypeID.FLOAT:
            # TO-DO: Add epsilon float evaluation from Kartothek
            # https://github.com/JDASoftwareGroup/kartothek/blob/master/kartothek/serialization/_parquet.py#L443
            return False

        self.lower_bounds[field_id] = min_value
        self.upper_bounds[field_id] = max_value
        return True

    @staticmethod
    def is_supported_type(type_id: TypeID) -> bool:
        """
        Checks if the type is supported for RowGroup filtering

        Parameters
        ----------
        type_id : iceberg.api.types.TypeID
            The iceberg typeID for the column being checked
        Returns
        -------
        bool
            Returns True if the type is supported for RowGroup filtering, False otherwise
        """
        return type_id in (TypeID.DATE,
                           TypeID.DECIMAL,
                           TypeID.FLOAT,
                           TypeID.INTEGER,
                           TypeID.LONG,
                           TypeID.STRING,
                           TypeID.TIMESTAMP)

    @staticmethod
    def get_decimal_stat_value(value: Any, iceberg_type: DecimalType) -> Decimal:
        """
        returns

        Parameters
        ----------
        value : bytes
            The value from the parquet stats, this will be an int if the precision is less than or equal to 18
            and will be a bytes object if it is greater than 18
        iceberg_type:
            The DecimalType object for that column, this allows the function to get the proper precision and scale to
            interpret the stat
        Returns
        -------
        Decimal
            Returns a Decimal value with the proper scaling applied to the unscaled value from the stats
        """
        if iceberg_type.precision <= 18:
            unscaled_val = value
        else:
            unscaled_val = int.from_bytes(value, byteorder="big", signed=True)

        return Decimal((0 if unscaled_val >= 0 else 1, [int(d) for d in str(abs(unscaled_val))], -iceberg_type.scale))


class ParquetRowgroupEvaluator(object):
    """
    Evaluator for determining if a pyarrow parquet row-group matches the given expression and index bounds.

    Parameters
    ----------
    schema : iceberg.api.Schema
        An iceberg schema to use for binding the predicate
    field_name_map: map
        A map that translates file column names to the current schema
    unbound : iceberg.api.expressions.UnboundPredicate
        The unbound predicate to evaluate
    start : int
        The start index of the assigned reader
    end : int
        The end index of the assigned reader
    """
    def __init__(self, schema: Schema, field_name_map: dict, unbound: UnboundPredicate, start: int, end: int) -> None:
        self.schema = schema
        self.struct = schema.as_struct()
        self.field_name_map = field_name_map

        self.expr = None if unbound is None else Binder.bind(self.struct, Expressions.rewrite_not(unbound))
        self.start = start
        self.end = end
        self._visitors: Optional[ParquetRowgroupEvalVisitor] = None

    def _visitor(self) -> ParquetRowgroupEvalVisitor:
        if self._visitors is None:
            self._visitors = ParquetRowgroupEvalVisitor(self.expr, self.schema, self.field_name_map,
                                                        self.struct, self.start, self.end)

        return self._visitors

    def eval(self, row_group: RowGroupMetaData) -> bool:
        return self._visitor().eval(row_group)

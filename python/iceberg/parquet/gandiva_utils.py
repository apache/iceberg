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

from iceberg.api.expressions import Operation, Predicate
import numpy as np
import pyarrow as pa
import pyarrow.gandiva as gandiva


builder = gandiva.TreeExprBuilder()
LIT_TYPE_MAP = {bytes: pa.binary(),
                bool: pa.bool_(),
                int: pa.int32(),
                # decimal.Decimal: pa.decimal128(field.type.precision, field.type.scale),
                float: pa.float64(),
                str: pa.string()}


def apply_iceberg_filter(table, expected_to_file_map, expr):
    """
         (Experimental) Not currently used anywhere.  This will build a selection vector
         for the given filter using pyarrow.gandiva

         Parameters
         ----------
         table: pyarrow.Table
             The table to apply the filter on
         expected_to_file_map: map
             A mapping of the current schema field names to the file field names
         expr: iceberg.api.expressions.Expression
             A predicate expression to use as a filter

         Returns
         -------
         pyarrow.Table
             A pyarrow Table with the rows that don't match the predicate filtered out

    """
    if expr is None:
        return table
    final_condition = builder.make_condition(get_gandiva_filter(table, expected_to_file_map, expr))
    final_filter = gandiva.make_filter(table.schema, final_condition)
    curr_idx = 0
    selection_vector = np.empty(0, dtype=np.uint32)
    for batch in table.to_batches():
        tmp_selection_vector = final_filter.evaluate(batch, pa.default_memory_pool()).to_array().to_numpy()
        selection_vector = np.append(selection_vector, [tmp_selection_vector + curr_idx])
        curr_idx += len(batch)

    indicator = np.zeros(len(table), np.bool_)
    indicator[selection_vector] = True
    filt_table = pa.Table.from_pandas(table.to_pandas()[indicator], schema=table.schema)
    return filt_table.remove_column(filt_table.num_columns - 1)


def get_gandiva_filter(table, field_map, expr):

    if isinstance(expr, Predicate):
        return predicate(table, field_map, expr)

    if expr.op() == Operation.TRUE:
        return True
    elif expr.op() == Operation.FALSE:
        return False
    elif expr.op() == Operation.NOT:
        return not_(get_gandiva_filter(table, field_map, expr.child))
    elif expr.op() == Operation.AND:
        return and_(get_gandiva_filter(table, field_map, expr.left),
                    get_gandiva_filter(table, field_map, expr.right))
    elif expr.op() == Operation.OR:
        return or_(get_gandiva_filter(table, field_map, expr.left),
                   get_gandiva_filter(table, field_map, expr.right))
    else:
        raise RuntimeError("Unknown operation: {}".format(expr.op()))


def predicate(table, field_map, pred):  # noqa: ignore=C901
    col_name = field_map.get(pred.ref.name)
    if col_name is None:
        if pred.op == Operation.IS_NULL:
            return np.full(table.num_rows, fill_value=True, dtype=np.bool)

        return np.full(table.num_rows, fill_value=False, dtype=np.bool)

    if pred.op == Operation.IS_NULL:
        raise NotImplementedError()
    elif pred.op == Operation.NOT_NULL:
        raise NotImplementedError()

    pred_node = builder.make_field(table.schema.field_by_name(col_name))
    lit_arrow_type = LIT_TYPE_MAP.get(type(pred.lit.value))
    if lit_arrow_type == pa.int32() and pred.lit.value > 2**31:
        lit_arrow_type = pa.int64()

    gandiva_lit = builder.make_literal(pred.lit.value, lit_arrow_type)

    if pred.op == Operation.EQ:
        return builder.make_function("equal", [pred_node, gandiva_lit], pa.bool_())
    elif pred.op == Operation.LT:
        return builder.make_function("less_than", [pred_node, gandiva_lit], pa.bool_())
    elif pred.op == Operation.LT_EQ:
        return builder.make_or([builder.make_function("less_than", [pred_node, gandiva_lit], pa.bool_()),
                               builder.make_function("equal", [pred_node, gandiva_lit], pa.bool_())])

    elif pred.op == Operation.GT:
        return builder.make_function("greater_than", [pred_node, gandiva_lit], pa.bool_())
    elif pred.op == Operation.GT_EQ:
        return builder.make_or([builder.make_function("greater_than", [pred_node, gandiva_lit], pa.bool_()),
                               builder.make_function("equal", [pred_node, gandiva_lit], pa.bool_())])
    elif pred.op == Operation.NOT_EQ:
        return builder.make_function("not_equal", [pred_node, gandiva_lit], pa.bool_())
    elif pred.op == Operation.IN:
        raise NotImplementedError()
    elif pred.op == Operation.NOT_IN:
        raise NotImplementedError()


def and_(left, right):
    return builder.make_and([left, right])


def or_(left, right):
    return builder.make_or([left, right])


def not_(child):
    raise NotImplementedError()

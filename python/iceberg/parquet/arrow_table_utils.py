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

import logging

from iceberg.api.expressions import Operation, Predicate
import numpy as np
import pandas as pd
import pyarrow as pa


_logger = logging.getLogger(__name__)


def apply_iceberg_filter(table, expected_to_file_map, expr):
    """
    Builds an indicator array to apply to a pandas dataframe and then returns the resulting
    pyarrow.Table

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

    indicator_array = get_indicator(table, expected_to_file_map, expr)
    # TODO: Re-implement with pyarrow api when it becomes available
    #   See https://issues.apache.org/jira/browse/ARROW-6996
    return pa.Table.from_pandas(table.to_pandas()[indicator_array], schema=table.schema, preserve_index=False)


def get_indicator(table, field_map, expr):
    """
    Builds an indicator array for the given expression by traversing the expression tree.

    Parameters
    ----------
    table: pyarrow.Table
        The table to apply the filter on
    field_map: map
        A mapping of the current schema field names to the file field names
    expr: iceberg.api.expressions.Expression
        An expression to use as a filter

    Returns
    -------
    pandas.core.series.Series
        A pandas series with a boolean indicator for whether the row qualifies for the filter

    """
    if isinstance(expr, Predicate):
        return predicate(table, field_map, expr)

    if expr.op() == Operation.TRUE:
        return np.full(table.num_rows, fill_value=True, dtype=np.bool)
    elif expr.op() == Operation.FALSE:
        return np.full(table.num_rows, fill_value=False, dtype=np.bool)
    elif expr.op() == Operation.NOT:
        return not_(get_indicator(table, field_map, expr.child))
    elif expr.op() == Operation.AND:
        return and_(get_indicator(table, field_map, expr.left),
                    get_indicator(table, field_map, expr.right))
    elif expr.op() == Operation.OR:
        return or_(get_indicator(table, field_map, expr.left),
                   get_indicator(table, field_map, expr.right))
    else:
        raise RuntimeError("Unknown operation: {}".format(expr.op()))


def predicate(table, field_map, pred):  # noqa: ignore=C901
    """
    Given a table and a predicate this function builds the indicator array
    by projecting the necessary field into a pandas dataframe and applying the
    appropriate operation.

    Parameters
    ----------
    table: pyarrow.Table
        The table to apply the filter on
    field_map: map
        A mapping of the current schema field names to the file field names
    pred: iceberg.api.expressions.Predicate
        A predicate to use as a filter

    Returns
    -------
    pandas.core.series.Series
        A pandas series with a boolean indicator for whether the row qualifies for the predicate

    """
    idx = table.schema.get_field_index(field_map[pred.ref.name])

    # if the field is missing, we can either say all rows do not match
    # or if the op is `is null` then all rows must match
    if idx < 0:
        if pred.op == Operation.IS_NULL:
            return np.full(table.num_rows, fill_value=True, dtype=np.bool)

        return np.full(table.num_rows, fill_value=False, dtype=np.bool)

    tbl_df = table[idx].data.to_pandas()
    if pred.op == Operation.IS_NULL:
        return pd.isnull(tbl_df)
    elif pred.op == Operation.NOT_NULL:
        return ~pd.isnull(tbl_df)
    elif pred.op == Operation.LT:
        final_df = tbl_df < pred.lit.value
    elif pred.op == Operation.LT_EQ:
        final_df = tbl_df <= pred.lit.value
    elif pred.op == Operation.GT:
        final_df = tbl_df > pred.lit.value
    elif pred.op == Operation.GT_EQ:
        final_df = tbl_df >= pred.lit.value
    elif pred.op == Operation.EQ:
        final_df = tbl_df == pred.lit.value
    elif pred.op == Operation.NOT_EQ:
        final_df = tbl_df != pred.lit.value
    elif pred.op == Operation.IN:
        raise NotImplementedError()
    elif pred.op == Operation.NOT_IN:
        raise NotImplementedError()
    else:
        raise NotImplementedError()

    # we combine the value indicator array with a is not null comparison to maintain
    # the semantics of SQL equality comparison wrt to null values
    return final_df & ~pd.isnull(tbl_df)


def and_(left, right):
    """
    And logical operator

    Parameters
    ----------
    left: iceberg.api.Expression
        The left side of the `and` expression
    right: iceberg.api.Expression
        The right side of the `and` expression

    Returns
    -------
    pandas.core.series.Series
        A pandas series with a boolean indicator for whether the row qualifies for the combined expression

    """
    return left & right


def or_(left, right):
    """
    or logical operator

    Parameters
    ----------
    left: iceberg.api.Expression
        The left side of the `or` expression
    right: iceberg.api.Expression
        The right side of the `or` expression

    Returns
    -------
    pandas.core.series.Series
        A pandas series with a boolean indicator for whether the row qualifies for the combined expression

    """
    return left | right


def not_(child):
    """
    Not logical operator

    Parameters
    ----------
    child: iceberg.api.Expression
        The expression to negate

    Returns
    -------
    pandas.core.series.Series
        A pandas series with a boolean indicator negated

    """
    return ~child

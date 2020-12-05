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


from iceberg.api.expressions import Expression, Operation, Predicate
import pyarrow.dataset as ds


def get_dataset_filter(expr: Expression, expected_to_file_map: dict) -> ds.Expression:
    """
    Given an Iceberg Expression and a mapping of names in the iceberg schema to the file schema,
    convert to an equivalent dataset filter using the file column names. Recursively iterate through the
    expressions to convert each portion one predicate at a time

    Parameters
    ----------
    expr : iceberg.api.expressions.Expression
        An Iceberg Expression to be converted
    expected_to_file_map : dict
        A dict that maps the iceberg schema names to the names from the file schema
    Returns
    -------
    pyarrow._dataset.Expression
        An equivalent dataset expression
    """
    if expr is None:
        return None

    if isinstance(expr, Predicate):
        return predicate(expr, expected_to_file_map)

    if expr.op() == Operation.TRUE:
        return None
    elif expr.op() == Operation.FALSE:
        return False
    elif expr.op() == Operation.NOT:
        return not_(get_dataset_filter(expr.child, expected_to_file_map))
    elif expr.op() == Operation.AND:
        return and_(get_dataset_filter(expr.left, expected_to_file_map),
                    get_dataset_filter(expr.right, expected_to_file_map))
    elif expr.op() == Operation.OR:
        return or_(get_dataset_filter(expr.left, expected_to_file_map),
                   get_dataset_filter(expr.right, expected_to_file_map))
    else:
        raise RuntimeError("Unknown operation: {}".format(expr.op()))


def predicate(pred: Predicate, field_map: dict) -> ds.Expression:  # noqa: ignore=C901
    """
    Given an Iceberg Predicate and a mapping of names in the iceberg schema to the file schema,
    convert to an equivalent dataset expression using the file column names.

    Parameters
    ----------
    pred : iceberg.api.expressions.Predicate
        An Iceberg Predicate to be converted
    field_map : dict
        A dict that maps the iceberg schema names to the names from the file schema
    Returns
    -------
    pyarrow._dataset.Expression
        An equivalent dataset expression
    """
    # get column name in the file schema so we can apply the predicate
    col_name = field_map.get(pred.ref.name)

    if col_name is None:
        if pred.op == Operation.IS_NULL:
            return ds.scalar(True) == ds.scalar(True)

        return ds.scalar(True) == ds.scalar(False)

    if pred.op == Operation.IS_NULL:
        return ~ds.field(col_name).is_valid()
    elif pred.op == Operation.NOT_NULL:
        return ds.field(col_name).is_valid()
    elif pred.op == Operation.LT:
        return ds.field(col_name) < pred.lit.value
    elif pred.op == Operation.LT_EQ:
        return ds.field(col_name) <= pred.lit.value
    elif pred.op == Operation.GT:
        return ds.field(col_name) > pred.lit.value
    elif pred.op == Operation.GT_EQ:
        return ds.field(col_name) >= pred.lit.value
    elif pred.op == Operation.EQ:
        return ds.field(col_name) == pred.lit.value
    elif pred.op == Operation.NOT_EQ:
        return ds.field(col_name) != pred.lit.value
    elif pred.op == Operation.IN:
        return ds.field(col_name).isin(pred.lit.value)
    elif pred.op == Operation.NOT_IN:
        return ds.field(col_name).isin(pred.lit.value)


def and_(left: ds.Expression, right: ds.Expression) -> ds.Expression:
    """
    Given a left and right expression combined them using the `AND` logical operator

    Parameters
    ----------
    left : pyarrow._dataset.Expression
        A Dataset `Expression` to logically `AND`
    right : pyarrow._dataset.Expression
       A Dataset `Expression` to logically `AND`
    Returns
    -------
    pyarrow._dataset.Expression
        The left and right `Expression` combined with `AND`
    """
    return left & right


def or_(left: ds.Expression, right: ds.Expression) -> ds.Expression:
    """
    Given a left and right expression combined them using the `OR` logical operator

    Parameters
    ----------
    left : pyarrow._dataset.Expression
        A Dataset `Expression` to logically `OR`
    right : pyarrow._dataset.Expression
       A Dataset `Expression` to logically `OR`
    Returns
    -------
    pyarrow._dataset.Expression
        The left and right `Expression` combined with `OR`
    """
    return left | right


def not_(child: ds.Expression) -> ds.Expression:
    """
    Given a child expression create the logical negation

    Parameters
    ----------
    child : pyarrow._dataset.Expression
        A Dataset `Expression` to logically `OR`
    Returns
    -------
    pyarrow._dataset.Expression
        The negation of the input `Expression`
    """
    return ~child

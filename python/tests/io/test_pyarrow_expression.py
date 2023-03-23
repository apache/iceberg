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
# pylint: disable=protected-access,unused-argument,redefined-outer-name


import pytest

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundNotStartsWith,
    BoundReference,
    BoundStartsWith,
    Not,
    Or,
    literal,
)
from pyiceberg.io.pyarrow import expression_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField


@pytest.fixture
def bound_reference(table_schema_simple: Schema) -> BoundReference[str]:
    return BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))


@pytest.fixture
def bound_double_reference() -> BoundReference[float]:
    schema = Schema(
        NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
        schema_id=1,
        identifier_field_ids=[2],
    )
    return BoundReference(schema.find_field(1), schema.accessor_for_field(1))


def test_expr_is_null_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundIsNull(bound_reference)))
        == "<pyarrow.compute.Expression is_null(foo, {nan_is_null=false})>"
    )


def test_expr_not_null_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotNull(bound_reference))) == "<pyarrow.compute.Expression is_valid(foo)>"


def test_expr_is_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundIsNaN(bound_double_reference))) == "<pyarrow.compute.Expression is_nan(foo)>"


def test_expr_not_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotNaN(bound_double_reference))) == "<pyarrow.compute.Expression invert(is_nan(foo))>"


def test_expr_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundEqualTo(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo == "hello")>'
    )


def test_expr_not_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotEqualTo(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo != "hello")>'
    )


def test_expr_greater_than_or_equal_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundGreaterThanOrEqual(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo >= "hello")>'
    )


def test_expr_greater_than_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundGreaterThan(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo > "hello")>'
    )


def test_expr_less_than_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundLessThan(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo < "hello")>'
    )


def test_expr_less_than_or_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundLessThanOrEqual(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo <= "hello")>'
    )


def test_expr_in_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundIn(bound_reference, {literal("hello"), literal("world")}))) in (
        """<pyarrow.compute.Expression is_in(foo, {value_set=string:[
  "world",
  "hello"
], skip_nulls=false})>""",
        """<pyarrow.compute.Expression is_in(foo, {value_set=string:[
  "hello",
  "world"
], skip_nulls=false})>""",
    )


def test_expr_not_in_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotIn(bound_reference, {literal("hello"), literal("world")}))) in (
        """<pyarrow.compute.Expression invert(is_in(foo, {value_set=string:[
  "world",
  "hello"
], skip_nulls=false}))>""",
        """<pyarrow.compute.Expression invert(is_in(foo, {value_set=string:[
  "hello",
  "world"
], skip_nulls=false}))>""",
    )


def test_expr_starts_with_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundStartsWith(bound_reference, literal("he"))))
        == '<pyarrow.compute.Expression starts_with(foo, {pattern="he", ignore_case=false})>'
    )


def test_expr_not_starts_with_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotStartsWith(bound_reference, literal("he"))))
        == '<pyarrow.compute.Expression invert(starts_with(foo, {pattern="he", ignore_case=false}))>'
    )


def test_and_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(And(BoundEqualTo(bound_reference, literal("hello")), BoundIsNull(bound_reference))))
        == '<pyarrow.compute.Expression ((foo == "hello") and is_null(foo, {nan_is_null=false}))>'
    )


def test_or_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(Or(BoundEqualTo(bound_reference, literal("hello")), BoundIsNull(bound_reference))))
        == '<pyarrow.compute.Expression ((foo == "hello") or is_null(foo, {nan_is_null=false}))>'
    )


def test_not_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(Not(BoundEqualTo(bound_reference, literal("hello")))))
        == '<pyarrow.compute.Expression invert((foo == "hello"))>'
    )


def test_always_true_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(AlwaysTrue())) == "<pyarrow.compute.Expression true>"


def test_always_false_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(AlwaysFalse())) == "<pyarrow.compute.Expression false>"

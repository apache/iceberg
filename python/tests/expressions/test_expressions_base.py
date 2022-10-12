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

import uuid
from decimal import Decimal
from typing import Any, List, Set

import pytest

from pyiceberg.conversions import to_bytes
from pyiceberg.expressions import base
from pyiceberg.expressions.base import (
    IN_PREDICATE_LIMIT,
    BoundPredicate,
    _from_byte_buffer,
    _ManifestEvalVisitor,
    rewrite_not,
    visit_bound_predicate,
)
from pyiceberg.expressions.literals import (
    Literal,
    LongLiteral,
    StringLiteral,
    literal,
)
from pyiceberg.manifest import ManifestFile, PartitionFieldSummary
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
)
from pyiceberg.utils.singleton import Singleton


class ExpressionA(base.BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionB()

    def __repr__(self):
        return "ExpressionA()"

    def __str__(self):
        return "testexpra"


class ExpressionB(base.BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionA()

    def __repr__(self):
        return "ExpressionB()"

    def __str__(self):
        return "testexprb"


class ExampleVisitor(base.BooleanExpressionVisitor[List]):
    """A test implementation of a BooleanExpressionVisitor

    As this visitor visits each node, it appends an element to a `visit_history` list. This enables testing that a given expression is
    visited in an expected order by the `visit` method.
    """

    def __init__(self):
        self.visit_history: List = []

    def visit_true(self) -> List:
        self.visit_history.append("TRUE")
        return self.visit_history

    def visit_false(self) -> List:
        self.visit_history.append("FALSE")
        return self.visit_history

    def visit_not(self, child_result: List) -> List:
        self.visit_history.append("NOT")
        return self.visit_history

    def visit_and(self, left_result: List, right_result: List) -> List:
        self.visit_history.append("AND")
        return self.visit_history

    def visit_or(self, left_result: List, right_result: List) -> List:
        self.visit_history.append("OR")
        return self.visit_history

    def visit_unbound_predicate(self, predicate) -> List:
        self.visit_history.append("UNBOUND PREDICATE")
        return self.visit_history

    def visit_bound_predicate(self, predicate) -> List:
        self.visit_history.append("BOUND PREDICATE")
        return self.visit_history

    def visit_test_expression_a(self) -> List:
        self.visit_history.append("ExpressionA")
        return self.visit_history

    def visit_test_expression_b(self) -> List:
        self.visit_history.append("ExpressionB")
        return self.visit_history


@base.visit.register(ExpressionA)
def _(obj: ExpressionA, visitor: ExampleVisitor) -> List:
    """Visit a ExpressionA with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_a()


@base.visit.register(ExpressionB)
def _(obj: ExpressionB, visitor: ExampleVisitor) -> List:
    """Visit a ExpressionB with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_b()


class FooBoundBooleanExpressionVisitor(base.BoundBooleanExpressionVisitor[List]):
    """A test implementation of a BoundBooleanExpressionVisitor
    As this visitor visits each node, it appends an element to a `visit_history` list. This enables testing that a given bound expression is
    visited in an expected order by the `visit` method.
    """

    def __init__(self):
        self.visit_history: List = []

    def visit_in(self, term: base.BoundTerm, literals: Set) -> List:
        self.visit_history.append("IN")
        return self.visit_history

    def visit_not_in(self, term: base.BoundTerm, literals: Set) -> List:
        self.visit_history.append("NOT_IN")
        return self.visit_history

    def visit_is_nan(self, term: base.BoundTerm) -> List:
        self.visit_history.append("IS_NAN")
        return self.visit_history

    def visit_not_nan(self, term: base.BoundTerm) -> List:
        self.visit_history.append("NOT_NAN")
        return self.visit_history

    def visit_is_null(self, term: base.BoundTerm) -> List:
        self.visit_history.append("IS_NULL")
        return self.visit_history

    def visit_not_null(self, term: base.BoundTerm) -> List:
        self.visit_history.append("NOT_NULL")
        return self.visit_history

    def visit_equal(self, term: base.BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("EQUAL")
        return self.visit_history

    def visit_not_equal(self, term: base.BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("NOT_EQUAL")
        return self.visit_history

    def visit_greater_than_or_equal(self, term: base.BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("GREATER_THAN_OR_EQUAL")
        return self.visit_history

    def visit_greater_than(self, term: base.BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("GREATER_THAN")
        return self.visit_history

    def visit_less_than(self, term: base.BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("LESS_THAN")
        return self.visit_history

    def visit_less_than_or_equal(self, term: base.BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("LESS_THAN_OR_EQUAL")
        return self.visit_history

    def visit_true(self) -> List:
        self.visit_history.append("TRUE")
        return self.visit_history

    def visit_false(self) -> List:
        self.visit_history.append("FALSE")
        return self.visit_history

    def visit_not(self, child_result: List) -> List:
        self.visit_history.append("NOT")
        return self.visit_history

    def visit_and(self, left_result: List, right_result: List) -> List:
        self.visit_history.append("AND")
        return self.visit_history

    def visit_or(self, left_result: List, right_result: List) -> List:
        self.visit_history.append("OR")
        return self.visit_history


@pytest.mark.parametrize(
    "op, rep",
    [
        (
            base.And(ExpressionA(), ExpressionB()),
            "And(left=ExpressionA(), right=ExpressionB())",
        ),
        (
            base.Or(ExpressionA(), ExpressionB()),
            "Or(left=ExpressionA(), right=ExpressionB())",
        ),
        (base.Not(ExpressionA()), "Not(child=ExpressionA())"),
    ],
)
def test_reprs(op: base.BooleanExpression, rep: str):
    assert repr(op) == rep


def test_isnull_inverse():
    assert ~base.IsNull(base.Reference("a")) == base.NotNull(base.Reference("a"))


def test_isnull_bind():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = base.BoundIsNull(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.IsNull(base.Reference("a")).bind(schema) == bound


def test_invert_is_null_bind():
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert ~base.IsNull(base.Reference("a")).bind(schema) == base.NotNull(base.Reference("a")).bind(schema)


def test_invert_not_null_bind():
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert ~base.NotNull(base.Reference("a")).bind(schema) == base.IsNull(base.Reference("a")).bind(schema)


def test_invert_is_nan_bind():
    schema = Schema(NestedField(2, "a", DoubleType(), required=False), schema_id=1)
    assert ~base.IsNaN(base.Reference("a")).bind(schema) == base.NotNaN(base.Reference("a")).bind(schema)


def test_invert_not_nan_bind():
    schema = Schema(NestedField(2, "a", DoubleType(), required=False), schema_id=1)
    assert ~base.NotNaN(base.Reference("a")).bind(schema) == base.IsNaN(base.Reference("a")).bind(schema)


def test_bind_expr_does_not_exists():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    with pytest.raises(ValueError) as exc_info:
        base.IsNull(base.Reference("b")).bind(schema)

    assert str(exc_info.value) == "Could not find field with name b, case_sensitive=True"


def test_bind_does_not_exists():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    with pytest.raises(ValueError) as exc_info:
        base.Reference("b").bind(schema)

    assert str(exc_info.value) == "Could not find field with name b, case_sensitive=True"


def test_isnull_bind_required():
    schema = Schema(NestedField(2, "a", IntegerType(), required=True), schema_id=1)
    assert base.IsNull(base.Reference("a")).bind(schema) == base.AlwaysFalse()


def test_notnull_inverse():
    assert ~base.NotNull(base.Reference("a")) == base.IsNull(base.Reference("a"))


def test_notnull_bind():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = base.BoundNotNull(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.NotNull(base.Reference("a")).bind(schema) == bound


def test_notnull_bind_required():
    schema = Schema(NestedField(2, "a", IntegerType(), required=True), schema_id=1)
    assert base.NotNull(base.Reference("a")).bind(schema) == base.AlwaysTrue()


def test_isnan_inverse():
    assert ~base.IsNaN(base.Reference("f")) == base.NotNaN(base.Reference("f"))


def test_isnan_bind_float():
    schema = Schema(NestedField(2, "f", FloatType()), schema_id=1)
    bound = base.BoundIsNaN(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.IsNaN(base.Reference("f")).bind(schema) == bound


def test_isnan_bind_double():
    schema = Schema(NestedField(2, "d", DoubleType()), schema_id=1)
    bound = base.BoundIsNaN(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.IsNaN(base.Reference("d")).bind(schema) == bound


def test_isnan_bind_nonfloat():
    schema = Schema(NestedField(2, "i", IntegerType()), schema_id=1)
    assert base.IsNaN(base.Reference("i")).bind(schema) == base.AlwaysFalse()


def test_notnan_inverse():
    assert ~base.NotNaN(base.Reference("f")) == base.IsNaN(base.Reference("f"))


def test_notnan_bind_float():
    schema = Schema(NestedField(2, "f", FloatType()), schema_id=1)
    bound = base.BoundNotNaN(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.NotNaN(base.Reference("f")).bind(schema) == bound


def test_notnan_bind_double():
    schema = Schema(NestedField(2, "d", DoubleType()), schema_id=1)
    bound = base.BoundNotNaN(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.NotNaN(base.Reference("d")).bind(schema) == bound


def test_notnan_bind_nonfloat():
    schema = Schema(NestedField(2, "i", IntegerType()), schema_id=1)
    assert base.NotNaN(base.Reference("i")).bind(schema) == base.AlwaysTrue()


@pytest.mark.parametrize(
    "op, string",
    [
        (base.And(ExpressionA(), ExpressionB()), "And(left=ExpressionA(), right=ExpressionB())"),
        (base.Or(ExpressionA(), ExpressionB()), "Or(left=ExpressionA(), right=ExpressionB())"),
        (base.Not(ExpressionA()), "Not(child=ExpressionA())"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


def test_ref_binding_case_sensitive(table_schema_simple: Schema):
    ref = base.Reference[str]("foo")
    bound = base.BoundReference[str](table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))
    assert ref.bind(table_schema_simple, case_sensitive=True) == bound


def test_ref_binding_case_sensitive_failure(table_schema_simple: Schema):
    ref = base.Reference[str]("Foo")
    with pytest.raises(ValueError):
        ref.bind(table_schema_simple, case_sensitive=True)


def test_ref_binding_case_insensitive(table_schema_simple: Schema):
    ref = base.Reference[str]("Foo")
    bound = base.BoundReference[str](table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))
    assert ref.bind(table_schema_simple, case_sensitive=False) == bound


def test_ref_binding_case_insensitive_failure(table_schema_simple: Schema):
    ref = base.Reference[str]("Foot")
    with pytest.raises(ValueError):
        ref.bind(table_schema_simple, case_sensitive=False)


def test_in_to_eq():
    assert base.In(base.Reference("x"), (literal(34.56),)) == base.EqualTo(base.Reference("x"), literal(34.56))


def test_empty_bind_in(table_schema_simple: Schema):
    bound = base.BoundIn[str](
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), set()
    )
    assert bound == base.AlwaysFalse()


def test_empty_bind_not_in(table_schema_simple: Schema):
    bound = base.BoundNotIn(
        base.BoundReference[str](table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), set()
    )
    assert bound == base.AlwaysTrue()


def test_bind_not_in_equal_term(table_schema_simple: Schema):
    bound = base.BoundNotIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), {literal("hello")}
    )
    assert (
        base.BoundNotEqualTo[str](
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literal=StringLiteral("hello"),
        )
        == bound
    )


def test_in_empty():
    assert base.In(base.Reference("foo"), ()) == base.AlwaysFalse()


def test_not_in_empty():
    assert base.NotIn(base.Reference("foo"), ()) == base.AlwaysTrue()


def test_not_in_equal():
    assert base.NotIn(base.Reference("foo"), (literal("hello"),)) == base.NotEqualTo(
        term=base.Reference(name="foo"), literal=StringLiteral("hello")
    )


def test_bind_in(table_schema_simple: Schema):
    bound = base.BoundIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert base.In(base.Reference("foo"), (literal("hello"), literal("world"))).bind(table_schema_simple) == bound


def test_bind_in_invert(table_schema_simple: Schema):
    bound = base.BoundIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == base.BoundNotIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_not_in_invert(table_schema_simple: Schema):
    bound = base.BoundNotIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == base.BoundIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_dedup(table_schema_simple: Schema):
    bound = base.BoundIn(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert (
        base.In(base.Reference("foo"), (literal("hello"), literal("world"), literal("world"))).bind(table_schema_simple) == bound
    )


def test_bind_dedup_to_eq(table_schema_simple: Schema):
    bound = base.BoundEqualTo(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert base.In(base.Reference("foo"), (literal("hello"), literal("hello"))).bind(table_schema_simple) == bound


def test_bound_equal_to_invert(table_schema_simple: Schema):
    bound = base.BoundEqualTo(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == base.BoundNotEqualTo(
        term=base.BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_not_equal_to_invert(table_schema_simple: Schema):
    bound = base.BoundNotEqualTo(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == base.BoundEqualTo(
        term=base.BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_greater_than_or_equal_invert(table_schema_simple: Schema):
    bound = base.BoundGreaterThanOrEqual(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == base.BoundLessThan(
        term=base.BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_greater_than_invert(table_schema_simple: Schema):
    bound = base.BoundGreaterThan(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == base.BoundLessThanOrEqual(
        term=base.BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_less_than_invert(table_schema_simple: Schema):
    bound = base.BoundLessThan(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == base.BoundGreaterThanOrEqual(
        term=base.BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_less_than_or_equal_invert(table_schema_simple: Schema):
    bound = base.BoundLessThanOrEqual(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == base.BoundGreaterThan(
        term=base.BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_not_equal_to_invert():
    bound = base.NotEqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )
    assert ~bound == base.EqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_greater_than_or_equal_invert():
    bound = base.GreaterThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )
    assert ~bound == base.LessThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_less_than_or_equal_invert():
    bound = base.LessThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )
    assert ~bound == base.GreaterThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


@pytest.mark.parametrize(
    "pred",
    [
        base.NotIn(base.Reference("foo"), (literal("hello"), literal("world"))),
        base.NotEqualTo(base.Reference("foo"), literal("hello")),
        base.EqualTo(base.Reference("foo"), literal("hello")),
        base.GreaterThan(base.Reference("foo"), literal("hello")),
        base.LessThan(base.Reference("foo"), literal("hello")),
        base.GreaterThanOrEqual(base.Reference("foo"), literal("hello")),
        base.LessThanOrEqual(base.Reference("foo"), literal("hello")),
    ],
)
def test_bind(pred, table_schema_simple: Schema):
    assert pred.bind(table_schema_simple, case_sensitive=True).term.field == table_schema_simple.find_field(
        pred.term.name, case_sensitive=True
    )


@pytest.mark.parametrize(
    "pred",
    [
        base.In(base.Reference("Bar"), (literal(5), literal(2))),
        base.NotIn(base.Reference("Bar"), (literal(5), literal(2))),
        base.NotEqualTo(base.Reference("Bar"), literal(5)),
        base.EqualTo(base.Reference("Bar"), literal(5)),
        base.GreaterThan(base.Reference("Bar"), literal(5)),
        base.LessThan(base.Reference("Bar"), literal(5)),
        base.GreaterThanOrEqual(base.Reference("Bar"), literal(5)),
        base.LessThanOrEqual(base.Reference("Bar"), literal(5)),
    ],
)
def test_bind_case_insensitive(pred, table_schema_simple: Schema):
    assert pred.bind(table_schema_simple, case_sensitive=False).term.field == table_schema_simple.find_field(
        pred.term.name, case_sensitive=False
    )


@pytest.mark.parametrize(
    "exp, testexpra, testexprb",
    [
        (
            base.And(ExpressionA(), ExpressionB()),
            base.And(ExpressionA(), ExpressionB()),
            base.Or(ExpressionA(), ExpressionB()),
        ),
        (
            base.Or(ExpressionA(), ExpressionB()),
            base.Or(ExpressionA(), ExpressionB()),
            base.And(ExpressionA(), ExpressionB()),
        ),
        (base.Not(ExpressionA()), base.Not(ExpressionA()), ExpressionB()),
        (ExpressionA(), ExpressionA(), ExpressionB()),
        (ExpressionB(), ExpressionB(), ExpressionA()),
        (
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("not_foo"), (literal("hello"), literal("world"))),
        ),
        (
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("goodbye"), literal("world"))),
        ),
    ],
)
def test_eq(exp, testexpra, testexprb):
    assert exp == testexpra and exp != testexprb


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            base.And(ExpressionA(), ExpressionB()),
            base.Or(ExpressionB(), ExpressionA()),
        ),
        (
            base.Or(ExpressionA(), ExpressionB()),
            base.And(ExpressionB(), ExpressionA()),
        ),
        (
            base.Not(ExpressionA()),
            ExpressionA(),
        ),
        (
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.NotIn(base.Reference("foo"), (literal("hello"), literal("world"))),
        ),
        (
            base.NotIn(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
        ),
        (base.GreaterThan(base.Reference("foo"), literal(5)), base.LessThanOrEqual(base.Reference("foo"), literal(5))),
        (base.LessThan(base.Reference("foo"), literal(5)), base.GreaterThanOrEqual(base.Reference("foo"), literal(5))),
        (base.EqualTo(base.Reference("foo"), literal(5)), base.NotEqualTo(base.Reference("foo"), literal(5))),
        (
            ExpressionA(),
            ExpressionB(),
        ),
    ],
)
def test_negate(lhs, rhs):
    assert ~lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            base.And(ExpressionA(), ExpressionB(), ExpressionA()),
            base.And(base.And(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (
            base.Or(ExpressionA(), ExpressionB(), ExpressionA()),
            base.Or(base.Or(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (base.Not(base.Not(ExpressionA())), ExpressionA()),
    ],
)
def test_reduce(lhs, rhs):
    assert lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (base.And(base.AlwaysTrue(), ExpressionB()), ExpressionB()),
        (base.And(base.AlwaysFalse(), ExpressionB()), base.AlwaysFalse()),
        (base.And(ExpressionB(), base.AlwaysTrue()), ExpressionB()),
        (base.Or(base.AlwaysTrue(), ExpressionB()), base.AlwaysTrue()),
        (base.Or(base.AlwaysFalse(), ExpressionB()), ExpressionB()),
        (base.Or(ExpressionA(), base.AlwaysFalse()), ExpressionA()),
        (base.Not(base.Not(ExpressionA())), ExpressionA()),
        (base.Not(base.AlwaysTrue()), base.AlwaysFalse()),
        (base.Not(base.AlwaysFalse()), base.AlwaysTrue()),
    ],
)
def test_base_AlwaysTrue_base_AlwaysFalse(lhs, rhs):
    assert lhs == rhs


def test_invert_always():
    assert ~base.AlwaysFalse() == base.AlwaysTrue()
    assert ~base.AlwaysTrue() == base.AlwaysFalse()


def test_accessor_base_class(foo_struct):
    """Test retrieving a value at a position of a container using an accessor"""

    uuid_value = uuid.uuid4()

    foo_struct.set(0, "foo")
    foo_struct.set(1, "bar")
    foo_struct.set(2, "baz")
    foo_struct.set(3, 1)
    foo_struct.set(4, 2)
    foo_struct.set(5, 3)
    foo_struct.set(6, 1.234)
    foo_struct.set(7, Decimal("1.234"))
    foo_struct.set(8, uuid_value)
    foo_struct.set(9, True)
    foo_struct.set(10, False)
    foo_struct.set(11, b"\x19\x04\x9e?")

    assert base.Accessor(position=0).get(foo_struct) == "foo"
    assert base.Accessor(position=1).get(foo_struct) == "bar"
    assert base.Accessor(position=2).get(foo_struct) == "baz"
    assert base.Accessor(position=3).get(foo_struct) == 1
    assert base.Accessor(position=4).get(foo_struct) == 2
    assert base.Accessor(position=5).get(foo_struct) == 3
    assert base.Accessor(position=6).get(foo_struct) == 1.234
    assert base.Accessor(position=7).get(foo_struct) == Decimal("1.234")
    assert base.Accessor(position=8).get(foo_struct) == uuid_value
    assert base.Accessor(position=9).get(foo_struct) is True
    assert base.Accessor(position=10).get(foo_struct) is False
    assert base.Accessor(position=11).get(foo_struct) == b"\x19\x04\x9e?"


def test_bound_reference_str_and_repr():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = base.Accessor(position=1)
    bound_ref = base.BoundReference(field=field, accessor=position1_accessor)
    assert str(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"
    assert repr(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"


def test_bound_reference_field_property():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = base.Accessor(position=1)
    bound_ref = base.BoundReference(field=field, accessor=position1_accessor)
    assert bound_ref.field == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


def test_bound_reference(table_schema_simple, foo_struct):
    """Test creating a BoundReference and evaluating it on a StructProtocol"""
    foo_struct.set(pos=1, value="foovalue")
    foo_struct.set(pos=2, value=123)
    foo_struct.set(pos=3, value=True)

    position1_accessor = base.Accessor(position=1)
    position2_accessor = base.Accessor(position=2)
    position3_accessor = base.Accessor(position=3)

    field1 = table_schema_simple.find_field(1)
    field2 = table_schema_simple.find_field(2)
    field3 = table_schema_simple.find_field(3)

    bound_ref1 = base.BoundReference(field=field1, accessor=position1_accessor)
    bound_ref2 = base.BoundReference(field=field2, accessor=position2_accessor)
    bound_ref3 = base.BoundReference(field=field3, accessor=position3_accessor)

    assert bound_ref1.eval(foo_struct) == "foovalue"
    assert bound_ref2.eval(foo_struct) == 123
    assert bound_ref3.eval(foo_struct) is True


def test_boolean_expression_visitor():
    """Test post-order traversal of boolean expression visit method"""
    expr = base.And(
        base.Or(base.Not(ExpressionA()), base.Not(ExpressionB()), ExpressionA(), ExpressionB()),
        base.Not(ExpressionA()),
        ExpressionB(),
    )
    visitor = ExampleVisitor()
    result = base.visit(expr, visitor=visitor)
    assert result == [
        "ExpressionA",
        "NOT",
        "ExpressionB",
        "NOT",
        "OR",
        "ExpressionA",
        "OR",
        "ExpressionB",
        "OR",
        "ExpressionA",
        "NOT",
        "AND",
        "ExpressionB",
        "AND",
    ]


def test_boolean_expression_visit_raise_not_implemented_error():
    """Test raise NotImplementedError when visiting an unsupported object type"""
    visitor = ExampleVisitor()
    with pytest.raises(NotImplementedError) as exc_info:
        base.visit("foo", visitor=visitor)

    assert str(exc_info.value) == "Cannot visit unsupported expression: foo"


def test_bind_visitor_already_bound(table_schema_simple: Schema):
    bound = base.BoundEqualTo(
        base.BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    with pytest.raises(TypeError) as exc_info:
        base.BindVisitor(base.visit(bound, visitor=base.BindVisitor(schema=table_schema_simple)))  # type: ignore
    assert (
        "Found already bound predicate: BoundEqualTo(term=BoundReference(field=NestedField(field_id=1, name='foo', field_type=StringType(), required=False), accessor=Accessor(position=0,inner=None)), literal=StringLiteral(hello))"
        == str(exc_info.value)
    )


def test_visit_bound_visitor_unknown_predicate():
    with pytest.raises(TypeError) as exc_info:
        visit_bound_predicate({"something"}, FooBoundBooleanExpressionVisitor())
    assert "Unknown predicate: {'something'}" == str(exc_info.value)


def test_always_true_expression_binding(table_schema_simple: Schema):
    """Test that visiting an always-true expression returns always-true"""
    unbound_expression = base.AlwaysTrue()
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysTrue()


def test_always_false_expression_binding(table_schema_simple: Schema):
    """Test that visiting an always-false expression returns always-false"""
    unbound_expression = base.AlwaysFalse()
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysFalse()


def test_always_false_and_always_true_expression_binding(table_schema_simple: Schema):
    """Test that visiting both an always-true AND always-false expression returns always-false"""
    unbound_expression = base.And(base.AlwaysTrue(), base.AlwaysFalse())
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysFalse()


def test_always_false_or_always_true_expression_binding(table_schema_simple: Schema):
    """Test that visiting always-true OR always-false expression returns always-true"""
    unbound_expression = base.Or(base.AlwaysTrue(), base.AlwaysFalse())
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysTrue()


@pytest.mark.parametrize(
    "unbound_and_expression,expected_bound_expression",
    [
        (
            base.And(
                base.In(base.Reference("foo"), (literal("foo"), literal("bar"))),
                base.In(base.Reference("bar"), (literal(1), literal(2), literal(3))),
            ),
            base.And(
                base.BoundIn[str](
                    base.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("foo"), StringLiteral("bar")},
                ),
                base.BoundIn[int](
                    base.BoundReference(
                        field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                        accessor=Accessor(position=1, inner=None),
                    ),
                    {LongLiteral(1), LongLiteral(2), LongLiteral(3)},
                ),
            ),
        ),
        (
            base.And(
                base.In(base.Reference("foo"), (literal("bar"), literal("baz"))),
                base.In(
                    base.Reference("bar"),
                    (literal(1),),
                ),
                base.In(
                    base.Reference("foo"),
                    (literal("baz"),),
                ),
            ),
            base.And(
                base.And(
                    base.BoundIn[str](
                        base.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("bar"), StringLiteral("baz")},
                    ),
                    base.BoundEqualTo[int](
                        base.BoundReference(
                            field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                            accessor=Accessor(position=1, inner=None),
                        ),
                        LongLiteral(1),
                    ),
                ),
                base.BoundEqualTo[str](
                    base.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    StringLiteral("baz"),
                ),
            ),
        ),
    ],
)
def test_and_expression_binding(unbound_and_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound AND expression with a bind-visitor returns the expected bound expression"""
    bound_expression = base.visit(unbound_and_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_or_expression,expected_bound_expression",
    [
        (
            base.Or(
                base.In(base.Reference("foo"), (literal("foo"), literal("bar"))),
                base.In(base.Reference("bar"), (literal(1), literal(2), literal(3))),
            ),
            base.Or(
                base.BoundIn[str](
                    base.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("foo"), StringLiteral("bar")},
                ),
                base.BoundIn[int](
                    base.BoundReference(
                        field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                        accessor=Accessor(position=1, inner=None),
                    ),
                    {LongLiteral(1), LongLiteral(2), LongLiteral(3)},
                ),
            ),
        ),
        (
            base.Or(
                base.In(base.Reference("foo"), (literal("bar"), literal("baz"))),
                base.In(
                    base.Reference("foo"),
                    (literal("bar"),),
                ),
                base.In(
                    base.Reference("foo"),
                    (literal("baz"),),
                ),
            ),
            base.Or(
                base.Or(
                    base.BoundIn[str](
                        base.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("bar"), StringLiteral("baz")},
                    ),
                    base.BoundIn[str](
                        base.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("bar")},
                    ),
                ),
                base.BoundIn[str](
                    base.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("baz")},
                ),
            ),
        ),
        (
            base.Or(
                base.AlwaysTrue(),
                base.AlwaysFalse(),
            ),
            base.AlwaysTrue(),
        ),
        (
            base.Or(
                base.AlwaysTrue(),
                base.AlwaysTrue(),
            ),
            base.AlwaysTrue(),
        ),
        (
            base.Or(
                base.AlwaysFalse(),
                base.AlwaysFalse(),
            ),
            base.AlwaysFalse(),
        ),
    ],
)
def test_or_expression_binding(unbound_or_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound OR expression with a bind-visitor returns the expected bound expression"""
    bound_expression = base.visit(unbound_or_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_in_expression,expected_bound_expression",
    [
        (
            base.In(base.Reference("foo"), (literal("foo"), literal("bar"))),
            base.BoundIn[str](
                base.BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                {StringLiteral("foo"), StringLiteral("bar")},
            ),
        ),
        (
            base.In(base.Reference("foo"), (literal("bar"), literal("baz"))),
            base.BoundIn[str](
                base.BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                {StringLiteral("bar"), StringLiteral("baz")},
            ),
        ),
        (
            base.In(
                base.Reference("foo"),
                (literal("bar"),),
            ),
            base.BoundEqualTo(
                base.BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                StringLiteral("bar"),
            ),
        ),
    ],
)
def test_in_expression_binding(unbound_in_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound IN expression with a bind-visitor returns the expected bound expression"""
    bound_expression = base.visit(unbound_in_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_not_expression,expected_bound_expression",
    [
        (
            base.Not(base.In(base.Reference("foo"), (literal("foo"), literal("bar")))),
            base.Not(
                base.BoundIn[str](
                    base.BoundReference[str](
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("foo"), StringLiteral("bar")},
                )
            ),
        ),
        (
            base.Not(
                base.Or(
                    base.In(base.Reference("foo"), (literal("foo"), literal("bar"))),
                    base.In(base.Reference("foo"), (literal("foo"), literal("bar"), literal("baz"))),
                )
            ),
            base.Not(
                base.Or(
                    base.BoundIn[str](
                        base.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("foo"), StringLiteral("bar")},
                    ),
                    base.BoundIn[str](
                        base.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("foo"), StringLiteral("bar"), StringLiteral("baz")},
                    ),
                ),
            ),
        ),
    ],
)
def test_not_expression_binding(unbound_not_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound NOT expression with a bind-visitor returns the expected bound expression"""
    bound_expression = base.visit(unbound_not_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


def test_bound_boolean_expression_visitor_and_in():
    """Test visiting an And and In expression with a bound boolean expression visitor"""
    bound_expression = base.And(
        base.BoundIn[str](
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals=(StringLiteral("foo"), StringLiteral("bar")),
        ),
        base.BoundIn[str](
            term=base.BoundReference(
                field=NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
                accessor=Accessor(position=1, inner=None),
            ),
            literals=(StringLiteral("baz"), StringLiteral("qux")),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["IN", "IN", "AND"]


def test_bound_boolean_expression_visitor_or():
    """Test visiting an Or expression with a bound boolean expression visitor"""
    bound_expression = base.Or(
        base.Not(
            base.BoundIn[str](
                base.BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                {StringLiteral("foo"), StringLiteral("bar")},
            )
        ),
        base.Not(
            base.BoundIn[str](
                base.BoundReference[str](
                    field=NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
                    accessor=Accessor(position=1, inner=None),
                ),
                {StringLiteral("baz"), StringLiteral("qux")},
            )
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["IN", "NOT", "IN", "NOT", "OR"]


def test_bound_boolean_expression_visitor_equal():
    bound_expression = base.BoundEqualTo[str](
        term=base.BoundReference(
            field=NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
            accessor=Accessor(position=1, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["EQUAL"]


def test_bound_boolean_expression_visitor_not_equal():
    bound_expression = base.BoundNotEqualTo[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["NOT_EQUAL"]


def test_bound_boolean_expression_visitor_always_true():
    bound_expression = base.AlwaysTrue()
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["TRUE"]


def test_bound_boolean_expression_visitor_always_false():
    bound_expression = base.AlwaysFalse()
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["FALSE"]


def test_bound_boolean_expression_visitor_in():
    bound_expression = base.BoundIn[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals=(StringLiteral("foo"), StringLiteral("bar")),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["IN"]


def test_bound_boolean_expression_visitor_not_in():
    bound_expression = base.BoundNotIn[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals=(StringLiteral("foo"), StringLiteral("bar")),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["NOT_IN"]


def test_bound_boolean_expression_visitor_is_nan():
    bound_expression = base.BoundIsNaN[str](
        term=base.BoundReference(
            field=NestedField(field_id=3, name="baz", field_type=FloatType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["IS_NAN"]


def test_bound_boolean_expression_visitor_not_nan():
    bound_expression = base.BoundNotNaN[str](
        term=base.BoundReference(
            field=NestedField(field_id=3, name="baz", field_type=FloatType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["NOT_NAN"]


def test_bound_boolean_expression_visitor_is_null():
    bound_expression = base.BoundIsNull[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["IS_NULL"]


def test_bound_boolean_expression_visitor_not_null():
    bound_expression = base.BoundNotNull[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["NOT_NULL"]


def test_bound_boolean_expression_visitor_greater_than():
    bound_expression = base.BoundGreaterThan[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["GREATER_THAN"]


def test_bound_boolean_expression_visitor_greater_than_or_equal():
    bound_expression = base.BoundGreaterThanOrEqual[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["GREATER_THAN_OR_EQUAL"]


def test_bound_boolean_expression_visitor_less_than():
    bound_expression = base.BoundLessThan[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["LESS_THAN"]


def test_bound_boolean_expression_visitor_less_than_or_equal():
    bound_expression = base.BoundLessThanOrEqual[str](
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = base.visit(bound_expression, visitor=visitor)
    assert result == ["LESS_THAN_OR_EQUAL"]


def test_bound_boolean_expression_visitor_raise_on_unbound_predicate():
    bound_expression = base.LessThanOrEqual[str](
        term=base.Reference("foo"),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    with pytest.raises(TypeError) as exc_info:
        base.visit(bound_expression, visitor=visitor)
    assert "Not a bound predicate" in str(exc_info.value)


def _to_byte_buffer(field_type: IcebergType, val: Any):
    if not isinstance(field_type, PrimitiveType):
        raise ValueError(f"Expected a PrimitiveType, got: {type(field_type)}")
    return to_bytes(field_type, val)


def _to_manifest_file(*partitions: PartitionFieldSummary) -> ManifestFile:
    return ManifestFile(
        manifest_path="",
        manifest_length=0,
        partition_spec_id=0,
        partitions=partitions,
    )


def _create_manifest_evaluator(bound_expr: BoundPredicate) -> _ManifestEvalVisitor:
    """For testing. Creates a bogus evaluator, and then replaces the expression"""
    evaluator = _ManifestEvalVisitor(
        Schema(NestedField(1, "id", LongType())), base.EqualTo(term=base.Reference("id"), literal=literal("foo"))
    )
    evaluator.partition_filter = bound_expr
    return evaluator


def test_manifest_evaluator_less_than_no_overlap():
    expr = base.BoundLessThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("c"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_overlap():
    expr = base.BoundLessThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_all_null():
    expr = base.BoundLessThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    # All null
    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_no_match():
    expr = base.BoundLessThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_no_overlap():
    expr = base.BoundLessThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("c"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_overlap():
    expr = base.BoundLessThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_all_null():
    expr = base.BoundLessThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    # All null
    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_no_match():
    expr = base.BoundLessThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_equal_no_overlap():
    expr = base.BoundEqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("c"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_equal_overlap():
    expr = base.BoundEqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = ManifestFile(
        manifest_path="",
        manifest_length=0,
        partition_spec_id=0,
        partitions=[
            PartitionFieldSummary(
                contains_null=False,
                contains_nan=False,
                lower_bound=_to_byte_buffer(StringType(), "a"),
                upper_bound=_to_byte_buffer(StringType(), "b"),
            )
        ],
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_equal_all_null():
    expr = base.BoundEqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    # All null
    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_equal_no_match():
    expr = base.BoundEqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_no_overlap():
    expr = base.BoundGreaterThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_overlap():
    expr = base.BoundGreaterThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("c"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_all_null():
    expr = base.BoundGreaterThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    # All null
    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_no_match():
    expr = base.BoundGreaterThan(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("d"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_no_overlap():
    expr = base.BoundGreaterThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_overlap():
    expr = base.BoundGreaterThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("b"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_all_null():
    expr = base.BoundGreaterThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("a"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    # All null
    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_no_match():
    expr = base.BoundGreaterThanOrEqual(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("d"),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_is_nan():
    expr = base.BoundIsNaN(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
            accessor=Accessor(position=0, inner=None),
        )
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=True, lower_bound=None, upper_bound=None)
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_is_nan_inverse():
    expr = base.BoundIsNaN(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
            accessor=Accessor(position=0, inner=None),
        )
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=False,
            lower_bound=_to_byte_buffer(DoubleType(), 18.15),
            upper_bound=_to_byte_buffer(DoubleType(), 19.25),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_nan():
    expr = base.BoundNotNaN(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
            accessor=Accessor(position=0, inner=None),
        )
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(DoubleType(), 18.15),
            upper_bound=_to_byte_buffer(DoubleType(), 19.25),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_nan_inverse():
    expr = base.BoundNotNaN(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
            accessor=Accessor(position=0, inner=None),
        )
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=True, lower_bound=None, upper_bound=None)
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_is_null():
    expr = base.BoundIsNull(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_is_null_inverse():
    expr = base.BoundIsNull(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=True, lower_bound=None, upper_bound=None)
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_null():
    expr = base.BoundNotNull(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_null_nan():
    expr = base.BoundNotNull(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=True, contains_nan=False, lower_bound=None, upper_bound=None)
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_null_inverse():
    expr = base.BoundNotNull(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )

    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_equal_to():
    expr = base.BoundNotEqualTo(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("this"),
    )

    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_in():
    expr = base.BoundIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(22)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not_in():
    expr = base.BoundNotIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(22)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=False,
            upper_bound=False,
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_in_null():
    expr = base.BoundIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(22)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=None,
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_in_inverse():
    expr = base.BoundIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(22)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 1815),
            upper_bound=_to_byte_buffer(LongType(), 1925),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_in_overflow():
    expr = base.BoundIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(IN_PREDICATE_LIMIT + 1)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 1815),
            upper_bound=_to_byte_buffer(LongType(), 1925),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_less_than_lower_bound():
    expr = base.BoundIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(2)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_greater_than_upper_bound():
    expr = base.BoundIn(
        term=base.BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals={LongLiteral(i) for i in range(20, 22)},
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_true():
    expr = base.AlwaysTrue()

    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_false():
    expr = base.AlwaysFalse()

    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))

    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_not():
    expr = base.Not(
        base.BoundIn(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={LongLiteral(i) for i in range(22)},
        )
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert not _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_and():
    expr = base.And(
        base.BoundIn(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={LongLiteral(i) for i in range(22)},
        ),
        base.BoundIn(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={LongLiteral(i) for i in range(22)},
        ),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert _create_manifest_evaluator(expr).eval(manifest)


def test_manifest_evaluator_or():
    expr = base.Or(
        base.BoundIn(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={LongLiteral(i) for i in range(22)},
        ),
        base.BoundIn(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=LongType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={LongLiteral(i) for i in range(22)},
        ),
    )

    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )

    assert _create_manifest_evaluator(expr).eval(manifest)


def test_bound_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.BoundPredicate(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            )
        )


def test_bound_unary_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.BoundUnaryPredicate(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            )
        )


def test_bound_set_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.BoundSetPredicate(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={literal("hello"), literal("world")},
        )


def test_bound_literal_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.BoundLiteralPredicate(
            term=base.BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literal=literal("world"),
        )


def test_non_primitive_from_byte_buffer():
    with pytest.raises(ValueError) as exc_info:
        _ = _from_byte_buffer(ListType(element_id=1, element_type=StringType()), b"\0x00")

    assert str(exc_info.value) == "Expected a PrimitiveType, got: <class 'pyiceberg.types.ListType'>"


def test_unbound_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.UnboundPredicate(term=base.Reference("a"))


def test_unbound_predicate_bind(table_schema_simple: Schema):
    with pytest.raises(NotImplementedError):
        _ = base.UnboundPredicate(term=base.Reference("a")).bind(table_schema_simple)


def test_unbound_unary_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.UnaryPredicate(term=base.Reference("a"))


def test_unbound_set_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.SetPredicate(term=base.Reference("a"), literals=(literal("hello"), literal("world")))


def test_unbound_literal_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~base.LiteralPredicate(term=base.Reference("a"), literal=literal("hello"))


def test_rewrite_not_equal_to():
    assert rewrite_not(base.Not(base.EqualTo(base.Reference("x"), literal(34.56)))) == base.NotEqualTo(
        base.Reference("x"), literal(34.56)
    )


def test_rewrite_not_not_equal_to():
    assert rewrite_not(base.Not(base.NotEqualTo(base.Reference("x"), literal(34.56)))) == base.EqualTo(
        base.Reference("x"), literal(34.56)
    )


def test_rewrite_not_in():
    assert rewrite_not(base.Not(base.In(base.Reference("x"), (literal(34.56),)))) == base.NotIn(
        base.Reference("x"), (literal(34.56),)
    )


def test_rewrite_and():
    assert rewrite_not(
        base.Not(
            base.And(
                base.EqualTo(base.Reference("x"), literal(34.56)),
                base.EqualTo(base.Reference("y"), literal(34.56)),
            )
        )
    ) == base.Or(
        base.NotEqualTo(term=base.Reference(name="x"), literal=literal(34.56)),
        base.NotEqualTo(term=base.Reference(name="y"), literal=literal(34.56)),
    )


def test_rewrite_or():
    assert rewrite_not(
        base.Not(
            base.Or(
                base.EqualTo(base.Reference("x"), literal(34.56)),
                base.EqualTo(base.Reference("y"), literal(34.56)),
            )
        )
    ) == base.And(
        base.NotEqualTo(term=base.Reference(name="x"), literal=literal(34.56)),
        base.NotEqualTo(term=base.Reference(name="y"), literal=literal(34.56)),
    )


def test_rewrite_always_false():
    assert rewrite_not(base.Not(base.AlwaysFalse())) == base.AlwaysTrue()


def test_rewrite_always_true():
    assert rewrite_not(base.Not(base.AlwaysTrue())) == base.AlwaysFalse()


def test_rewrite_bound():
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert rewrite_not(base.IsNull(base.Reference("a")).bind(schema)) == base.BoundIsNull(
        term=base.BoundReference(
            field=NestedField(field_id=2, name="a", field_type=IntegerType(), required=False),
            accessor=Accessor(position=0, inner=None),
        )
    )

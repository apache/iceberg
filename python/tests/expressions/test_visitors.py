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
# pylint:disable=redefined-outer-name

from typing import Any, List, Set

import pytest

from pyiceberg.conversions import to_bytes
from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundLiteralPredicate,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundPredicate,
    BoundReference,
    BoundSetPredicate,
    BoundTerm,
    BoundUnaryPredicate,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    LiteralPredicate,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    Or,
    Reference,
    SetPredicate,
    UnaryPredicate,
    UnboundPredicate,
)
from pyiceberg.expressions.literals import (
    Literal,
    LongLiteral,
    StringLiteral,
    literal,
)
from pyiceberg.expressions.visitors import (
    BindVisitor,
    BooleanExpressionVisitor,
    BoundBooleanExpressionVisitor,
    _ManifestEvalVisitor,
    rewrite_not,
    visit,
    visit_bound_predicate,
)
from pyiceberg.manifest import ManifestFile, PartitionFieldSummary
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    NestedField,
    PrimitiveType,
    StringType,
)
from pyiceberg.utils.singleton import Singleton


class ExpressionA(BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionB()

    def __repr__(self):
        return "ExpressionA()"

    def __str__(self):
        return "testexpra"


class ExpressionB(BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionA()

    def __repr__(self):
        return "ExpressionB()"

    def __str__(self):
        return "testexprb"


class ExampleVisitor(BooleanExpressionVisitor[List]):
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


@visit.register(ExpressionA)
def _(obj: ExpressionA, visitor: ExampleVisitor) -> List:
    """Visit a ExpressionA with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_a()


@visit.register(ExpressionB)
def _(obj: ExpressionB, visitor: ExampleVisitor) -> List:
    """Visit a ExpressionB with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_b()


class FooBoundBooleanExpressionVisitor(BoundBooleanExpressionVisitor[List]):
    """A test implementation of a BoundBooleanExpressionVisitor
    As this visitor visits each node, it appends an element to a `visit_history` list. This enables testing that a given bound expression is
    visited in an expected order by the `visit` method.
    """

    def __init__(self):
        self.visit_history: List = []

    def visit_in(self, term: BoundTerm, literals: Set) -> List:
        self.visit_history.append("IN")
        return self.visit_history

    def visit_not_in(self, term: BoundTerm, literals: Set) -> List:
        self.visit_history.append("NOT_IN")
        return self.visit_history

    def visit_is_nan(self, term: BoundTerm) -> List:
        self.visit_history.append("IS_NAN")
        return self.visit_history

    def visit_not_nan(self, term: BoundTerm) -> List:
        self.visit_history.append("NOT_NAN")
        return self.visit_history

    def visit_is_null(self, term: BoundTerm) -> List:
        self.visit_history.append("IS_NULL")
        return self.visit_history

    def visit_not_null(self, term: BoundTerm) -> List:
        self.visit_history.append("NOT_NULL")
        return self.visit_history

    def visit_equal(self, term: BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("EQUAL")
        return self.visit_history

    def visit_not_equal(self, term: BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("NOT_EQUAL")
        return self.visit_history

    def visit_greater_than_or_equal(self, term: BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("GREATER_THAN_OR_EQUAL")
        return self.visit_history

    def visit_greater_than(self, term: BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("GREATER_THAN")
        return self.visit_history

    def visit_less_than(self, term: BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
        self.visit_history.append("LESS_THAN")
        return self.visit_history

    def visit_less_than_or_equal(self, term: BoundTerm, literal: Literal) -> List:  # pylint: disable=redefined-outer-name
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


def test_boolean_expression_visitor():
    """Test post-order traversal of boolean expression visit method"""
    expr = And(
        Or(Not(ExpressionA()), Not(ExpressionB()), ExpressionA(), ExpressionB()),
        Not(ExpressionA()),
        ExpressionB(),
    )
    visitor = ExampleVisitor()
    result = visit(expr, visitor=visitor)
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
        visit("foo", visitor=visitor)

    assert str(exc_info.value) == "Cannot visit unsupported expression: foo"


def test_bind_visitor_already_bound(table_schema_simple: Schema):
    bound = BoundEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    with pytest.raises(TypeError) as exc_info:
        BindVisitor(visit(bound, visitor=BindVisitor(schema=table_schema_simple)))  # type: ignore
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
    unbound_expression = AlwaysTrue()
    bound_expression = visit(unbound_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == AlwaysTrue()


def test_always_false_expression_binding(table_schema_simple: Schema):
    """Test that visiting an always-false expression returns always-false"""
    unbound_expression = AlwaysFalse()
    bound_expression = visit(unbound_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == AlwaysFalse()


def test_always_false_and_always_true_expression_binding(table_schema_simple: Schema):
    """Test that visiting both an always-true AND always-false expression returns always-false"""
    unbound_expression = And(AlwaysTrue(), AlwaysFalse())
    bound_expression = visit(unbound_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == AlwaysFalse()


def test_always_false_or_always_true_expression_binding(table_schema_simple: Schema):
    """Test that visiting always-true OR always-false expression returns always-true"""
    unbound_expression = Or(AlwaysTrue(), AlwaysFalse())
    bound_expression = visit(unbound_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == AlwaysTrue()


@pytest.mark.parametrize(
    "unbound_and_expression,expected_bound_expression",
    [
        (
            And(
                In(Reference("foo"), (literal("foo"), literal("bar"))),
                In(Reference("bar"), (literal(1), literal(2), literal(3))),
            ),
            And(
                BoundIn[str](
                    BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("foo"), StringLiteral("bar")},
                ),
                BoundIn[int](
                    BoundReference(
                        field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                        accessor=Accessor(position=1, inner=None),
                    ),
                    {LongLiteral(1), LongLiteral(2), LongLiteral(3)},
                ),
            ),
        ),
        (
            And(
                In(Reference("foo"), (literal("bar"), literal("baz"))),
                In(
                    Reference("bar"),
                    (literal(1),),
                ),
                In(
                    Reference("foo"),
                    (literal("baz"),),
                ),
            ),
            And(
                And(
                    BoundIn[str](
                        BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("bar"), StringLiteral("baz")},
                    ),
                    BoundEqualTo[int](
                        BoundReference(
                            field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                            accessor=Accessor(position=1, inner=None),
                        ),
                        LongLiteral(1),
                    ),
                ),
                BoundEqualTo[str](
                    BoundReference(
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
    bound_expression = visit(unbound_and_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_or_expression,expected_bound_expression",
    [
        (
            Or(
                In(Reference("foo"), (literal("foo"), literal("bar"))),
                In(Reference("bar"), (literal(1), literal(2), literal(3))),
            ),
            Or(
                BoundIn[str](
                    BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("foo"), StringLiteral("bar")},
                ),
                BoundIn[int](
                    BoundReference(
                        field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                        accessor=Accessor(position=1, inner=None),
                    ),
                    {LongLiteral(1), LongLiteral(2), LongLiteral(3)},
                ),
            ),
        ),
        (
            Or(
                In(Reference("foo"), (literal("bar"), literal("baz"))),
                In(
                    Reference("foo"),
                    (literal("bar"),),
                ),
                In(
                    Reference("foo"),
                    (literal("baz"),),
                ),
            ),
            Or(
                Or(
                    BoundIn[str](
                        BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("bar"), StringLiteral("baz")},
                    ),
                    BoundIn[str](
                        BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("bar")},
                    ),
                ),
                BoundIn[str](
                    BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("baz")},
                ),
            ),
        ),
        (
            Or(
                AlwaysTrue(),
                AlwaysFalse(),
            ),
            AlwaysTrue(),
        ),
        (
            Or(
                AlwaysTrue(),
                AlwaysTrue(),
            ),
            AlwaysTrue(),
        ),
        (
            Or(
                AlwaysFalse(),
                AlwaysFalse(),
            ),
            AlwaysFalse(),
        ),
    ],
)
def test_or_expression_binding(unbound_or_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound OR expression with a bind-visitor returns the expected bound expression"""
    bound_expression = visit(unbound_or_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_in_expression,expected_bound_expression",
    [
        (
            In(Reference("foo"), (literal("foo"), literal("bar"))),
            BoundIn[str](
                BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                {StringLiteral("foo"), StringLiteral("bar")},
            ),
        ),
        (
            In(Reference("foo"), (literal("bar"), literal("baz"))),
            BoundIn[str](
                BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                {StringLiteral("bar"), StringLiteral("baz")},
            ),
        ),
        (
            In(
                Reference("foo"),
                (literal("bar"),),
            ),
            BoundEqualTo(
                BoundReference[str](
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
    bound_expression = visit(unbound_in_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_not_expression,expected_bound_expression",
    [
        (
            Not(In(Reference("foo"), (literal("foo"), literal("bar")))),
            Not(
                BoundIn[str](
                    BoundReference[str](
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    {StringLiteral("foo"), StringLiteral("bar")},
                )
            ),
        ),
        (
            Not(
                Or(
                    In(Reference("foo"), (literal("foo"), literal("bar"))),
                    In(Reference("foo"), (literal("foo"), literal("bar"), literal("baz"))),
                )
            ),
            Not(
                Or(
                    BoundIn[str](
                        BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        {StringLiteral("foo"), StringLiteral("bar")},
                    ),
                    BoundIn[str](
                        BoundReference(
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
    bound_expression = visit(unbound_not_expression, visitor=BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


def test_bound_boolean_expression_visitor_and_in():
    """Test visiting an And and In expression with a bound boolean expression visitor"""
    bound_expression = And(
        BoundIn[str](
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals=(StringLiteral("foo"), StringLiteral("bar")),
        ),
        BoundIn[str](
            term=BoundReference(
                field=NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
                accessor=Accessor(position=1, inner=None),
            ),
            literals=(StringLiteral("baz"), StringLiteral("qux")),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["IN", "IN", "AND"]


def test_bound_boolean_expression_visitor_or():
    """Test visiting an Or expression with a bound boolean expression visitor"""
    bound_expression = Or(
        Not(
            BoundIn[str](
                BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                {StringLiteral("foo"), StringLiteral("bar")},
            )
        ),
        Not(
            BoundIn[str](
                BoundReference[str](
                    field=NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
                    accessor=Accessor(position=1, inner=None),
                ),
                {StringLiteral("baz"), StringLiteral("qux")},
            )
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["IN", "NOT", "IN", "NOT", "OR"]


def test_bound_boolean_expression_visitor_equal():
    bound_expression = BoundEqualTo[str](
        term=BoundReference(
            field=NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
            accessor=Accessor(position=1, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["EQUAL"]


def test_bound_boolean_expression_visitor_not_equal():
    bound_expression = BoundNotEqualTo[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["NOT_EQUAL"]


def test_bound_boolean_expression_visitor_always_true():
    bound_expression = AlwaysTrue()
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["TRUE"]


def test_bound_boolean_expression_visitor_always_false():
    bound_expression = AlwaysFalse()
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["FALSE"]


def test_bound_boolean_expression_visitor_in():
    bound_expression = BoundIn[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals=(StringLiteral("foo"), StringLiteral("bar")),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["IN"]


def test_bound_boolean_expression_visitor_not_in():
    bound_expression = BoundNotIn[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literals=(StringLiteral("foo"), StringLiteral("bar")),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["NOT_IN"]


def test_bound_boolean_expression_visitor_is_nan():
    bound_expression = BoundIsNaN[str](
        term=BoundReference(
            field=NestedField(field_id=3, name="baz", field_type=FloatType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["IS_NAN"]


def test_bound_boolean_expression_visitor_not_nan():
    bound_expression = BoundNotNaN[str](
        term=BoundReference(
            field=NestedField(field_id=3, name="baz", field_type=FloatType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["NOT_NAN"]


def test_bound_boolean_expression_visitor_is_null():
    bound_expression = BoundIsNull[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["IS_NULL"]


def test_bound_boolean_expression_visitor_not_null():
    bound_expression = BoundNotNull[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["NOT_NULL"]


def test_bound_boolean_expression_visitor_greater_than():
    bound_expression = BoundGreaterThan[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["GREATER_THAN"]


def test_bound_boolean_expression_visitor_greater_than_or_equal():
    bound_expression = BoundGreaterThanOrEqual[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["GREATER_THAN_OR_EQUAL"]


def test_bound_boolean_expression_visitor_less_than():
    bound_expression = BoundLessThan[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["LESS_THAN"]


def test_bound_boolean_expression_visitor_less_than_or_equal():
    bound_expression = BoundLessThanOrEqual[str](
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    result = visit(bound_expression, visitor=visitor)
    assert result == ["LESS_THAN_OR_EQUAL"]


def test_bound_boolean_expression_visitor_raise_on_unbound_predicate():
    bound_expression = LessThanOrEqual[str](
        term=Reference("foo"),
        literal=StringLiteral("foo"),
    )
    visitor = FooBoundBooleanExpressionVisitor()
    with pytest.raises(TypeError) as exc_info:
        visit(bound_expression, visitor=visitor)
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


INT_MIN_VALUE = 30
INT_MAX_VALUE = 79

INT_MIN = _to_byte_buffer(IntegerType(), INT_MIN_VALUE)
INT_MAX = _to_byte_buffer(IntegerType(), INT_MAX_VALUE)

STRING_MIN = _to_byte_buffer(StringType(), "a")
STRING_MAX = _to_byte_buffer(StringType(), "z")


@pytest.fixture
def schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "all_nulls_missing_nan", StringType(), required=False),
        NestedField(3, "some_nulls", StringType(), required=False),
        NestedField(4, "no_nulls", StringType(), required=False),
        NestedField(5, "float", FloatType(), required=False),
        NestedField(6, "all_nulls_double", DoubleType(), required=False),
        NestedField(7, "all_nulls_no_nans", FloatType(), required=False),
        NestedField(8, "all_nans", DoubleType(), required=False),
        NestedField(9, "both_nan_and_null", FloatType(), required=False),
        NestedField(10, "no_nan_or_null", DoubleType(), required=False),
        NestedField(11, "all_nulls_missing_nan_float", FloatType(), required=False),
        NestedField(12, "all_same_value_or_null", StringType(), required=False),
        NestedField(13, "no_nulls_same_value_a", StringType(), required=False),
    )


@pytest.fixture
def manifest_no_stats() -> ManifestFile:
    return _to_manifest_file()


@pytest.fixture
def manifest() -> ManifestFile:
    return _to_manifest_file(
        # id
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=None,
            lower_bound=INT_MIN,
            upper_bound=INT_MAX,
        ),
        # all_nulls_missing_nan
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=None,
            lower_bound=None,
            upper_bound=None,
        ),
        # some_nulls
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=None,
            lower_bound=STRING_MIN,
            upper_bound=STRING_MAX,
        ),
        # no_nulls
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=None,
            lower_bound=STRING_MIN,
            upper_bound=STRING_MAX,
        ),
        # float
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=None,
            lower_bound=_to_byte_buffer(FloatType(), 0.0),
            upper_bound=_to_byte_buffer(FloatType(), 20.0),
        ),
        # all_nulls_double
        PartitionFieldSummary(contains_null=True, contains_nan=None, lower_bound=None, upper_bound=None),
        # all_nulls_no_nans
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=False,
            lower_bound=None,
            upper_bound=None,
        ),
        # all_nans
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=True,
            lower_bound=None,
            upper_bound=None,
        ),
        # both_nan_and_null
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=None,
            upper_bound=None,
        ),
        # no_nan_or_null
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(FloatType(), 0.0),
            upper_bound=_to_byte_buffer(FloatType(), 20.0),
        ),
        # all_nulls_missing_nan_float
        PartitionFieldSummary(contains_null=True, contains_nan=None, lower_bound=None, upper_bound=None),
        # all_same_value_or_null
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=None,
            lower_bound=STRING_MIN,
            upper_bound=STRING_MIN,
        ),
        # no_nulls_same_value_a
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=None,
            lower_bound=STRING_MIN,
            upper_bound=STRING_MIN,
        ),
    )


def test_all_nulls(schema: Schema, manifest: ManifestFile) -> None:
    assert not _ManifestEvalVisitor(schema, NotNull(Reference("all_nulls_missing_nan"))).eval(
        manifest
    ), "Should skip: all nulls column with non-floating type contains all null"

    assert _ManifestEvalVisitor(schema, NotNull(Reference("all_nulls_missing_nan_float"))).eval(
        manifest
    ), "Should read: no NaN information may indicate presence of NaN value"

    assert _ManifestEvalVisitor(schema, NotNull(Reference("some_nulls"))).eval(
        manifest
    ), "Should read: column with some nulls contains a non-null value"

    assert _ManifestEvalVisitor(schema, NotNull(Reference("no_nulls"))).eval(
        manifest
    ), "Should read: non-null column contains a non-null value"


def test_no_nulls(schema: Schema, manifest: ManifestFile) -> None:
    assert _ManifestEvalVisitor(schema, IsNull(Reference("all_nulls_missing_nan"))).eval(
        manifest
    ), "Should read: at least one null value in all null column"

    assert _ManifestEvalVisitor(schema, IsNull(Reference("some_nulls"))).eval(
        manifest
    ), "Should read: column with some nulls contains a null value"

    assert not _ManifestEvalVisitor(schema, IsNull(Reference("no_nulls"))).eval(
        manifest
    ), "Should skip: non-null column contains no null values"

    assert _ManifestEvalVisitor(schema, IsNull(Reference("both_nan_and_null"))).eval(
        manifest
    ), "Should read: both_nan_and_null column contains no null values"


def test_is_nan(schema: Schema, manifest: ManifestFile) -> None:
    assert _ManifestEvalVisitor(schema, IsNaN(Reference("float"))).eval(
        manifest
    ), "Should read: no information on if there are nan value in float column"

    assert _ManifestEvalVisitor(schema, IsNaN(Reference("all_nulls_double"))).eval(
        manifest
    ), "Should read: no NaN information may indicate presence of NaN value"

    assert _ManifestEvalVisitor(schema, IsNaN(Reference("all_nulls_missing_nan_float"))).eval(
        manifest
    ), "Should read: no NaN information may indicate presence of NaN value"

    assert not _ManifestEvalVisitor(schema, IsNaN(Reference("all_nulls_no_nans"))).eval(
        manifest
    ), "Should skip: no nan column doesn't contain nan value"

    assert _ManifestEvalVisitor(schema, IsNaN(Reference("all_nans"))).eval(
        manifest
    ), "Should read: all_nans column contains nan value"

    assert _ManifestEvalVisitor(schema, IsNaN(Reference("both_nan_and_null"))).eval(
        manifest
    ), "Should read: both_nan_and_null column contains nan value"

    assert not _ManifestEvalVisitor(schema, IsNaN(Reference("no_nan_or_null"))).eval(
        manifest
    ), "Should skip: no_nan_or_null column doesn't contain nan value"


def test_not_nan(schema: Schema, manifest: ManifestFile) -> None:
    assert _ManifestEvalVisitor(schema, NotNaN(Reference("float"))).eval(
        manifest
    ), "Should read: no information on if there are nan value in float column"

    assert _ManifestEvalVisitor(schema, NotNaN(Reference("all_nulls_double"))).eval(
        manifest
    ), "Should read: all null column contains non nan value"

    assert _ManifestEvalVisitor(schema, NotNaN(Reference("all_nulls_no_nans"))).eval(
        manifest
    ), "Should read: no_nans column contains non nan value"

    assert not _ManifestEvalVisitor(schema, NotNaN(Reference("all_nans"))).eval(
        manifest
    ), "Should skip: all nans column doesn't contain non nan value"

    assert _ManifestEvalVisitor(schema, NotNaN(Reference("both_nan_and_null"))).eval(
        manifest
    ), "Should read: both_nan_and_null nans column contains non nan value"

    assert _ManifestEvalVisitor(schema, NotNaN(Reference("no_nan_or_null"))).eval(
        manifest
    ), "Should read: no_nan_or_null column contains non nan value"


def test_missing_stats(schema: Schema, manifest_no_stats: ManifestFile):
    expressions: List[BooleanExpression] = [
        LessThan(Reference[int]("id"), LongLiteral(5)),
        LessThanOrEqual(Reference[int]("id"), LongLiteral(30)),
        EqualTo(Reference[int]("id"), LongLiteral(70)),
        GreaterThan(Reference[int]("id"), LongLiteral(78)),
        GreaterThanOrEqual(Reference[int]("id"), LongLiteral(90)),
        NotEqualTo(Reference[int]("id"), LongLiteral(101)),
        IsNull(Reference[int]("id")),
        NotNull(Reference[int]("id")),
        IsNaN(Reference[float]("float")),
        NotNaN(Reference[float]("float")),
    ]

    for expr in expressions:
        assert _ManifestEvalVisitor(schema, expr).eval(manifest_no_stats), f"Should read when missing stats for expr: {expr}"


def test_not(schema: Schema, manifest: ManifestFile):
    assert _ManifestEvalVisitor(schema, Not(LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)))).eval(
        manifest
    ), "Should read: not(false)"

    assert not _ManifestEvalVisitor(schema, Not(GreaterThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)))).eval(
        manifest
    ), "Should skip: not(true)"


def test_and(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(
        schema,
        And(
            LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)),
            GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 30)),
        ),
    ).eval(manifest), "Should skip: and(false, true)"

    assert not _ManifestEvalVisitor(
        schema,
        And(
            LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)),
            GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 1)),
        ),
    ).eval(manifest), "Should skip: and(false, false)"

    assert _ManifestEvalVisitor(
        schema,
        And(
            GreaterThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)),
            LessThanOrEqual(Reference[int]("id"), LongLiteral(INT_MIN_VALUE)),
        ),
    ).eval(manifest), "Should read: and(true, true)"


def test_or(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(
        schema,
        Or(
            LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)),
            GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 1)),
        ),
    ).eval(manifest), "Should skip: or(false, false)"

    assert _ManifestEvalVisitor(
        schema,
        Or(
            LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)),
            GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE - 19)),
        ),
    ).eval(manifest), "Should read: or(false, true)"


def test_integer_lt(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(schema, LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25))).eval(
        manifest
    ), "Should not read: id range below lower bound (5 < 30)"

    assert not _ManifestEvalVisitor(schema, LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE))).eval(
        manifest
    ), "Should not read: id range below lower bound (30 is not < 30)"

    assert _ManifestEvalVisitor(schema, LessThan(Reference[int]("id"), LongLiteral(INT_MIN_VALUE + 1))).eval(
        manifest
    ), "Should read: one possible id"

    assert _ManifestEvalVisitor(schema, LessThan(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should read: may possible ids"


def test_integer_lt_eq(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(schema, LessThanOrEqual(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25))).eval(
        manifest
    ), "Should not read: id range below lower bound (5 < 30)"

    assert not _ManifestEvalVisitor(schema, LessThanOrEqual(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 1))).eval(
        manifest
    ), "Should not read: id range below lower bound (29 < 30)"

    assert _ManifestEvalVisitor(schema, LessThanOrEqual(Reference[int]("id"), LongLiteral(INT_MIN_VALUE))).eval(
        manifest
    ), "Should read: one possible id"

    assert _ManifestEvalVisitor(schema, LessThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should read: many possible ids"


def test_integer_gt(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(schema, GreaterThan(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 6))).eval(
        manifest
    ), "Should not read: id range above upper bound (85 < 79)"

    assert not _ManifestEvalVisitor(schema, GreaterThan(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should not read: id range above upper bound (79 is not > 79)"

    assert _ManifestEvalVisitor(schema, GreaterThan(Reference[int]("id"), LongLiteral(INT_MAX_VALUE - 1))).eval(
        manifest
    ), "Should read: one possible id"

    assert _ManifestEvalVisitor(schema, GreaterThan(Reference[int]("id"), LongLiteral(INT_MAX_VALUE - 4))).eval(
        manifest
    ), "Should read: may possible ids"


def test_integer_gt_eq(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(schema, GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 6))).eval(
        manifest
    ), "Should not read: id range above upper bound (85 < 79)"

    assert not _ManifestEvalVisitor(schema, GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 1))).eval(
        manifest
    ), "Should not read: id range above upper bound (80 > 79)"

    assert _ManifestEvalVisitor(schema, GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should read: one possible id"

    assert _ManifestEvalVisitor(schema, GreaterThanOrEqual(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should read: may possible ids"


def test_integer_eq(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25))).eval(
        manifest
    ), "Should not read: id below lower bound"

    assert not _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 1))).eval(
        manifest
    ), "Should not read: id below lower bound"

    assert _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE))).eval(
        manifest
    ), "Should read: id equal to lower bound"

    assert _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE - 4))).eval(
        manifest
    ), "Should read: id between lower and upper bounds"

    assert _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should read: id equal to upper bound"

    assert not _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 1))).eval(
        manifest
    ), "Should not read: id above upper bound"

    assert not _ManifestEvalVisitor(schema, EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 6))).eval(
        manifest
    ), "Should not read: id above upper bound"


def test_integer_not_eq(schema: Schema, manifest: ManifestFile):
    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25))).eval(
        manifest
    ), "Should read: id below lower bound"

    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 1))).eval(
        manifest
    ), "Should read: id below lower bound"

    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE))).eval(
        manifest
    ), "Should read: id equal to lower bound"

    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE - 4))).eval(
        manifest
    ), "Should read: id between lower and upper bounds"

    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE))).eval(
        manifest
    ), "Should read: id equal to upper bound"

    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 1))).eval(
        manifest
    ), "Should read: id above upper bound"

    assert _ManifestEvalVisitor(schema, NotEqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 6))).eval(
        manifest
    ), "Should read: id above upper bound"


def test_integer_not_eq_rewritten(schema: Schema, manifest: ManifestFile):
    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 25)))).eval(
        manifest
    ), "Should read: id below lower bound"

    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE - 1)))).eval(
        manifest
    ), "Should read: id below lower bound"

    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MIN_VALUE)))).eval(
        manifest
    ), "Should read: id equal to lower bound"

    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE - 4)))).eval(
        manifest
    ), "Should read: id between lower and upper bounds"

    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE)))).eval(
        manifest
    ), "Should read: id equal to upper bound"

    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 1)))).eval(
        manifest
    ), "Should read: id above upper bound"

    assert _ManifestEvalVisitor(schema, Not(EqualTo(Reference[int]("id"), LongLiteral(INT_MAX_VALUE + 6)))).eval(
        manifest
    ), "Should read: id above upper bound"


def test_integer_not_eq_rewritten_case_insensitive(schema: Schema, manifest: ManifestFile):
    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MIN_VALUE - 25))), case_sensitive=False
    ).eval(manifest), "Should read: id below lower bound"

    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MIN_VALUE - 1))), case_sensitive=False
    ).eval(manifest), "Should read: id below lower bound"

    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MIN_VALUE))), case_sensitive=False
    ).eval(manifest), "Should read: id equal to lower bound"

    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MAX_VALUE - 4))), case_sensitive=False
    ).eval(manifest), "Should read: id between lower and upper bounds"

    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MAX_VALUE))), case_sensitive=False
    ).eval(manifest), "Should read: id equal to upper bound"

    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MAX_VALUE + 1))), case_sensitive=False
    ).eval(manifest), "Should read: id above upper bound"

    assert _ManifestEvalVisitor(
        schema, Not(EqualTo(Reference[int]("ID"), LongLiteral(INT_MAX_VALUE + 6))), case_sensitive=False
    ).eval(manifest), "Should read: id above upper bound"


def test_integer_in(schema: Schema, manifest: ManifestFile):
    assert not _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MIN_VALUE - 25), LongLiteral(INT_MIN_VALUE - 24)))
    ).eval(manifest), "Should not read: id below lower bound (5 < 30, 6 < 30)"

    assert not _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MIN_VALUE - 2), LongLiteral(INT_MIN_VALUE - 1)))
    ).eval(manifest), "Should not read: id below lower bound (28 < 30, 29 < 30)"

    assert _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MIN_VALUE - 1), LongLiteral(INT_MIN_VALUE)))
    ).eval(manifest), "Should read: id equal to lower bound (30 == 30)"

    assert _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE - 4), LongLiteral(INT_MAX_VALUE - 3)))
    ).eval(manifest), "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"

    assert _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE), LongLiteral(INT_MAX_VALUE + 1)))
    ).eval(manifest), "Should read: id equal to upper bound (79 == 79)"

    assert not _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE + 1), LongLiteral(INT_MAX_VALUE + 2)))
    ).eval(manifest), "Should not read: id above upper bound (80 > 79, 81 > 79)"

    assert not _ManifestEvalVisitor(
        schema, In(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE + 6), LongLiteral(INT_MAX_VALUE + 7)))
    ).eval(manifest), "Should not read: id above upper bound (85 > 79, 86 > 79)"

    assert not _ManifestEvalVisitor(
        schema, In(Reference[str]("all_nulls_missing_nan"), (StringLiteral("abc"), StringLiteral("def")))
    ).eval(manifest), "Should skip: in on all nulls column"

    assert _ManifestEvalVisitor(schema, In(Reference[str]("some_nulls"), (StringLiteral("abc"), StringLiteral("def")))).eval(
        manifest
    ), "Should read: in on some nulls column"

    assert _ManifestEvalVisitor(schema, In(Reference[str]("no_nulls"), (StringLiteral("abc"), StringLiteral("def")))).eval(
        manifest
    ), "Should read: in on no nulls column"


def test_integer_not_in(schema: Schema, manifest: ManifestFile):
    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MIN_VALUE - 25), LongLiteral(INT_MIN_VALUE - 24)))
    ).eval(manifest), "Should read: id below lower bound (5 < 30, 6 < 30)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MIN_VALUE - 2), LongLiteral(INT_MIN_VALUE - 1)))
    ).eval(manifest), "Should read: id below lower bound (28 < 30, 29 < 30)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MIN_VALUE - 1), LongLiteral(INT_MIN_VALUE)))
    ).eval(manifest), "Should read: id equal to lower bound (30 == 30)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE - 4), LongLiteral(INT_MAX_VALUE - 3)))
    ).eval(manifest), "Should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE), LongLiteral(INT_MAX_VALUE + 1)))
    ).eval(manifest), "Should read: id equal to upper bound (79 == 79)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE + 1), LongLiteral(INT_MAX_VALUE + 2)))
    ).eval(manifest), "Should read: id above upper bound (80 > 79, 81 > 79)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[int]("id"), (LongLiteral(INT_MAX_VALUE + 6), LongLiteral(INT_MAX_VALUE + 7)))
    ).eval(manifest), "Should read: id above upper bound (85 > 79, 86 > 79)"

    assert _ManifestEvalVisitor(
        schema, NotIn(Reference[str]("all_nulls_missing_nan"), (StringLiteral("abc"), StringLiteral("def")))
    ).eval(manifest), "Should read: notIn on no nulls column"

    assert _ManifestEvalVisitor(schema, NotIn(Reference[str]("some_nulls"), (StringLiteral("abc"), StringLiteral("def")))).eval(
        manifest
    ), "Should read: in on some nulls column"

    assert _ManifestEvalVisitor(schema, NotIn(Reference[str]("no_nulls"), (StringLiteral("abc"), StringLiteral("def")))).eval(
        manifest
    ), "Should read: in on no nulls column"


def test_bound_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            )
        )


def test_bound_unary_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundUnaryPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            )
        )


def test_bound_set_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundSetPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={literal("hello"), literal("world")},
        )


def test_bound_literal_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundLiteralPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literal=literal("world"),
        )


def test_unbound_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~UnboundPredicate(term=Reference("a"))


def test_unbound_predicate_bind(table_schema_simple: Schema):
    with pytest.raises(NotImplementedError):
        _ = UnboundPredicate(term=Reference("a")).bind(table_schema_simple)


def test_unbound_unary_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~UnaryPredicate(term=Reference("a"))


def test_unbound_set_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~SetPredicate(term=Reference("a"), literals=(literal("hello"), literal("world")))


def test_unbound_literal_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~LiteralPredicate(term=Reference("a"), literal=literal("hello"))


def test_rewrite_not_equal_to():
    assert rewrite_not(Not(EqualTo(Reference("x"), literal(34.56)))) == NotEqualTo(Reference("x"), literal(34.56))


def test_rewrite_not_not_equal_to():
    assert rewrite_not(Not(NotEqualTo(Reference("x"), literal(34.56)))) == EqualTo(Reference("x"), literal(34.56))


def test_rewrite_not_in():
    assert rewrite_not(Not(In(Reference("x"), (literal(34.56),)))) == NotIn(Reference("x"), (literal(34.56),))


def test_rewrite_and():
    assert rewrite_not(Not(And(EqualTo(Reference("x"), literal(34.56)), EqualTo(Reference("y"), literal(34.56)),))) == Or(
        NotEqualTo(term=Reference(name="x"), literal=literal(34.56)),
        NotEqualTo(term=Reference(name="y"), literal=literal(34.56)),
    )


def test_rewrite_or():
    assert rewrite_not(Not(Or(EqualTo(Reference("x"), literal(34.56)), EqualTo(Reference("y"), literal(34.56)),))) == And(
        NotEqualTo(term=Reference(name="x"), literal=literal(34.56)),
        NotEqualTo(term=Reference(name="y"), literal=literal(34.56)),
    )


def test_rewrite_always_false():
    assert rewrite_not(Not(AlwaysFalse())) == AlwaysTrue()


def test_rewrite_always_true():
    assert rewrite_not(Not(AlwaysTrue())) == AlwaysFalse()


def test_rewrite_bound():
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert rewrite_not(IsNull(Reference("a")).bind(schema)) == BoundIsNull(
        term=BoundReference(
            field=NestedField(field_id=2, name="a", field_type=IntegerType(), required=False),
            accessor=Accessor(position=0, inner=None),
        )
    )

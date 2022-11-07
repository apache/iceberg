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
    IN_PREDICATE_LIMIT,
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
    LongType,
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


def _create_manifest_evaluator(bound_expr: BoundPredicate) -> _ManifestEvalVisitor:
    """For testing. Creates a bogus evaluator, and then replaces the expression"""
    evaluator = _ManifestEvalVisitor(
        Schema(NestedField(1, "id", LongType())), EqualTo(term=Reference("id"), literal=literal("foo"))
    )
    evaluator.partition_filter = bound_expr
    return evaluator


@pytest.fixture
def string_schema() -> Schema:
    return Schema(NestedField(field_id=1, name="col_str", field_type=StringType(), required=False))


@pytest.fixture
def double_schema() -> Schema:
    return Schema(NestedField(field_id=1, name="col_double", field_type=DoubleType(), required=False))


@pytest.fixture
def long_schema() -> Schema:
    return Schema(NestedField(field_id=1, name="col_long", field_type=LongType(), required=False))


def test_manifest_evaluator_less_than_no_overlap(string_schema: Schema):
    expr = LessThan[str](Reference("col_str"), StringLiteral("c"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_overlap(string_schema: Schema):
    expr = LessThan[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_all_null(string_schema: Schema):
    expr = LessThan[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_no_match(string_schema: Schema):
    expr = LessThan[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_no_overlap(string_schema: Schema):
    expr = LessThanOrEqual[str](Reference("col_str"), StringLiteral("c"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_overlap(string_schema: Schema):
    expr = LessThanOrEqual[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_all_null(string_schema: Schema):
    expr = LessThanOrEqual[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    # All null
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_or_equal_no_match(string_schema: Schema):
    expr = LessThanOrEqual[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_equal_no_overlap(string_schema: Schema):
    expr = EqualTo[str](Reference("col_str"), StringLiteral("c"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_equal_overlap(string_schema: Schema):
    expr = EqualTo[str](Reference("col_str"), StringLiteral("a"))
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
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_equal_all_null(string_schema: Schema):
    expr = EqualTo[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_equal_no_match(string_schema: Schema):
    expr = EqualTo[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_no_overlap(string_schema: Schema):
    expr = GreaterThan[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_overlap(string_schema: Schema):
    expr = GreaterThan[str](Reference("col_str"), StringLiteral("c"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_all_null(string_schema: Schema):
    expr = GreaterThan[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_no_match(string_schema: Schema):
    expr = GreaterThan[str](Reference("col_str"), StringLiteral("d"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_no_overlap(string_schema: Schema):
    expr = GreaterThanOrEqual[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_overlap(string_schema: Schema):
    expr = GreaterThanOrEqual[str](Reference("col_str"), StringLiteral("b"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_all_null(string_schema: Schema):
    expr = GreaterThanOrEqual[str](Reference("col_str"), StringLiteral("a"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_or_equal_no_match(string_schema: Schema):
    expr = GreaterThanOrEqual[str](Reference("col_str"), StringLiteral("d"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "b"),
            upper_bound=_to_byte_buffer(StringType(), "c"),
        )
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_is_nan(double_schema: Schema):
    expr = IsNaN[float](
        Reference("col_double"),
    )
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=True, lower_bound=None, upper_bound=None)
    )

    assert _ManifestEvalVisitor(double_schema, expr).eval(manifest)


def test_manifest_evaluator_is_nan_inverse(double_schema: Schema):
    expr = IsNaN(Reference[float]("col_double"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=False,
            lower_bound=_to_byte_buffer(DoubleType(), 18.15),
            upper_bound=_to_byte_buffer(DoubleType(), 19.25),
        )
    )
    assert not _ManifestEvalVisitor(double_schema, expr).eval(manifest)


def test_manifest_evaluator_not_nan(double_schema: Schema):
    expr = NotNaN(Reference[float]("col_double"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=False,
            contains_nan=False,
            lower_bound=_to_byte_buffer(DoubleType(), 18.15),
            upper_bound=_to_byte_buffer(DoubleType(), 19.25),
        )
    )
    assert _ManifestEvalVisitor(double_schema, expr).eval(manifest)


def test_manifest_evaluator_not_nan_inverse(double_schema: Schema):
    expr = NotNaN(Reference[float]("col_double"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=True, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(double_schema, expr).eval(manifest)


def test_manifest_evaluator_is_null(string_schema: Schema):
    expr = IsNull(Reference[str]("col_str"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=False,
            lower_bound=_to_byte_buffer(StringType(), "a"),
            upper_bound=_to_byte_buffer(StringType(), "b"),
        )
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_is_null_inverse(string_schema: Schema):
    expr = IsNull(Reference[str]("col_str"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=True, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_not_null(string_schema: Schema):
    expr = NotNull(Reference[str]("col_str"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=False, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_not_null_nan(double_schema: Schema):
    expr = NotNull(Reference[float]("col_double"))
    manifest = _to_manifest_file(
        PartitionFieldSummary(contains_null=True, contains_nan=False, lower_bound=None, upper_bound=None)
    )
    assert not _ManifestEvalVisitor(double_schema, expr).eval(manifest)


def test_manifest_evaluator_not_null_inverse(string_schema: Schema):
    expr = NotNull(Reference[str]("col_str"))
    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))
    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_not_equal_to(string_schema: Schema):
    expr = NotEqualTo(Reference[str]("col_str"), StringLiteral("this"))
    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))
    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_in(long_schema: Schema):
    expr = In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_not_in(long_schema: Schema):
    expr = NotIn(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=False,
            upper_bound=False,
        )
    )
    assert _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_in_null(long_schema: Schema):
    expr = In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=None,
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert not _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_in_inverse(long_schema: Schema):
    expr = In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 1815),
            upper_bound=_to_byte_buffer(LongType(), 1925),
        )
    )
    assert not _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_in_overflow(long_schema: Schema):
    expr = In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(IN_PREDICATE_LIMIT + 1)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 1815),
            upper_bound=_to_byte_buffer(LongType(), 1925),
        )
    )
    assert _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_less_than_lower_bound(long_schema: Schema):
    expr = In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(2)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert not _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_greater_than_upper_bound(long_schema: Schema):
    expr = In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(20, 22)}))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert not _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_true(string_schema: Schema):
    expr = AlwaysTrue()

    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))

    assert _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_false(string_schema: Schema):
    expr = AlwaysFalse()

    manifest = _to_manifest_file(PartitionFieldSummary(contains_null=True, contains_nan=True, lower_bound=None, upper_bound=None))

    assert not _ManifestEvalVisitor(string_schema, expr).eval(manifest)


def test_manifest_evaluator_not(long_schema: Schema):
    expr = Not(GreaterThan(Reference[int]("col_long"), LongLiteral(4)))
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert not _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_and(long_schema: Schema):
    expr = And(
        In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)})),
        In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)})),
    )
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_manifest_evaluator_or(long_schema: Schema):
    expr = Or(
        In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)})),
        In(Reference[int]("col_long"), tuple({LongLiteral(i) for i in range(22)})),
    )
    manifest = _to_manifest_file(
        PartitionFieldSummary(
            contains_null=True,
            contains_nan=True,
            lower_bound=_to_byte_buffer(LongType(), 5),
            upper_bound=_to_byte_buffer(LongType(), 10),
        )
    )
    assert _ManifestEvalVisitor(long_schema, expr).eval(manifest)


def test_bound_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="col_long", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            )
        )


def test_bound_unary_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundUnaryPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="col_long", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            )
        )


def test_bound_set_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundSetPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="col_long", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literals={literal("hello"), literal("world")},
        )


def test_bound_literal_predicate_invert():
    with pytest.raises(NotImplementedError):
        _ = ~BoundLiteralPredicate(
            term=BoundReference(
                field=NestedField(field_id=1, name="col_long", field_type=StringType(), required=False),
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

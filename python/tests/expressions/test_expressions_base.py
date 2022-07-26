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
from typing import List

import pytest

from pyiceberg.expression import expressions
from pyiceberg.expression.literals import LongLiteral, StringLiteral, literal
from pyiceberg.schema import Accessor
from pyiceberg.types import IntegerType, NestedField, StringType
from pyiceberg.utils.singleton import Singleton


class ExpressionA(expressions.BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionB()

    def __repr__(self):
        return "ExpressionA()"

    def __str__(self):
        return "testexpra"


class ExpressionB(expressions.BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionA()

    def __repr__(self):
        return "ExpressionB()"

    def __str__(self):
        return "testexprb"


class BooleanExpressionVisitor(expressions.BooleanExpressionVisitor[List]):
    """A test implementation of a BooleanExpressionVisit

    As this visitor visits each node, it appends an element to a `visit_histor` list. This enables testing that a given expression is
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


@expressions.visit.register(ExpressionA)
def _(obj: ExpressionA, visitor: BooleanExpressionVisitor) -> List:
    """Visit a ExpressionA with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_a()


@expressions.visit.register(ExpressionB)
def _(obj: ExpressionB, visitor: BooleanExpressionVisitor) -> List:
    """Visit a ExpressionB with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_b()


@pytest.mark.parametrize(
    "op, rep",
    [
        (
            expressions.And(ExpressionA(), ExpressionB()),
            "And(ExpressionA(), ExpressionB())",
        ),
        (
            expressions.Or(ExpressionA(), ExpressionB()),
            "Or(ExpressionA(), ExpressionB())",
        ),
        (expressions.Not(ExpressionA()), "Not(ExpressionA())"),
    ],
)
def test_reprs(op, rep):
    assert repr(op) == rep


@pytest.mark.parametrize(
    "op, string",
    [
        (expressions.And(ExpressionA(), ExpressionB()), "And(testexpra, testexprb)"),
        (expressions.Or(ExpressionA(), ExpressionB()), "Or(testexpra, testexprb)"),
        (expressions.Not(ExpressionA()), "Not(testexpra)"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


@pytest.mark.parametrize(
    "a,  schema, case_sensitive, success",
    [
        (
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.In(expressions.Reference("not_foo"), literal("hello"), literal("world")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.In(expressions.Reference("Bar"), literal(5), literal(2)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.In(expressions.Reference("Bar"), literal(5), literal(2)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.NotIn(expressions.Reference("foo"), literal("hello"), literal("world")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.NotIn(expressions.Reference("not_foo"), literal("hello"), literal("world")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.NotIn(expressions.Reference("Bar"), literal(5), literal(2)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.NotIn(expressions.Reference("Bar"), literal(5), literal(2)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.NotEq(expressions.Reference("foo"), literal("hello")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.NotEq(expressions.Reference("not_foo"), literal("hello")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.NotEq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.NotEq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.Eq(expressions.Reference("foo"), literal("hello")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.Eq(expressions.Reference("not_foo"), literal("hello")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.Eq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.Eq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.Gt(expressions.Reference("foo"), literal("hello")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.Gt(expressions.Reference("not_foo"), literal("hello")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.Gt(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.Gt(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.Lt(expressions.Reference("foo"), literal("hello")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.Lt(expressions.Reference("not_foo"), literal("hello")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.Lt(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.Lt(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.GtEq(expressions.Reference("foo"), literal("hello")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.GtEq(expressions.Reference("not_foo"), literal("hello")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.GtEq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.GtEq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            True,
            False,
        ),
        (
            expressions.LtEq(expressions.Reference("foo"), literal("hello")),
            "table_schema_simple",
            True,
            True,
        ),
        (
            expressions.LtEq(expressions.Reference("not_foo"), literal("hello")),
            "table_schema_simple",
            False,
            False,
        ),
        (
            expressions.LtEq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            False,
            True,
        ),
        (
            expressions.LtEq(expressions.Reference("Bar"), literal(5)),
            "table_schema_simple",
            True,
            False,
        ),
    ],
)
def test_bind(a, schema, case_sensitive, success, request):
    schema = request.getfixturevalue(schema)
    if success:
        assert a.bind(schema, case_sensitive).term.field == schema.find_field(a.term.name, case_sensitive)
    else:
        with pytest.raises(ValueError):
            a.bind(schema, case_sensitive)


@pytest.mark.parametrize(
    "exp, testexpra, testexprb",
    [
        (
            expressions.And(ExpressionA(), ExpressionB()),
            expressions.And(ExpressionA(), ExpressionB()),
            expressions.Or(ExpressionA(), ExpressionB()),
        ),
        (
            expressions.Or(ExpressionA(), ExpressionB()),
            expressions.Or(ExpressionA(), ExpressionB()),
            expressions.And(ExpressionA(), ExpressionB()),
        ),
        (expressions.Not(ExpressionA()), expressions.Not(ExpressionA()), ExpressionB()),
        (ExpressionA(), ExpressionA(), ExpressionB()),
        (ExpressionB(), ExpressionB(), ExpressionA()),
        (
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
            expressions.In(expressions.Reference("not_foo"), literal("hello"), literal("world")),
        ),
        (
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
            expressions.In(expressions.Reference("foo"), literal("goodbye"), literal("world")),
        ),
    ],
)
def test_eq(exp, testexpra, testexprb):
    assert exp == testexpra and exp != testexprb


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            expressions.And(ExpressionA(), ExpressionB()),
            expressions.Or(ExpressionB(), ExpressionA()),
        ),
        (
            expressions.Or(ExpressionA(), ExpressionB()),
            expressions.And(ExpressionB(), ExpressionA()),
        ),
        (
            expressions.Not(ExpressionA()),
            ExpressionA(),
        ),
        (
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
            expressions.NotIn(expressions.Reference("foo"), literal("hello"), literal("world")),
        ),
        (
            expressions.NotIn(expressions.Reference("foo"), literal("hello"), literal("world")),
            expressions.In(expressions.Reference("foo"), literal("hello"), literal("world")),
        ),
        (expressions.Gt(expressions.Reference("foo"), literal(5)), expressions.LtEq(expressions.Reference("foo"), literal(5))),
        (expressions.Lt(expressions.Reference("foo"), literal(5)), expressions.GtEq(expressions.Reference("foo"), literal(5))),
        (expressions.Eq(expressions.Reference("foo"), literal(5)), expressions.NotEq(expressions.Reference("foo"), literal(5))),
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
            expressions.And(ExpressionA(), ExpressionB(), ExpressionA()),
            expressions.And(expressions.And(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (
            expressions.Or(ExpressionA(), ExpressionB(), ExpressionA()),
            expressions.Or(expressions.Or(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (expressions.Not(expressions.Not(ExpressionA())), ExpressionA()),
    ],
)
def test_reduce(lhs, rhs):
    assert lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (expressions.And(expressions.AlwaysTrue(), ExpressionB()), ExpressionB()),
        (expressions.And(expressions.AlwaysFalse(), ExpressionB()), expressions.AlwaysFalse()),
        (expressions.Or(expressions.AlwaysTrue(), ExpressionB()), expressions.AlwaysTrue()),
        (expressions.Or(expressions.AlwaysFalse(), ExpressionB()), ExpressionB()),
        (expressions.Not(expressions.Not(ExpressionA())), ExpressionA()),
    ],
)
def test_base_AlwaysTrue_base_AlwaysFalse(lhs, rhs):
    assert lhs == rhs


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

    assert expressions.Accessor(position=0).get(foo_struct) == "foo"
    assert expressions.Accessor(position=1).get(foo_struct) == "bar"
    assert expressions.Accessor(position=2).get(foo_struct) == "baz"
    assert expressions.Accessor(position=3).get(foo_struct) == 1
    assert expressions.Accessor(position=4).get(foo_struct) == 2
    assert expressions.Accessor(position=5).get(foo_struct) == 3
    assert expressions.Accessor(position=6).get(foo_struct) == 1.234
    assert expressions.Accessor(position=7).get(foo_struct) == Decimal("1.234")
    assert expressions.Accessor(position=8).get(foo_struct) == uuid_value
    assert expressions.Accessor(position=9).get(foo_struct) is True
    assert expressions.Accessor(position=10).get(foo_struct) is False
    assert expressions.Accessor(position=11).get(foo_struct) == b"\x19\x04\x9e?"


def test_bound_reference_str_and_repr():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = expressions.Accessor(position=1)
    bound_ref = expressions.BoundReference(field=field, accessor=position1_accessor)
    assert str(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"
    assert repr(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"


def test_bound_reference_field_property():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = expressions.Accessor(position=1)
    bound_ref = expressions.BoundReference(field=field, accessor=position1_accessor)
    assert bound_ref.field == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


def test_bound_reference(table_schema_simple, foo_struct):
    """Test creating a BoundReference and evaluating it on a StructProtocol"""
    foo_struct.set(pos=1, value="foovalue")
    foo_struct.set(pos=2, value=123)
    foo_struct.set(pos=3, value=True)

    position1_accessor = expressions.Accessor(position=1)
    position2_accessor = expressions.Accessor(position=2)
    position3_accessor = expressions.Accessor(position=3)

    field1 = table_schema_simple.find_field(1)
    field2 = table_schema_simple.find_field(2)
    field3 = table_schema_simple.find_field(3)

    bound_ref1 = expressions.BoundReference(field=field1, accessor=position1_accessor)
    bound_ref2 = expressions.BoundReference(field=field2, accessor=position2_accessor)
    bound_ref3 = expressions.BoundReference(field=field3, accessor=position3_accessor)

    assert bound_ref1.eval(foo_struct) == "foovalue"
    assert bound_ref2.eval(foo_struct) == 123
    assert bound_ref3.eval(foo_struct) is True


def test_boolean_expression_visitor():
    """Test post-order traversal of boolean expression visit method"""
    expr = expressions.And(
        expressions.Or(expressions.Not(ExpressionA()), expressions.Not(ExpressionB()), ExpressionA(), ExpressionB()),
        expressions.Not(ExpressionA()),
        ExpressionB(),
    )
    visitor = BooleanExpressionVisitor()
    result = expressions.visit(expr, visitor=visitor)
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
    visitor = BooleanExpressionVisitor()
    with pytest.raises(NotImplementedError) as exc_info:
        expressions.visit("foo", visitor=visitor)

    assert str(exc_info.value) == "Cannot visit unsupported expression: foo"


def test_always_true_expression_binding(table_schema_simple):
    """Test that visiting an always-true expression returns always-true"""
    unbound_expression = expressions.AlwaysTrue()
    bound_expression = expressions.visit(unbound_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expressions.AlwaysTrue()


def test_always_false_expression_binding(table_schema_simple):
    """Test that visiting an always-false expression returns always-false"""
    unbound_expression = expressions.AlwaysFalse()
    bound_expression = expressions.visit(unbound_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expressions.AlwaysFalse()


def test_always_false_and_always_true_expression_binding(table_schema_simple):
    """Test that visiting both an always-true AND always-false expression returns always-false"""
    unbound_expression = expressions.And(expressions.AlwaysTrue(), expressions.AlwaysFalse())
    bound_expression = expressions.visit(unbound_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expressions.AlwaysFalse()


def test_always_false_or_always_true_expression_binding(table_schema_simple):
    """Test that visiting always-true OR always-false expression returns always-true"""
    unbound_expression = expressions.Or(expressions.AlwaysTrue(), expressions.AlwaysFalse())
    bound_expression = expressions.visit(unbound_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expressions.AlwaysTrue()


@pytest.mark.parametrize(
    "unbound_and_expression,expected_bound_expression",
    [
        (
            expressions.And(
                expressions.In(expressions.Reference("foo"), literal("foo"), literal("bar")),
                expressions.In(expressions.Reference("bar"), literal(1), literal(2), literal(3)),
            ),
            expressions.And(
                expressions.BoundIn[str](
                    expressions.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    StringLiteral("foo"),
                    StringLiteral("bar"),
                ),
                expressions.BoundIn[int](
                    expressions.BoundReference(
                        field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                        accessor=Accessor(position=1, inner=None),
                    ),
                    LongLiteral(1),
                    LongLiteral(2),
                    LongLiteral(3),
                ),
            ),
        ),
        (
            expressions.And(
                expressions.In(expressions.Reference("foo"), literal("bar"), literal("baz")),
                expressions.In(
                    expressions.Reference("bar"),
                    literal(1),
                ),
                expressions.In(
                    expressions.Reference("foo"),
                    literal("baz"),
                ),
            ),
            expressions.And(
                expressions.And(
                    expressions.BoundIn[str](
                        expressions.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        StringLiteral("bar"),
                        StringLiteral("baz"),
                    ),
                    expressions.BoundIn[int](
                        expressions.BoundReference(
                            field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                            accessor=Accessor(position=1, inner=None),
                        ),
                        LongLiteral(1),
                    ),
                ),
                expressions.BoundIn[str](
                    expressions.BoundReference(
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
    bound_expression = expressions.visit(unbound_and_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_or_expression,expected_bound_expression",
    [
        (
            expressions.Or(
                expressions.In(expressions.Reference("foo"), literal("foo"), literal("bar")),
                expressions.In(expressions.Reference("bar"), literal(1), literal(2), literal(3)),
            ),
            expressions.Or(
                expressions.BoundIn[str](
                    expressions.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    StringLiteral("foo"),
                    StringLiteral("bar"),
                ),
                expressions.BoundIn[int](
                    expressions.BoundReference(
                        field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                        accessor=Accessor(position=1, inner=None),
                    ),
                    LongLiteral(1),
                    LongLiteral(2),
                    LongLiteral(3),
                ),
            ),
        ),
        (
            expressions.Or(
                expressions.In(expressions.Reference("foo"), literal("bar"), literal("baz")),
                expressions.In(
                    expressions.Reference("foo"),
                    literal("bar"),
                ),
                expressions.In(
                    expressions.Reference("foo"),
                    literal("baz"),
                ),
            ),
            expressions.Or(
                expressions.Or(
                    expressions.BoundIn[str](
                        expressions.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        StringLiteral("bar"),
                        StringLiteral("baz"),
                    ),
                    expressions.BoundIn[str](
                        expressions.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        StringLiteral("bar"),
                    ),
                ),
                expressions.BoundIn[str](
                    expressions.BoundReference(
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    StringLiteral("baz"),
                ),
            ),
        ),
        (
            expressions.Or(
                expressions.AlwaysTrue(),
                expressions.AlwaysFalse(),
            ),
            expressions.AlwaysTrue(),
        ),
        (
            expressions.Or(
                expressions.AlwaysTrue(),
                expressions.AlwaysTrue(),
            ),
            expressions.AlwaysTrue(),
        ),
        (
            expressions.Or(
                expressions.AlwaysFalse(),
                expressions.AlwaysFalse(),
            ),
            expressions.AlwaysFalse(),
        ),
    ],
)
def test_or_expression_binding(unbound_or_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound OR expression with a bind-visitor returns the expected bound expression"""
    bound_expression = expressions.visit(unbound_or_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_in_expression,expected_bound_expression",
    [
        (
            expressions.In(expressions.Reference("foo"), literal("foo"), literal("bar")),
            expressions.BoundIn[str](
                expressions.BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                StringLiteral("foo"),
                StringLiteral("bar"),
            ),
        ),
        (
            expressions.In(expressions.Reference("foo"), literal("bar"), literal("baz")),
            expressions.BoundIn[str](
                expressions.BoundReference[str](
                    field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                    accessor=Accessor(position=0, inner=None),
                ),
                StringLiteral("bar"),
                StringLiteral("baz"),
            ),
        ),
        (
            expressions.In(
                expressions.Reference("foo"),
                literal("bar"),
            ),
            expressions.BoundIn[str](
                expressions.BoundReference[str](
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
    bound_expression = expressions.visit(unbound_in_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression


@pytest.mark.parametrize(
    "unbound_not_expression,expected_bound_expression",
    [
        (
            expressions.Not(expressions.In(expressions.Reference("foo"), literal("foo"), literal("bar"))),
            expressions.Not(
                expressions.BoundIn[str](
                    expressions.BoundReference[str](
                        field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                        accessor=Accessor(position=0, inner=None),
                    ),
                    StringLiteral("foo"),
                    StringLiteral("bar"),
                )
            ),
        ),
        (
            expressions.Not(
                expressions.Or(
                    expressions.In(expressions.Reference("foo"), literal("foo"), literal("bar")),
                    expressions.In(expressions.Reference("foo"), literal("foo"), literal("bar"), literal("baz")),
                )
            ),
            expressions.Not(
                expressions.Or(
                    expressions.BoundIn[str](
                        expressions.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        StringLiteral("foo"),
                        StringLiteral("bar"),
                    ),
                    expressions.BoundIn[str](
                        expressions.BoundReference(
                            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                            accessor=Accessor(position=0, inner=None),
                        ),
                        StringLiteral("foo"),
                        StringLiteral("bar"),
                        StringLiteral("baz"),
                    ),
                ),
            ),
        ),
    ],
)
def test_not_expression_binding(unbound_not_expression, expected_bound_expression, table_schema_simple):
    """Test that visiting an unbound NOT expression with a bind-visitor returns the expected bound expression"""
    bound_expression = expressions.visit(unbound_not_expression, visitor=expressions.BindVisitor(schema=table_schema_simple))
    assert bound_expression == expected_bound_expression

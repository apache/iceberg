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

from pyiceberg.expressions import base
from pyiceberg.expressions.literals import LongLiteral, StringLiteral, literal
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IntegerType,
    NestedField,
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


class BooleanExpressionVisitor(base.BooleanExpressionVisitor[List]):
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


@base.visit.register(ExpressionA)
def _(obj: ExpressionA, visitor: BooleanExpressionVisitor) -> List:
    """Visit a ExpressionA with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_a()


@base.visit.register(ExpressionB)
def _(obj: ExpressionB, visitor: BooleanExpressionVisitor) -> List:
    """Visit a ExpressionB with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_b()


@pytest.mark.parametrize(
    "op, rep",
    [
        (
            base.And(ExpressionA(), ExpressionB()),
            "And(ExpressionA(), ExpressionB())",
        ),
        (
            base.Or(ExpressionA(), ExpressionB()),
            "Or(ExpressionA(), ExpressionB())",
        ),
        (base.Not(ExpressionA()), "Not(ExpressionA())"),
    ],
)
def test_reprs(op, rep):
    assert repr(op) == rep


def test_isnull_inverse():
    assert ~base.IsNull(base.Reference("a")) == base.NotNull(base.Reference("a"))


def test_isnull_bind():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = base.BoundIsNull(base.BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert base.IsNull(base.Reference("a")).bind(schema) == bound


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
        (base.And(ExpressionA(), ExpressionB()), "And(testexpra, testexprb)"),
        (base.Or(ExpressionA(), ExpressionB()), "Or(testexpra, testexprb)"),
        (base.Not(ExpressionA()), "Not(testexpra)"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


def test_ref_binding_case_sensitive(request):
    schema = request.getfixturevalue("table_schema_simple")
    ref = base.Reference("foo")
    bound = base.BoundReference(schema.find_field(1), schema.accessor_for_field(1))
    assert ref.bind(schema, case_sensitive=True) == bound


def test_ref_binding_case_sensitive_failure(request):
    schema = request.getfixturevalue("table_schema_simple")
    ref = base.Reference("Foo")
    with pytest.raises(ValueError):
        ref.bind(schema, case_sensitive=True)


def test_ref_binding_case_insensitive(request):
    schema = request.getfixturevalue("table_schema_simple")
    ref = base.Reference("Foo")
    bound = base.BoundReference(schema.find_field(1), schema.accessor_for_field(1))
    assert ref.bind(schema, case_sensitive=False) == bound


def test_ref_binding_case_insensitive_failure(request):
    schema = request.getfixturevalue("table_schema_simple")
    ref = base.Reference("Foot")
    with pytest.raises(ValueError):
        ref.bind(schema, case_sensitive=False)


def test_in_to_eq():
    assert base.In(base.Reference("x"), (literal(34.56),)) == base.Eq(base.Reference("x"), literal(34.56))


def test_bind_in(request):
    schema = request.getfixturevalue("table_schema_simple")
    bound = base.BoundIn(
        base.BoundReference(schema.find_field(1), schema.accessor_for_field(1)), {literal("hello"), literal("world")}
    )
    assert base.In(base.Reference("foo"), (literal("hello"), literal("world"))).bind(schema) == bound


def test_bind_dedup(request):
    schema = request.getfixturevalue("table_schema_simple")
    bound = base.BoundIn(
        base.BoundReference(schema.find_field(1), schema.accessor_for_field(1)), {literal("hello"), literal("world")}
    )
    assert base.In(base.Reference("foo"), (literal("hello"), literal("world"), literal("world"))).bind(schema) == bound


def test_bind_dedup_to_eq(request):
    schema = request.getfixturevalue("table_schema_simple")
    bound = base.BoundEq(base.BoundReference(schema.find_field(1), schema.accessor_for_field(1)), literal("hello"))
    assert base.In(base.Reference("foo"), (literal("hello"), literal("hello"))).bind(schema) == bound


@pytest.mark.parametrize(
    "a,  schema",
    [
        (
            base.NotIn(base.Reference("foo"), (literal("hello"), literal("world"))),
            "table_schema_simple",
        ),
        (
            base.NotEq(base.Reference("foo"), literal("hello")),
            "table_schema_simple",
        ),
        (
            base.Eq(base.Reference("foo"), literal("hello")),
            "table_schema_simple",
        ),
        (
            base.Gt(base.Reference("foo"), literal("hello")),
            "table_schema_simple",
        ),
        (
            base.Lt(base.Reference("foo"), literal("hello")),
            "table_schema_simple",
        ),
        (
            base.GtEq(base.Reference("foo"), literal("hello")),
            "table_schema_simple",
        ),
        (
            base.LtEq(base.Reference("foo"), literal("hello")),
            "table_schema_simple",
        ),
    ],
)
def test_bind(a, schema, request):
    schema = request.getfixturevalue(schema)
    assert a.bind(schema, case_sensitive=True).term.field == schema.find_field(a.term.name, case_sensitive=True)


@pytest.mark.parametrize(
    "a,  schema",
    [
        (
            base.In(base.Reference("Bar"), (literal(5), literal(2))),
            "table_schema_simple",
        ),
        (
            base.NotIn(base.Reference("Bar"), (literal(5), literal(2))),
            "table_schema_simple",
        ),
        (
            base.NotEq(base.Reference("Bar"), literal(5)),
            "table_schema_simple",
        ),
        (
            base.Eq(base.Reference("Bar"), literal(5)),
            "table_schema_simple",
        ),
        (
            base.Gt(base.Reference("Bar"), literal(5)),
            "table_schema_simple",
        ),
        (
            base.Lt(base.Reference("Bar"), literal(5)),
            "table_schema_simple",
        ),
        (
            base.GtEq(base.Reference("Bar"), literal(5)),
            "table_schema_simple",
        ),
        (
            base.LtEq(base.Reference("Bar"), literal(5)),
            "table_schema_simple",
        ),
    ],
)
def test_bind_case_insensitive(a, schema, request):
    schema = request.getfixturevalue(schema)
    assert a.bind(schema, case_sensitive=False).term.field == schema.find_field(a.term.name, case_sensitive=False)


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
        (base.Gt(base.Reference("foo"), literal(5)), base.LtEq(base.Reference("foo"), literal(5))),
        (base.Lt(base.Reference("foo"), literal(5)), base.GtEq(base.Reference("foo"), literal(5))),
        (base.Eq(base.Reference("foo"), literal(5)), base.NotEq(base.Reference("foo"), literal(5))),
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
        (base.Or(base.AlwaysTrue(), ExpressionB()), base.AlwaysTrue()),
        (base.Or(base.AlwaysFalse(), ExpressionB()), ExpressionB()),
        (base.Not(base.Not(ExpressionA())), ExpressionA()),
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
    visitor = BooleanExpressionVisitor()
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
    visitor = BooleanExpressionVisitor()
    with pytest.raises(NotImplementedError) as exc_info:
        base.visit("foo", visitor=visitor)

    assert str(exc_info.value) == "Cannot visit unsupported expression: foo"


def test_always_true_expression_binding(table_schema_simple):
    """Test that visiting an always-true expression returns always-true"""
    unbound_expression = base.AlwaysTrue()
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysTrue()


def test_always_false_expression_binding(table_schema_simple):
    """Test that visiting an always-false expression returns always-false"""
    unbound_expression = base.AlwaysFalse()
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysFalse()


def test_always_false_and_always_true_expression_binding(table_schema_simple):
    """Test that visiting both an always-true AND always-false expression returns always-false"""
    unbound_expression = base.And(base.AlwaysTrue(), base.AlwaysFalse())
    bound_expression = base.visit(unbound_expression, visitor=base.BindVisitor(schema=table_schema_simple))
    assert bound_expression == base.AlwaysFalse()


def test_always_false_or_always_true_expression_binding(table_schema_simple):
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
                    base.BoundEq[int](
                        base.BoundReference(
                            field=NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
                            accessor=Accessor(position=1, inner=None),
                        ),
                        LongLiteral(1),
                    ),
                ),
                base.BoundEq[str](
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
            base.BoundEq(
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

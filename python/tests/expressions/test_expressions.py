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

import pytest

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
from pyiceberg.expressions.literals import StringLiteral, literal
from pyiceberg.expressions.visitors import _from_byte_buffer
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
)
from tests.expressions.test_visitors import ExpressionA, ExpressionB


@pytest.mark.parametrize(
    "op, rep",
    [
        (
            And(ExpressionA(), ExpressionB()),
            "And(left=ExpressionA(), right=ExpressionB())",
        ),
        (
            Or(ExpressionA(), ExpressionB()),
            "Or(left=ExpressionA(), right=ExpressionB())",
        ),
        (Not(ExpressionA()), "Not(child=ExpressionA())"),
    ],
)
def test_reprs(op: BooleanExpression, rep: str):
    assert repr(op) == rep


def test_isnull_inverse():
    assert ~IsNull(Reference("a")) == NotNull(Reference("a"))


def test_isnull_bind():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = BoundIsNull(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNull(Reference("a")).bind(schema) == bound


def test_invert_is_null_bind():
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert ~IsNull(Reference("a")).bind(schema) == NotNull(Reference("a")).bind(schema)


def test_invert_not_null_bind():
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert ~NotNull(Reference("a")).bind(schema) == IsNull(Reference("a")).bind(schema)


def test_invert_is_nan_bind():
    schema = Schema(NestedField(2, "a", DoubleType(), required=False), schema_id=1)
    assert ~IsNaN(Reference("a")).bind(schema) == NotNaN(Reference("a")).bind(schema)


def test_invert_not_nan_bind():
    schema = Schema(NestedField(2, "a", DoubleType(), required=False), schema_id=1)
    assert ~NotNaN(Reference("a")).bind(schema) == IsNaN(Reference("a")).bind(schema)


def test_bind_expr_does_not_exists():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    with pytest.raises(ValueError) as exc_info:
        IsNull(Reference("b")).bind(schema)

    assert str(exc_info.value) == "Could not find field with name b, case_sensitive=True"


def test_bind_does_not_exists():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    with pytest.raises(ValueError) as exc_info:
        Reference("b").bind(schema)

    assert str(exc_info.value) == "Could not find field with name b, case_sensitive=True"


def test_isnull_bind_required():
    schema = Schema(NestedField(2, "a", IntegerType(), required=True), schema_id=1)
    assert IsNull(Reference("a")).bind(schema) == AlwaysFalse()


def test_notnull_inverse():
    assert ~NotNull(Reference("a")) == IsNull(Reference("a"))


def test_notnull_bind():
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = BoundNotNull(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert NotNull(Reference("a")).bind(schema) == bound


def test_notnull_bind_required():
    schema = Schema(NestedField(2, "a", IntegerType(), required=True), schema_id=1)
    assert NotNull(Reference("a")).bind(schema) == AlwaysTrue()


def test_isnan_inverse():
    assert ~IsNaN(Reference("f")) == NotNaN(Reference("f"))


def test_isnan_bind_float():
    schema = Schema(NestedField(2, "f", FloatType()), schema_id=1)
    bound = BoundIsNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNaN(Reference("f")).bind(schema) == bound


def test_isnan_bind_double():
    schema = Schema(NestedField(2, "d", DoubleType()), schema_id=1)
    bound = BoundIsNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNaN(Reference("d")).bind(schema) == bound


def test_isnan_bind_nonfloat():
    schema = Schema(NestedField(2, "i", IntegerType()), schema_id=1)
    assert IsNaN(Reference("i")).bind(schema) == AlwaysFalse()


def test_notnan_inverse():
    assert ~NotNaN(Reference("f")) == IsNaN(Reference("f"))


def test_notnan_bind_float():
    schema = Schema(NestedField(2, "f", FloatType()), schema_id=1)
    bound = BoundNotNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert NotNaN(Reference("f")).bind(schema) == bound


def test_notnan_bind_double():
    schema = Schema(NestedField(2, "d", DoubleType()), schema_id=1)
    bound = BoundNotNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert NotNaN(Reference("d")).bind(schema) == bound


def test_notnan_bind_nonfloat():
    schema = Schema(NestedField(2, "i", IntegerType()), schema_id=1)
    assert NotNaN(Reference("i")).bind(schema) == AlwaysTrue()


@pytest.mark.parametrize(
    "op, string",
    [
        (And(ExpressionA(), ExpressionB()), "And(left=ExpressionA(), right=ExpressionB())"),
        (Or(ExpressionA(), ExpressionB()), "Or(left=ExpressionA(), right=ExpressionB())"),
        (Not(ExpressionA()), "Not(child=ExpressionA())"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


def test_ref_binding_case_sensitive(table_schema_simple: Schema):
    ref = Reference[str]("foo")
    bound = BoundReference[str](table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))
    assert ref.bind(table_schema_simple, case_sensitive=True) == bound


def test_ref_binding_case_sensitive_failure(table_schema_simple: Schema):
    ref = Reference[str]("Foo")
    with pytest.raises(ValueError):
        ref.bind(table_schema_simple, case_sensitive=True)


def test_ref_binding_case_insensitive(table_schema_simple: Schema):
    ref = Reference[str]("Foo")
    bound = BoundReference[str](table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))
    assert ref.bind(table_schema_simple, case_sensitive=False) == bound


def test_ref_binding_case_insensitive_failure(table_schema_simple: Schema):
    ref = Reference[str]("Foot")
    with pytest.raises(ValueError):
        ref.bind(table_schema_simple, case_sensitive=False)


def test_in_to_eq():
    assert In(Reference("x"), (literal(34.56),)) == EqualTo(Reference("x"), literal(34.56))


def test_empty_bind_in(table_schema_simple: Schema):
    bound = BoundIn[str](BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), set())
    assert bound == AlwaysFalse()


def test_empty_bind_not_in(table_schema_simple: Schema):
    bound = BoundNotIn(BoundReference[str](table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), set())
    assert bound == AlwaysTrue()


def test_bind_not_in_equal_term(table_schema_simple: Schema):
    bound = BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), {literal("hello")}
    )
    assert (
        BoundNotEqualTo[str](
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literal=StringLiteral("hello"),
        )
        == bound
    )


def test_in_empty():
    assert In(Reference("foo"), ()) == AlwaysFalse()


def test_not_in_empty():
    assert NotIn(Reference("foo"), ()) == AlwaysTrue()


def test_not_in_equal():
    assert NotIn(Reference("foo"), (literal("hello"),)) == NotEqualTo(term=Reference(name="foo"), literal=StringLiteral("hello"))


def test_bind_in(table_schema_simple: Schema):
    bound = BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert In(Reference("foo"), (literal("hello"), literal("world"))).bind(table_schema_simple) == bound


def test_bind_in_invert(table_schema_simple: Schema):
    bound = BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_not_in_invert(table_schema_simple: Schema):
    bound = BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_dedup(table_schema_simple: Schema):
    bound = BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert In(Reference("foo"), (literal("hello"), literal("world"), literal("world"))).bind(table_schema_simple) == bound


def test_bind_dedup_to_eq(table_schema_simple: Schema):
    bound = BoundEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert In(Reference("foo"), (literal("hello"), literal("hello"))).bind(table_schema_simple) == bound


def test_bound_equal_to_invert(table_schema_simple: Schema):
    bound = BoundEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundNotEqualTo(
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_not_equal_to_invert(table_schema_simple: Schema):
    bound = BoundNotEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundEqualTo(
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_greater_than_or_equal_invert(table_schema_simple: Schema):
    bound = BoundGreaterThanOrEqual(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundLessThan(
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_greater_than_invert(table_schema_simple: Schema):
    bound = BoundGreaterThan(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundLessThanOrEqual(
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_less_than_invert(table_schema_simple: Schema):
    bound = BoundLessThan(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundGreaterThanOrEqual(
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_less_than_or_equal_invert(table_schema_simple: Schema):
    bound = BoundLessThanOrEqual(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundGreaterThan(
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_not_equal_to_invert():
    bound = NotEqualTo(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )
    assert ~bound == EqualTo(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_greater_than_or_equal_invert():
    bound = GreaterThanOrEqual(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )
    assert ~bound == LessThan(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_less_than_or_equal_invert():
    bound = LessThanOrEqual(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )
    assert ~bound == GreaterThan(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


@pytest.mark.parametrize(
    "pred",
    [
        NotIn(Reference("foo"), (literal("hello"), literal("world"))),
        NotEqualTo(Reference("foo"), literal("hello")),
        EqualTo(Reference("foo"), literal("hello")),
        GreaterThan(Reference("foo"), literal("hello")),
        LessThan(Reference("foo"), literal("hello")),
        GreaterThanOrEqual(Reference("foo"), literal("hello")),
        LessThanOrEqual(Reference("foo"), literal("hello")),
    ],
)
def test_bind(pred, table_schema_simple: Schema):
    assert pred.bind(table_schema_simple, case_sensitive=True).term.field == table_schema_simple.find_field(
        pred.term.name, case_sensitive=True
    )


@pytest.mark.parametrize(
    "pred",
    [
        In(Reference("Bar"), (literal(5), literal(2))),
        NotIn(Reference("Bar"), (literal(5), literal(2))),
        NotEqualTo(Reference("Bar"), literal(5)),
        EqualTo(Reference("Bar"), literal(5)),
        GreaterThan(Reference("Bar"), literal(5)),
        LessThan(Reference("Bar"), literal(5)),
        GreaterThanOrEqual(Reference("Bar"), literal(5)),
        LessThanOrEqual(Reference("Bar"), literal(5)),
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
            And(ExpressionA(), ExpressionB()),
            And(ExpressionA(), ExpressionB()),
            Or(ExpressionA(), ExpressionB()),
        ),
        (
            Or(ExpressionA(), ExpressionB()),
            Or(ExpressionA(), ExpressionB()),
            And(ExpressionA(), ExpressionB()),
        ),
        (Not(ExpressionA()), Not(ExpressionA()), ExpressionB()),
        (ExpressionA(), ExpressionA(), ExpressionB()),
        (ExpressionB(), ExpressionB(), ExpressionA()),
        (
            In(Reference("foo"), (literal("hello"), literal("world"))),
            In(Reference("foo"), (literal("hello"), literal("world"))),
            In(Reference("not_foo"), (literal("hello"), literal("world"))),
        ),
        (
            In(Reference("foo"), (literal("hello"), literal("world"))),
            In(Reference("foo"), (literal("hello"), literal("world"))),
            In(Reference("foo"), (literal("goodbye"), literal("world"))),
        ),
    ],
)
def test_eq(exp, testexpra, testexprb):
    assert exp == testexpra and exp != testexprb


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            And(ExpressionA(), ExpressionB()),
            Or(ExpressionB(), ExpressionA()),
        ),
        (
            Or(ExpressionA(), ExpressionB()),
            And(ExpressionB(), ExpressionA()),
        ),
        (
            Not(ExpressionA()),
            ExpressionA(),
        ),
        (
            In(Reference("foo"), (literal("hello"), literal("world"))),
            NotIn(Reference("foo"), (literal("hello"), literal("world"))),
        ),
        (
            NotIn(Reference("foo"), (literal("hello"), literal("world"))),
            In(Reference("foo"), (literal("hello"), literal("world"))),
        ),
        (GreaterThan(Reference("foo"), literal(5)), LessThanOrEqual(Reference("foo"), literal(5))),
        (LessThan(Reference("foo"), literal(5)), GreaterThanOrEqual(Reference("foo"), literal(5))),
        (EqualTo(Reference("foo"), literal(5)), NotEqualTo(Reference("foo"), literal(5))),
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
            And(ExpressionA(), ExpressionB(), ExpressionA()),
            And(And(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (
            Or(ExpressionA(), ExpressionB(), ExpressionA()),
            Or(Or(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (Not(Not(ExpressionA())), ExpressionA()),
    ],
)
def test_reduce(lhs, rhs):
    assert lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (And(AlwaysTrue(), ExpressionB()), ExpressionB()),
        (And(AlwaysFalse(), ExpressionB()), AlwaysFalse()),
        (And(ExpressionB(), AlwaysTrue()), ExpressionB()),
        (Or(AlwaysTrue(), ExpressionB()), AlwaysTrue()),
        (Or(AlwaysFalse(), ExpressionB()), ExpressionB()),
        (Or(ExpressionA(), AlwaysFalse()), ExpressionA()),
        (Not(Not(ExpressionA())), ExpressionA()),
        (Not(AlwaysTrue()), AlwaysFalse()),
        (Not(AlwaysFalse()), AlwaysTrue()),
    ],
)
def test_base_AlwaysTrue_base_AlwaysFalse(lhs, rhs):
    assert lhs == rhs


def test_invert_always():
    assert ~AlwaysFalse() == AlwaysTrue()
    assert ~AlwaysTrue() == AlwaysFalse()


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

    assert Accessor(position=0).get(foo_struct) == "foo"
    assert Accessor(position=1).get(foo_struct) == "bar"
    assert Accessor(position=2).get(foo_struct) == "baz"
    assert Accessor(position=3).get(foo_struct) == 1
    assert Accessor(position=4).get(foo_struct) == 2
    assert Accessor(position=5).get(foo_struct) == 3
    assert Accessor(position=6).get(foo_struct) == 1.234
    assert Accessor(position=7).get(foo_struct) == Decimal("1.234")
    assert Accessor(position=8).get(foo_struct) == uuid_value
    assert Accessor(position=9).get(foo_struct) is True
    assert Accessor(position=10).get(foo_struct) is False
    assert Accessor(position=11).get(foo_struct) == b"\x19\x04\x9e?"


def test_bound_reference_str_and_repr():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = Accessor(position=1)
    bound_ref = BoundReference(field=field, accessor=position1_accessor)
    assert str(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"
    assert repr(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"


def test_bound_reference_field_property():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = Accessor(position=1)
    bound_ref = BoundReference(field=field, accessor=position1_accessor)
    assert bound_ref.field == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


def test_bound_reference(table_schema_simple, foo_struct):
    """Test creating a BoundReference and evaluating it on a StructProtocol"""
    foo_struct.set(pos=1, value="foovalue")
    foo_struct.set(pos=2, value=123)
    foo_struct.set(pos=3, value=True)

    position1_accessor = Accessor(position=1)
    position2_accessor = Accessor(position=2)
    position3_accessor = Accessor(position=3)

    field1 = table_schema_simple.find_field(1)
    field2 = table_schema_simple.find_field(2)
    field3 = table_schema_simple.find_field(3)

    bound_ref1 = BoundReference(field=field1, accessor=position1_accessor)
    bound_ref2 = BoundReference(field=field2, accessor=position2_accessor)
    bound_ref3 = BoundReference(field=field3, accessor=position3_accessor)

    assert bound_ref1.eval(foo_struct) == "foovalue"
    assert bound_ref2.eval(foo_struct) == 123
    assert bound_ref3.eval(foo_struct) is True


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


def test_non_primitive_from_byte_buffer():
    with pytest.raises(ValueError) as exc_info:
        _ = _from_byte_buffer(ListType(element_id=1, element_type=StringType()), b"\0x00")

    assert str(exc_info.value) == "Expected a PrimitiveType, got: <class 'pyiceberg.types.ListType'>"


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

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
# pylint:disable=redefined-outer-name,eval-used

import uuid
from decimal import Decimal

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
    BoundReference,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    Or,
    Reference,
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
from tests.conftest import FooStruct
from tests.expressions.test_visitors import ExpressionA, ExpressionB


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
    bound = BoundNotIn[str](
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


def test_in_set():
    assert In(Reference("foo"), {"a", "bc", "def"}).literals == {StringLiteral("a"), StringLiteral("bc"), StringLiteral("def")}


def test_in_tuple():
    assert In(Reference("foo"), ("a", "bc", "def")).literals == {StringLiteral("a"), StringLiteral("bc"), StringLiteral("def")}


def test_in_list():
    assert In(Reference("foo"), ["a", "bc", "def"]).literals == {StringLiteral("a"), StringLiteral("bc"), StringLiteral("def")}


def test_single_string():
    assert In(Reference("foo"), "a").literal == StringLiteral("a")
    assert In(Reference("foo"), {"a"}).literal == StringLiteral("a")


def test_in_positional_args():
    assert In(Reference("foo"), "a", "bc", "def").literals == {StringLiteral("a"), StringLiteral("bc"), StringLiteral("def")}


def test_in_sets():
    with pytest.raises(TypeError) as exc_info:
        assert In(Reference("foo"), {"a"}, {"bc", "def"}).literals == {
            StringLiteral("a"),
            StringLiteral("bc"),
            StringLiteral("def"),
        }
    assert str(exc_info.value) == "Invalid literal value: {'a'}"


def test_in_string_and_set():
    with pytest.raises(TypeError) as exc_info:
        assert In(Reference("foo"), "a", {"bc", "def"}).literals == {
            StringLiteral("a"),
            StringLiteral("bc"),
            StringLiteral("def"),
        }
    # Invalid literal value: {'bc', 'def'}
    # or
    # Invalid literal value: {'def', 'bc'}
    assert str(exc_info.value).startswith("Invalid literal value:")


def test_not_in_empty():
    assert NotIn(Reference("foo"), ()) == AlwaysTrue()


def test_not_in_equal():
    assert NotIn(Reference("foo"), (literal("hello"),)) == NotEqualTo(term=Reference(name="foo"), literal=StringLiteral("hello"))


def test_bind_in(table_schema_simple: Schema):
    bound = BoundIn[str](
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert In(Reference("foo"), (literal("hello"), literal("world"))).bind(table_schema_simple) == bound


def test_bind_in_invert(table_schema_simple: Schema):
    bound = BoundIn[str](
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_not_in_invert(table_schema_simple: Schema):
    bound = BoundNotIn[str](
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_dedup(table_schema_simple: Schema):
    bound = BoundIn[str](
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert In(Reference("foo"), (literal("hello"), literal("world"), literal("world"))).bind(table_schema_simple) == bound


def test_bind_dedup_to_eq(table_schema_simple: Schema):
    bound = BoundEqualTo[str](
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert In(Reference("foo"), (literal("hello"), literal("hello"))).bind(table_schema_simple) == bound


def test_bound_equal_to_invert(table_schema_simple: Schema):
    bound = BoundEqualTo[str](
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
    bound = BoundNotEqualTo[str](
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
    bound = BoundGreaterThanOrEqual[str](
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
    bound = BoundGreaterThan[str](
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
    bound = BoundLessThan[str](
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundGreaterThanOrEqual[str](
        term=BoundReference[str](
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=StringLiteral("hello"),
    )


def test_bound_less_than_or_equal_invert(table_schema_simple: Schema):
    bound = BoundLessThanOrEqual[str](
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
        NotEqualTo(Reference("foo"), literal("hello")),  # type: ignore
        EqualTo(Reference("foo"), literal("hello")),  # type: ignore
        GreaterThan(Reference("foo"), literal("hello")),  # type: ignore
        LessThan(Reference("foo"), literal("hello")),  # type: ignore
        GreaterThanOrEqual(Reference("foo"), literal("hello")),  # type: ignore
        LessThanOrEqual(Reference("foo"), literal("hello")),  # type: ignore
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
        NotEqualTo(Reference("Bar"), literal(5)),  # type: ignore
        EqualTo(Reference("Bar"), literal(5)),  # type: ignore
        GreaterThan(Reference("Bar"), literal(5)),  # type: ignore
        LessThan(Reference("Bar"), literal(5)),  # type: ignore
        GreaterThanOrEqual(Reference("Bar"), literal(5)),  # type: ignore
        LessThanOrEqual(Reference("Bar"), literal(5)),  # type: ignore
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
        (GreaterThan(Reference("foo"), literal(5)), LessThanOrEqual(Reference("foo"), literal(5))),  # type: ignore
        (LessThan(Reference("foo"), literal(5)), GreaterThanOrEqual(Reference("foo"), literal(5))),  # type: ignore
        (EqualTo(Reference("foo"), literal(5)), NotEqualTo(Reference("foo"), literal(5))),  # type: ignore
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


@pytest.fixture
def field() -> NestedField:
    return NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


@pytest.fixture
def accessor() -> Accessor:
    return Accessor(position=1)


@pytest.fixture
def term(field: NestedField, accessor: Accessor) -> BoundReference:
    return BoundReference(
        field=field,
        accessor=accessor,
    )


def test_bound_reference(field: NestedField, accessor: Accessor) -> None:
    bound_ref = BoundReference[str](field=field, accessor=accessor)
    assert str(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(accessor)})"
    assert repr(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(accessor)})"
    assert bound_ref == eval(repr(bound_ref))


def test_reference() -> None:
    abc = "abc"
    ref = Reference[str](abc)
    assert str(ref) == "Reference(name='abc')"
    assert repr(ref) == "Reference(name='abc')"
    assert ref == eval(repr(ref))


def test_and() -> None:
    null = IsNull(Reference[str]("a"))
    nan = IsNaN(Reference[str]("b"))
    and_ = And(null, nan)
    assert str(and_) == f"And(left={str(null)}, right={str(nan)})"
    assert repr(and_) == f"And(left={repr(null)}, right={repr(nan)})"
    assert and_ == eval(repr(and_))


def test_or() -> None:
    null = IsNull(Reference[str]("a"))
    nan = IsNaN(Reference[str]("b"))
    or_ = Or(null, nan)
    assert str(or_) == f"Or(left={str(null)}, right={str(nan)})"
    assert repr(or_) == f"Or(left={repr(null)}, right={repr(nan)})"
    assert or_ == eval(repr(or_))


def test_not() -> None:
    null = IsNull(Reference[str]("a"))
    or_ = Not(null)
    assert str(or_) == f"Not(child={str(null)})"
    assert repr(or_) == f"Not(child={repr(null)})"
    assert or_ == eval(repr(or_))


def test_always_true() -> None:
    always_true = AlwaysTrue()
    assert str(always_true) == "AlwaysTrue()"
    assert repr(always_true) == "AlwaysTrue()"
    assert always_true == eval(repr(always_true))


def test_always_false() -> None:
    always_false = AlwaysFalse()
    assert str(always_false) == "AlwaysFalse()"
    assert repr(always_false) == "AlwaysFalse()"
    assert always_false == eval(repr(always_false))


def test_bound_reference_field_property():
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = Accessor(position=1)
    bound_ref = BoundReference(field=field, accessor=position1_accessor)
    assert bound_ref.field == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


def test_bound_is_null(term: BoundReference) -> None:
    bound_is_null = BoundIsNull(term)
    assert str(bound_is_null) == f"BoundIsNull(term={str(term)})"
    assert repr(bound_is_null) == f"BoundIsNull(term={repr(term)})"
    assert bound_is_null == eval(repr(bound_is_null))


def test_bound_is_not_null(term: BoundReference) -> None:
    bound_not_null = BoundNotNull(term)
    assert str(bound_not_null) == f"BoundNotNull(term={str(term)})"
    assert repr(bound_not_null) == f"BoundNotNull(term={repr(term)})"
    assert bound_not_null == eval(repr(bound_not_null))


def test_is_null() -> None:
    ref = Reference[str]("a")
    is_null = IsNull(ref)
    assert str(is_null) == f"IsNull(term={str(ref)})"
    assert repr(is_null) == f"IsNull(term={repr(ref)})"
    assert is_null == eval(repr(is_null))


def test_not_null() -> None:
    ref = Reference[str]("a")
    non_null = NotNull(ref)
    assert str(non_null) == f"NotNull(term={str(ref)})"
    assert repr(non_null) == f"NotNull(term={repr(ref)})"
    assert non_null == eval(repr(non_null))


def test_bound_is_nan(accessor: Accessor) -> None:
    # We need a FloatType here
    term = BoundReference[float](
        field=NestedField(field_id=1, name="foo", field_type=FloatType(), required=False),
        accessor=accessor,
    )
    bound_is_nan = BoundIsNaN(term)
    assert str(bound_is_nan) == f"BoundIsNaN(term={str(term)})"
    assert repr(bound_is_nan) == f"BoundIsNaN(term={repr(term)})"
    assert bound_is_nan == eval(repr(bound_is_nan))


def test_bound_is_not_nan(accessor: Accessor) -> None:
    # We need a FloatType here
    term = BoundReference[float](
        field=NestedField(field_id=1, name="foo", field_type=FloatType(), required=False),
        accessor=accessor,
    )
    bound_not_nan = BoundNotNaN(term)
    assert str(bound_not_nan) == f"BoundNotNaN(term={str(term)})"
    assert repr(bound_not_nan) == f"BoundNotNaN(term={repr(term)})"
    assert bound_not_nan == eval(repr(bound_not_nan))


def test_is_nan() -> None:
    ref = Reference[str]("a")
    is_nan = IsNaN(ref)
    assert str(is_nan) == f"IsNaN(term={str(ref)})"
    assert repr(is_nan) == f"IsNaN(term={repr(ref)})"
    assert is_nan == eval(repr(is_nan))


def test_not_nan() -> None:
    ref = Reference[str]("a")
    not_nan = NotNaN(ref)
    assert str(not_nan) == f"NotNaN(term={str(ref)})"
    assert repr(not_nan) == f"NotNaN(term={repr(ref)})"
    assert not_nan == eval(repr(not_nan))


def test_bound_in(term: BoundReference) -> None:
    bound_in = BoundIn(term, "a", "b", "c")
    assert str(bound_in) == f"BoundIn({str(term)}, {{a, b, c}})"
    assert repr(bound_in) == f"BoundIn({repr(term)}, {{StringLiteral('a'), StringLiteral('b'), StringLiteral('c')}})"
    assert bound_in == eval(repr(bound_in))


def test_bound_not_in(term: BoundReference) -> None:
    bound_not_in = BoundNotIn(term, "a", "b", "c")
    assert str(bound_not_in) == f"BoundNotIn({str(term)}, {{a, b, c}})"
    assert repr(bound_not_in) == f"BoundNotIn({repr(term)}, {{StringLiteral('a'), StringLiteral('b'), StringLiteral('c')}})"
    assert bound_not_in == eval(repr(bound_not_in))


def test_in() -> None:
    ref = Reference[str]("a")
    unbound_in = In(ref, "a", "b", "c")
    assert str(unbound_in) == f"In({str(ref)}, {{a, b, c}})"
    assert repr(unbound_in) == f"In({repr(ref)}, {{StringLiteral('a'), StringLiteral('b'), StringLiteral('c')}})"
    assert unbound_in == eval(repr(unbound_in))


def test_not_in() -> None:
    ref = Reference[str]("a")
    not_in = NotIn(ref, "a", "b", "c")
    assert str(not_in) == f"NotIn({str(ref)}, {{a, b, c}})"
    assert repr(not_in) == f"NotIn({repr(ref)}, {{StringLiteral('a'), StringLiteral('b'), StringLiteral('c')}})"
    assert not_in == eval(repr(not_in))


def test_bound_equal_to(term: BoundReference) -> None:
    bound_equal_to = BoundEqualTo(term, "a")
    assert str(bound_equal_to) == f"BoundEqualTo(term={str(term)}, literal=StringLiteral('a'))"
    assert repr(bound_equal_to) == f"BoundEqualTo(term={repr(term)}, literal=StringLiteral('a'))"
    assert bound_equal_to == eval(repr(bound_equal_to))


def test_bound_not_equal_to(term: BoundReference) -> None:
    bound_not_equal_to = BoundNotEqualTo(term, "a")
    assert str(bound_not_equal_to) == f"BoundNotEqualTo(term={str(term)}, literal=StringLiteral('a'))"
    assert repr(bound_not_equal_to) == f"BoundNotEqualTo(term={repr(term)}, literal=StringLiteral('a'))"
    assert bound_not_equal_to == eval(repr(bound_not_equal_to))


def test_bound_greater_than_or_equal_to(term: BoundReference) -> None:
    bound_greater_than_or_equal_to = BoundGreaterThanOrEqual(term, "a")
    assert str(bound_greater_than_or_equal_to) == f"BoundGreaterThanOrEqual(term={str(term)}, literal=StringLiteral('a'))"
    assert repr(bound_greater_than_or_equal_to) == f"BoundGreaterThanOrEqual(term={repr(term)}, literal=StringLiteral('a'))"
    assert bound_greater_than_or_equal_to == eval(repr(bound_greater_than_or_equal_to))


def test_bound_greater_than(term: BoundReference) -> None:
    bound_greater_than = BoundGreaterThan(term, "a")
    assert str(bound_greater_than) == f"BoundGreaterThan(term={str(term)}, literal=StringLiteral('a'))"
    assert repr(bound_greater_than) == f"BoundGreaterThan(term={repr(term)}, literal=StringLiteral('a'))"
    assert bound_greater_than == eval(repr(bound_greater_than))


def test_bound_less_than(term: BoundReference) -> None:
    bound_less_than = BoundLessThan(term, "a")
    assert str(bound_less_than) == f"BoundLessThan(term={str(term)}, literal=StringLiteral('a'))"
    assert repr(bound_less_than) == f"BoundLessThan(term={repr(term)}, literal=StringLiteral('a'))"
    assert bound_less_than == eval(repr(bound_less_than))


def test_bound_less_than_or_equal(term: BoundReference) -> None:
    bound_less_than_or_equal = BoundLessThanOrEqual(term, "a")
    assert str(bound_less_than_or_equal) == f"BoundLessThanOrEqual(term={str(term)}, literal=StringLiteral('a'))"
    assert repr(bound_less_than_or_equal) == f"BoundLessThanOrEqual(term={repr(term)}, literal=StringLiteral('a'))"
    assert bound_less_than_or_equal == eval(repr(bound_less_than_or_equal))


def test_equal_to() -> None:
    equal_to = EqualTo(Reference("a"), "a")  # type: ignore
    assert str(equal_to) == "EqualTo(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert repr(equal_to) == "EqualTo(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert equal_to == eval(repr(equal_to))


def test_not_equal_to() -> None:
    not_equal_to = NotEqualTo(Reference("a"), "a")  # type: ignore
    assert str(not_equal_to) == "NotEqualTo(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert repr(not_equal_to) == "NotEqualTo(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert not_equal_to == eval(repr(not_equal_to))


def test_greater_than_or_equal_to() -> None:
    greater_than_or_equal_to = GreaterThanOrEqual(Reference("a"), "a")  # type: ignore
    assert str(greater_than_or_equal_to) == "GreaterThanOrEqual(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert repr(greater_than_or_equal_to) == "GreaterThanOrEqual(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert greater_than_or_equal_to == eval(repr(greater_than_or_equal_to))


def test_greater_than() -> None:
    greater_than = GreaterThan(Reference("a"), "a")  # type: ignore
    assert str(greater_than) == "GreaterThan(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert repr(greater_than) == "GreaterThan(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert greater_than == eval(repr(greater_than))


def test_less_than() -> None:
    less_than = LessThan(Reference("a"), "a")  # type: ignore
    assert str(less_than) == "LessThan(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert repr(less_than) == "LessThan(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert less_than == eval(repr(less_than))


def test_less_than_or_equal() -> None:
    less_than_or_equal = LessThanOrEqual(Reference("a"), "a")  # type: ignore
    assert str(less_than_or_equal) == "LessThanOrEqual(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert repr(less_than_or_equal) == "LessThanOrEqual(term=Reference(name='a'), literal=StringLiteral('a'))"
    assert less_than_or_equal == eval(repr(less_than_or_equal))


def test_bound_reference_eval(table_schema_simple: Schema, foo_struct: FooStruct) -> None:
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

    bound_ref1 = BoundReference[str](field=field1, accessor=position1_accessor)
    bound_ref2 = BoundReference[int](field=field2, accessor=position2_accessor)
    bound_ref3 = BoundReference[bool](field=field3, accessor=position3_accessor)

    assert bound_ref1.eval(foo_struct) == "foovalue"
    assert bound_ref2.eval(foo_struct) == 123
    assert bound_ref3.eval(foo_struct) is True


def test_non_primitive_from_byte_buffer():
    with pytest.raises(ValueError) as exc_info:
        _ = _from_byte_buffer(ListType(element_id=1, element_type=StringType()), b"\0x00")

    assert str(exc_info.value) == "Expected a PrimitiveType, got: <class 'pyiceberg.types.ListType'>"


def test_string_argument_unbound_unary():
    assert IsNull("a") == IsNull(Reference("a"))


def test_string_argument_unbound_literal():
    assert EqualTo("a", StringLiteral("b")) == EqualTo(Reference("a"), StringLiteral("b"))


def test_string_argument_unbound_set():
    assert In("a", StringLiteral("b"), StringLiteral("c")) == In(Reference("a"), StringLiteral("b"), StringLiteral("c"))

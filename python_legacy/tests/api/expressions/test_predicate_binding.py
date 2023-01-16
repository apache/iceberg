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

from decimal import Decimal

from iceberg.api.expressions import (Expressions,
                                     Literal,
                                     Operation,
                                     UnboundPredicate)
from iceberg.api.types import (DecimalType,
                               FloatType,
                               IntegerType,
                               NestedField,
                               StringType,
                               StructType)
from iceberg.exceptions import ValidationException


def test_multiple_fields(assert_and_unwrap):
    struct = StructType.of([NestedField.required(10, 'x', IntegerType.get()),
                           NestedField.required(11, 'y', IntegerType.get()),
                           NestedField.required(12, 'z', IntegerType.get())])

    unbound = UnboundPredicate(Operation.LT, Expressions.ref("y"), 6)
    expr = unbound.bind(struct)

    bound = assert_and_unwrap(expr)
    assert 11 == bound.ref.field.field_id
    assert Operation.LT == bound.op
    assert 6 == bound.lit.value


def test_missing_field():
    struct = StructType.of([NestedField.required(13, "x", IntegerType.get())])

    unbound = UnboundPredicate(Operation.LT, Expressions.ref("missing"), 6)
    try:
        unbound.bind(struct)
    except ValidationException as e:
        assert e.args[0].startswith("Cannot find field 'missing' in struct")


def test_comparison_predicate_binding(op, assert_and_unwrap):
    struct = StructType.of([NestedField.required(14, "x", IntegerType.get())])
    unbound = UnboundPredicate(op, Expressions.ref("x"), 5)
    bound = assert_and_unwrap(unbound.bind(struct))

    assert 5 == bound.lit.value
    assert 14 == bound.ref.field.field_id
    assert op == bound.op


def test_literal_conversion(op, assert_and_unwrap):
    struct = StructType.of([NestedField.required(15, "d", DecimalType.of(9, 2))])
    unbound = UnboundPredicate(op, Expressions.ref("d"), "12.40")
    bound = assert_and_unwrap(unbound.bind(struct))

    assert Decimal(12.40).quantize(Decimal(".01")).as_tuple() == bound.lit.value.as_tuple()
    assert 15 == bound.ref.field.field_id
    assert op == bound.op


def test_invalid_conversions(op):
    struct = StructType.of([NestedField.required(16, "f", FloatType.get())])
    unbound = UnboundPredicate(op, Expressions.ref("f"), "12.40")

    try:
        unbound.bind(struct)
    except ValidationException as e:
        assert e.args[0].startswith('Invalid Value for conversion to type float: "12.40" (StringLiteral)')


def test_long_to_integer_conversion(assert_and_unwrap):
    struct = StructType.of([NestedField.required(17, "i", IntegerType.get())])

    lt = UnboundPredicate(Operation.LT, Expressions.ref("i"), Literal.JAVA_MAX_INT + 1)
    assert lt.bind(struct) == Expressions.always_true()

    lt_eq = UnboundPredicate(Operation.LT_EQ, Expressions.ref("i"), Literal.JAVA_MAX_INT + 1)
    assert lt_eq.bind(struct) == Expressions.always_true()

    gt = UnboundPredicate(Operation.GT, Expressions.ref("i"), Literal.JAVA_MIN_INT - 1)
    assert gt.bind(struct) == Expressions.always_true()

    gt_eq = UnboundPredicate(Operation.GT_EQ, Expressions.ref("i"), Literal.JAVA_MIN_INT - 1)
    assert gt_eq.bind(struct) == Expressions.always_true()

    gt_max = UnboundPredicate(Operation.GT, Expressions.ref("i"), Literal.JAVA_MAX_INT + 1)
    assert gt_max.bind(struct) == Expressions.always_false()

    gt_eq_max = UnboundPredicate(Operation.GT_EQ, Expressions.ref("i"), Literal.JAVA_MAX_INT + 1)
    assert gt_eq_max.bind(struct) == Expressions.always_false()

    lt_min = UnboundPredicate(Operation.LT, Expressions.ref("i"), Literal.JAVA_MIN_INT - 1)
    assert lt_min.bind(struct) == Expressions.always_false()

    lt_eq_min = UnboundPredicate(Operation.LT_EQ, Expressions.ref("i"), Literal.JAVA_MIN_INT - 1)
    assert lt_eq_min.bind(struct) == Expressions.always_false()

    lt_expr = UnboundPredicate(Operation.LT, Expressions.ref("i"), Literal.JAVA_MAX_INT).bind(struct)
    lt_max = assert_and_unwrap(lt_expr)
    assert lt_max.lit.value == Literal.JAVA_MAX_INT

    lt_eq_expr = UnboundPredicate(Operation.LT_EQ, Expressions.ref("i"), Literal.JAVA_MAX_INT).bind(struct)
    lt_eq_max = assert_and_unwrap(lt_eq_expr)
    assert lt_eq_max.lit.value == Literal.JAVA_MAX_INT

    gt_expr = UnboundPredicate(Operation.GT, Expressions.ref("i"), Literal.JAVA_MIN_INT).bind(struct)
    gt_min = assert_and_unwrap(gt_expr)
    assert gt_min.lit.value == Literal.JAVA_MIN_INT

    gt_eq_expr = UnboundPredicate(Operation.GT_EQ, Expressions.ref("i"), Literal.JAVA_MIN_INT).bind(struct)
    gt_eq_min = assert_and_unwrap(gt_eq_expr)
    assert gt_eq_min.lit.value == Literal.JAVA_MIN_INT


def test_double_to_float_conversion(assert_and_unwrap):
    struct = StructType.of([NestedField.required(18, "f", FloatType.get())])

    lt = UnboundPredicate(Operation.LT, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * 2)
    assert lt.bind(struct) == Expressions.always_true()

    lt_eq = UnboundPredicate(Operation.LT_EQ, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * 2)
    assert lt_eq.bind(struct) == Expressions.always_true()

    gt = UnboundPredicate(Operation.GT, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * -2)
    assert gt.bind(struct) == Expressions.always_true()

    gt_eq = UnboundPredicate(Operation.GT_EQ, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * -2)
    assert gt_eq.bind(struct) == Expressions.always_true()

    gt_max = UnboundPredicate(Operation.GT, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * 2)
    assert gt_max.bind(struct) == Expressions.always_false()

    gt_eq_max = UnboundPredicate(Operation.GT_EQ, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * 2)
    assert gt_eq_max.bind(struct) == Expressions.always_false()

    lt_min = UnboundPredicate(Operation.LT, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * -2)
    assert lt_min.bind(struct) == Expressions.always_false()

    lt_eq_min = UnboundPredicate(Operation.LT_EQ, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT * -2)
    assert lt_eq_min.bind(struct) == Expressions.always_false()

    lt_expr = UnboundPredicate(Operation.LT, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT).bind(struct)
    lt_max = assert_and_unwrap(lt_expr)
    assert lt_max.lit.value == Literal.JAVA_MAX_FLOAT

    lt_eq_expr = UnboundPredicate(Operation.LT_EQ, Expressions.ref("f"), Literal.JAVA_MAX_FLOAT).bind(struct)
    lt_eq_max = assert_and_unwrap(lt_eq_expr)
    assert lt_eq_max.lit.value == Literal.JAVA_MAX_FLOAT

    gt_expr = UnboundPredicate(Operation.GT, Expressions.ref("f"), Literal.JAVA_MIN_INT).bind(struct)
    gt_min = assert_and_unwrap(gt_expr)
    assert gt_min.lit.value == Literal.JAVA_MIN_INT

    gt_eq_expr = UnboundPredicate(Operation.GT_EQ, Expressions.ref("f"), Literal.JAVA_MIN_INT).bind(struct)
    gt_eq_min = assert_and_unwrap(gt_eq_expr)
    assert gt_eq_min.lit.value == Literal.JAVA_MIN_INT


def test_is_null(assert_and_unwrap):
    optional = StructType.of([NestedField.optional(19, "s", StringType.get())])
    unbound = UnboundPredicate(Operation.IS_NULL, Expressions.ref("s"))
    expr = unbound.bind(optional)
    bound = assert_and_unwrap(expr)

    assert Operation.IS_NULL == bound.op
    assert 19 == bound.ref.field.field_id
    assert bound.lit is None

    required = StructType.of([NestedField.required(20, "s", StringType.get())])
    assert Expressions.always_false() == unbound.bind(required)


def test_not_null(assert_and_unwrap):
    optional = StructType.of([NestedField.optional(21, "s", StringType.get())])
    unbound = UnboundPredicate(Operation.NOT_NULL, Expressions.ref("s"))
    expr = unbound.bind(optional)
    bound = assert_and_unwrap(expr)
    assert Operation.NOT_NULL == bound.op
    assert 21 == bound.ref.field.field_id
    assert bound.lit is None

    required = StructType.of([NestedField.required(22, "s", StringType.get())])
    assert Expressions.always_true() == unbound.bind(required)

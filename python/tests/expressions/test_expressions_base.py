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

import pytest

from iceberg.expressions import base
from iceberg.types import Singleton


@pytest.mark.parametrize(
    "operation,opposite_operation",
    [
        (base.Operation.TRUE, base.Operation.FALSE),
        (base.Operation.FALSE, base.Operation.TRUE),
        (base.Operation.IS_NULL, base.Operation.NOT_NULL),
        (base.Operation.NOT_NULL, base.Operation.IS_NULL),
        (base.Operation.IS_NAN, base.Operation.NOT_NAN),
        (base.Operation.NOT_NAN, base.Operation.IS_NAN),
        (base.Operation.LT, base.Operation.GT_EQ),
        (base.Operation.LT_EQ, base.Operation.GT),
        (base.Operation.GT, base.Operation.LT_EQ),
        (base.Operation.GT_EQ, base.Operation.LT),
        (base.Operation.EQ, base.Operation.NOT_EQ),
        (base.Operation.NOT_EQ, base.Operation.EQ),
        (base.Operation.IN, base.Operation.NOT_IN),
        (base.Operation.NOT_IN, base.Operation.IN),
    ],
)
def test_negation_of_operations(operation, opposite_operation):
    assert operation.negate() == opposite_operation


@pytest.mark.parametrize(
    "operation",
    [
        base.Operation.NOT,
        base.Operation.AND,
        base.Operation.OR,
    ],
)
def test_raise_on_no_negation_for_operation(operation):
    with pytest.raises(ValueError) as exc_info:
        operation.negate()

    assert str(exc_info.value) == f"No negation defined for operation {operation}"


class TestExpressionA(base.BooleanExpression, Singleton):
    def __invert__(self):
        return TestExpressionB()

    def __repr__(self):
        return "TestExpressionA()"

    def __str__(self):
        return "testexpra"


class TestExpressionB(base.BooleanExpression, Singleton):
    def __invert__(self):
        return TestExpressionA()

    def __repr__(self):
        return "TestExpressionB()"

    def __str__(self):
        return "testexprb"


@pytest.mark.parametrize(
    "op, rep",
    [
        (
            base.And(TestExpressionA(), TestExpressionB()),
            "And(TestExpressionA(), TestExpressionB())",
        ),
        (
            base.Or(TestExpressionA(), TestExpressionB()),
            "Or(TestExpressionA(), TestExpressionB())",
        ),
        (base.Not(TestExpressionA()), "Not(TestExpressionA())"),
    ],
)
def test_reprs(op, rep):
    assert repr(op) == rep


@pytest.mark.parametrize(
    "op, string",
    [
        (base.And(TestExpressionA(), TestExpressionB()), "(testexpra and testexprb)"),
        (base.Or(TestExpressionA(), TestExpressionB()), "(testexpra or testexprb)"),
        (base.Not(TestExpressionA()), "(not testexpra)"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


@pytest.mark.parametrize(
    "input, testexpra, testexprb",
    [
        (
            base.And(TestExpressionA(), TestExpressionB()),
            base.And(TestExpressionA(), TestExpressionB()),
            base.Or(TestExpressionA(), TestExpressionB()),
        ),
        (
            base.Or(TestExpressionA(), TestExpressionB()),
            base.Or(TestExpressionA(), TestExpressionB()),
            base.And(TestExpressionA(), TestExpressionB()),
        ),
        (base.Not(TestExpressionA()), base.Not(TestExpressionA()), TestExpressionB()),
        (TestExpressionA(), TestExpressionA(), TestExpressionB()),
        (TestExpressionB(), TestExpressionB(), TestExpressionA()),
    ],
)
def test_eq(input, testexpra, testexprb):
    assert input == testexpra and input != testexprb


@pytest.mark.parametrize(
    "input, exp",
    [
        (
            base.And(TestExpressionA(), TestExpressionB()),
            base.Or(TestExpressionB(), TestExpressionA()),
        ),
        (
            base.Or(TestExpressionA(), TestExpressionB()),
            base.And(TestExpressionB(), TestExpressionA()),
        ),
        (base.Not(TestExpressionA()), TestExpressionA()),
        (TestExpressionA(), TestExpressionB()),
    ],
)
def test_negate(input, exp):
    assert ~input == exp


@pytest.mark.parametrize(
    "input, exp",
    [
        (
            base.And(TestExpressionA(), TestExpressionB(), TestExpressionA()),
            base.And(base.And(TestExpressionA(), TestExpressionB()), TestExpressionA()),
        ),
        (
            base.Or(TestExpressionA(), TestExpressionB(), TestExpressionA()),
            base.Or(base.Or(TestExpressionA(), TestExpressionB()), TestExpressionA()),
        ),
        (base.Not(base.Not(TestExpressionA())), TestExpressionA()),
    ],
)
def test_reduce(input, exp):
    assert input == exp


@pytest.mark.parametrize(
    "input, exp",
    [
        (base.And(base.AlwaysTrue(), TestExpressionB()), TestExpressionB()),
        (base.And(base.AlwaysFalse(), TestExpressionB()), base.AlwaysFalse()),
        (base.Or(base.AlwaysTrue(), TestExpressionB()), base.AlwaysTrue()),
        (base.Or(base.AlwaysFalse(), TestExpressionB()), TestExpressionB()),
        (base.Not(base.Not(TestExpressionA())), TestExpressionA()),
    ],
)
def test_base_AlwaysTrue_base_AlwaysFalse(input, exp):
    assert input == exp

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

from iceberg.expression.base import (
    And,
    BooleanExpression,
    Not,
    Or,
    alwaysFalse,
    alwaysTrue,
)


class TestExpressionA(BooleanExpression):
    def __invert__(self):
        return TestExpressionB()

    def __repr__(self):
        return "TestExpressionA()"

    def __str__(self):
        return "testexpra"


class TestExpressionB(BooleanExpression):
    def __invert__(self):
        return TestExpressionA()

    def __repr__(self):
        return "TestExpressionB()"

    def __str__(self):
        return "testexprb"


@pytest.mark.parametrize(
    "op, rep",
    [
        (And(TestExpressionA(), TestExpressionB()), "And(TestExpressionA(), TestExpressionB())"),
        (Or(TestExpressionA(), TestExpressionB()), "Or(TestExpressionA(), TestExpressionB())"),
        (Not(TestExpressionA()), "Not(TestExpressionA())"),
    ],
)
def test_reprs(op, rep):
    assert repr(op) == rep


@pytest.mark.parametrize(
    "op, string",
    [
        (And(TestExpressionA(), TestExpressionB()), "(testexpra and testexprb)"),
        (Or(TestExpressionA(), TestExpressionB()), "(testexpra or testexprb)"),
        (Not(TestExpressionA()), "(not testexpra)"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


@pytest.mark.parametrize(
    "input, testexpra, testexprb",
    [
        (
            And(TestExpressionA(), TestExpressionB()),
            And(TestExpressionA(), TestExpressionB()),
            Or(TestExpressionA(), TestExpressionB()),
        ),
        (
            Or(TestExpressionA(), TestExpressionB()),
            Or(TestExpressionA(), TestExpressionB()),
            And(TestExpressionA(), TestExpressionB()),
        ),
        (Not(TestExpressionA()), Not(TestExpressionA()), TestExpressionB()),
        (TestExpressionA(), TestExpressionA(), TestExpressionB()),
        (TestExpressionB(), TestExpressionB(), TestExpressionA()),
    ],
)
def test_eq(input, testexpra, testexprb):
    assert input == testexpra and input != testexprb


@pytest.mark.parametrize(
    "input, exp",
    [
        (And(TestExpressionA(), TestExpressionB()), Or(TestExpressionB(), TestExpressionA())),
        (Or(TestExpressionA(), TestExpressionB()), And(TestExpressionB(), TestExpressionA())),
        (Not(TestExpressionA()), TestExpressionA()),
        (TestExpressionA(), TestExpressionB()),
    ],
)
def test_negate(input, exp):
    assert ~input == exp


@pytest.mark.parametrize(
    "input, exp",
    [
        (
            And(TestExpressionA(), TestExpressionB(), TestExpressionA()),
            And(And(TestExpressionA(), TestExpressionB()), TestExpressionA()),
        ),
        (
            Or(TestExpressionA(), TestExpressionB(), TestExpressionA()),
            Or(Or(TestExpressionA(), TestExpressionB()), TestExpressionA()),
        ),
        (Not(Not(TestExpressionA())), TestExpressionA()),
    ],
)
def test_reduce(input, exp):
    assert ~input == exp


@pytest.mark.parametrize(
    "input, exp",
    [
        (And(alwaysTrue(), TestExpressionB()), TestExpressionB()),
        (And(alwaysFalse(), TestExpressionB()), And(alwaysTrue(), TestExpressionB()))(
            Or(alwaysTrue(), TestExpressionB()), alwaysTrue()
        ),
        (Or(alwaysFalse(), TestExpressionB()), TestExpressionB()),
        (Not(Not(TestExpressionA())), TestExpressionA()),
    ],
)
def test_alwaysTrue_alwaysFalse(input, exp):
    assert ~input == exp

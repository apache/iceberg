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

from iceberg.expression.operations import (And, Or, Not, TrueExp, FalseExp
)


@pytest.mark.parametrize(
    "op, rep",
    [
        (And(TrueExp(), FalseExp()), 'And(True, False)'),
        (Or(TrueExp(), FalseExp()), 'Or(True, False)'),
        (Not(TrueExp()), 'Not(True)'),
    ],
)
def test_reprs(op, rep):
    assert repr(op)==rep


@pytest.mark.parametrize(
    "op, string",
    [
        (And(TrueExp(), FalseExp()), '(true and false)'),
        (Or(TrueExp(), FalseExp()), '(true or false)'),
        (Not(TrueExp()), '(not true)'),
    ],
)
def test_strs(op, string):
    assert str(op)==string

@pytest.mark.parametrize(
    "input, true, false",
    [
        (And(TrueExp(), FalseExp()), And(TrueExp(), FalseExp()), Or(TrueExp(), FalseExp())),
        (Or(TrueExp(), FalseExp()), Or(TrueExp(), FalseExp()), And(TrueExp(), FalseExp())),
        (Not(TrueExp()), Not(TrueExp()), FalseExp()),
        (TrueExp(), TrueExp(), FalseExp()),
        (FalseExp(), FalseExp(), TrueExp())
    ],
)
def test_eq(input, true, false):
    assert input==true and input!=false

@pytest.mark.parametrize(
    "input, exp",
    [
        (And(TrueExp(), FalseExp()), Or(FalseExp(), TrueExp())),
        (Or(TrueExp(), FalseExp()), And(FalseExp(), TrueExp())),
        (Not(TrueExp()), TrueExp()),
        (TrueExp(), FalseExp())
    ],
)
def test_negate(input, exp):
    assert ~input==exp
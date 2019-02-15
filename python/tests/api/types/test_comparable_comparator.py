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

from iceberg.api.expressions import (IntegerLiteral,
                                     Literal)
import pytest


@pytest.mark.parametrize("larger,smaller", [
    (34, 33),
    (-1, -2)])
@pytest.mark.parametrize("op", [
    lambda x, y: x > y,
    lambda y, x: x < y])
def test_natural_order(larger, smaller, op):
    assert op(Literal.of(larger), Literal.of(smaller))


@pytest.mark.parametrize("input_val", [
    1,
    0,
    -1])
def test_natural_order_eq(input_val):
    assert Literal.of(input_val) == Literal.of(input_val)


@pytest.mark.parametrize("larger,smaller", [
    (34, None)])
@pytest.mark.parametrize("op", [
    lambda x, y: x > y,
    lambda y, x: x < y])
def test_null_handling(larger, smaller, op):
    assert op(IntegerLiteral(larger), IntegerLiteral(smaller))


def test_null_handling_eq():
    assert IntegerLiteral(None) == IntegerLiteral(None)

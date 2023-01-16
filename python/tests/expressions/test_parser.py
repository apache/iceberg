#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import pytest
from pyparsing import ParseException

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
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
    parser,
)


def test_true() -> None:
    assert AlwaysTrue() == parser.parse("true")


def test_false() -> None:
    assert AlwaysFalse() == parser.parse("false")


def test_is_null() -> None:
    assert IsNull("x") == parser.parse("x is null")
    assert IsNull("x") == parser.parse("x IS NULL")


def test_not_null() -> None:
    assert NotNull("x") == parser.parse("x is not null")
    assert NotNull("x") == parser.parse("x IS NOT NULL")


def test_is_nan() -> None:
    assert IsNaN("x") == parser.parse("x is nan")
    assert IsNaN("x") == parser.parse("x IS NAN")


def test_not_nan() -> None:
    assert NotNaN("x") == parser.parse("x is not nan")
    assert NotNaN("x") == parser.parse("x IS NOT NaN")


def test_less_than() -> None:
    assert LessThan("x", 5) == parser.parse("x < 5")
    assert LessThan("x", "a") == parser.parse("'a' > x")


def test_less_than_or_equal() -> None:
    assert LessThanOrEqual("x", 5) == parser.parse("x <= 5")
    assert LessThanOrEqual("x", "a") == parser.parse("'a' >= x")


def test_greater_than() -> None:
    assert GreaterThan("x", 5) == parser.parse("x > 5")
    assert GreaterThan("x", "a") == parser.parse("'a' < x")


def test_greater_than_or_equal() -> None:
    assert GreaterThanOrEqual("x", 5) == parser.parse("x <= 5")
    assert GreaterThanOrEqual("x", "a") == parser.parse("'a' >= x")


def test_equal_to() -> None:
    assert EqualTo("x", 5) == parser.parse("x = 5")
    assert EqualTo("x", "a") == parser.parse("'a' = x")
    assert EqualTo("x", "a") == parser.parse("x == 'a'")
    assert EqualTo("x", 5) == parser.parse("5 == x")


def test_not_equal_to() -> None:
    assert NotEqualTo("x", 5) == parser.parse("x != 5")
    assert NotEqualTo("x", "a") == parser.parse("'a' != x")
    assert NotEqualTo("x", "a") == parser.parse("x <> 'a'")
    assert NotEqualTo("x", 5) == parser.parse("5 <> x")


def test_in() -> None:
    assert In("x", {5, 6, 7}) == parser.parse("x in (5, 6, 7)")
    assert In("x", {"a", "b", "c"}) == parser.parse("x IN ('a', 'b', 'c')")


def test_in_different_types() -> None:
    with pytest.raises(ParseException):
        parser.parse("x in (5, 'a')")


def test_not_in() -> None:
    assert NotIn("x", {5, 6, 7}) == parser.parse("x not in (5, 6, 7)")
    assert NotIn("x", {"a", "b", "c"}) == parser.parse("x NOT IN ('a', 'b', 'c')")


def test_not_in_different_types() -> None:
    with pytest.raises(ParseException):
        parser.parse("x not in (5, 'a')")


def test_simple_and() -> None:
    assert And(GreaterThanOrEqual("x", 5), LessThan("x", 10)) == parser.parse("5 <= x and x < 10")


def test_and_with_not() -> None:
    assert And(Not(GreaterThanOrEqual("x", 5)), LessThan("x", 10)) == parser.parse("not 5 <= x and x < 10")
    assert And(GreaterThanOrEqual("x", 5), Not(LessThan("x", 10))) == parser.parse("5 <= x and not x < 10")


def test_or_with_not() -> None:
    assert Or(Not(LessThan("x", 5)), GreaterThan("x", 10)) == parser.parse("not x < 5 or 10 < x")
    assert Or(LessThan("x", 5), Not(GreaterThan("x", 10))) == parser.parse("x < 5 or not 10 < x")


def test_simple_or() -> None:
    assert Or(LessThan("x", 5), GreaterThan("x", 10)) == parser.parse("x < 5 or 10 < x")


def test_and_or_without_parens() -> None:
    assert Or(And(NotNull("x"), LessThan("x", 5)), GreaterThan("x", 10)) == parser.parse("x is not null and x < 5 or 10 < x")
    assert Or(IsNull("x"), And(GreaterThanOrEqual("x", 5), LessThan("x", 10))) == parser.parse("x is null or 5 <= x and x < 10")


def test_and_or_with_parens() -> None:
    assert And(NotNull("x"), Or(LessThan("x", 5), GreaterThan("x", 10))) == parser.parse("x is not null and (x < 5 or 10 < x)")
    assert Or(IsNull("x"), And(GreaterThanOrEqual("x", 5), Not(LessThan("x", 10)))) == parser.parse(
        "(x is null) or (5 <= x) and not(x < 10)"
    )

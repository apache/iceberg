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
    NotStartsWith,
    Or,
    StartsWith,
    parser,
)


def test_true() -> None:
    assert AlwaysTrue() == parser.parse("true")


def test_false() -> None:
    assert AlwaysFalse() == parser.parse("false")


def test_is_null() -> None:
    assert IsNull("foo") == parser.parse("foo is null")
    assert IsNull("foo") == parser.parse("foo IS NULL")


def test_not_null() -> None:
    assert NotNull("foo") == parser.parse("foo is not null")
    assert NotNull("foo") == parser.parse("foo IS NOT NULL")


def test_is_nan() -> None:
    assert IsNaN("foo") == parser.parse("foo is nan")
    assert IsNaN("foo") == parser.parse("foo IS NAN")


def test_not_nan() -> None:
    assert NotNaN("foo") == parser.parse("foo is not nan")
    assert NotNaN("foo") == parser.parse("foo IS NOT NaN")


def test_less_than() -> None:
    assert LessThan("foo", 5) == parser.parse("foo < 5")
    assert LessThan("foo", "a") == parser.parse("'a' > foo")


def test_less_than_or_equal() -> None:
    assert LessThanOrEqual("foo", 5) == parser.parse("foo <= 5")
    assert LessThanOrEqual("foo", "a") == parser.parse("'a' >= foo")


def test_greater_than() -> None:
    assert GreaterThan("foo", 5) == parser.parse("foo > 5")
    assert GreaterThan("foo", "a") == parser.parse("'a' < foo")


def test_greater_than_or_equal() -> None:
    assert GreaterThanOrEqual("foo", 5) == parser.parse("foo <= 5")
    assert GreaterThanOrEqual("foo", "a") == parser.parse("'a' >= foo")


def test_equal_to() -> None:
    assert EqualTo("foo", 5) == parser.parse("foo = 5")
    assert EqualTo("foo", "a") == parser.parse("'a' = foo")
    assert EqualTo("foo", "a") == parser.parse("foo == 'a'")
    assert EqualTo("foo", 5) == parser.parse("5 == foo")


def test_not_equal_to() -> None:
    assert NotEqualTo("foo", 5) == parser.parse("foo != 5")
    assert NotEqualTo("foo", "a") == parser.parse("'a' != foo")
    assert NotEqualTo("foo", "a") == parser.parse("foo <> 'a'")
    assert NotEqualTo("foo", 5) == parser.parse("5 <> foo")


def test_in() -> None:
    assert In("foo", {5, 6, 7}) == parser.parse("foo in (5, 6, 7)")
    assert In("foo", {"a", "b", "c"}) == parser.parse("foo IN ('a', 'b', 'c')")


def test_in_different_types() -> None:
    with pytest.raises(ParseException):
        parser.parse("foo in (5, 'a')")


def test_not_in() -> None:
    assert NotIn("foo", {5, 6, 7}) == parser.parse("foo not in (5, 6, 7)")
    assert NotIn("foo", {"a", "b", "c"}) == parser.parse("foo NOT IN ('a', 'b', 'c')")


def test_not_in_different_types() -> None:
    with pytest.raises(ParseException):
        parser.parse("foo not in (5, 'a')")


def test_simple_and() -> None:
    assert And(GreaterThanOrEqual("foo", 5), LessThan("foo", 10)) == parser.parse("5 <= foo and foo < 10")


def test_and_with_not() -> None:
    assert And(Not(GreaterThanOrEqual("foo", 5)), LessThan("foo", 10)) == parser.parse("not 5 <= foo and foo < 10")
    assert And(GreaterThanOrEqual("foo", 5), Not(LessThan("foo", 10))) == parser.parse("5 <= foo and not foo < 10")


def test_or_with_not() -> None:
    assert Or(Not(LessThan("foo", 5)), GreaterThan("foo", 10)) == parser.parse("not foo < 5 or 10 < foo")
    assert Or(LessThan("foo", 5), Not(GreaterThan("foo", 10))) == parser.parse("foo < 5 or not 10 < foo")


def test_simple_or() -> None:
    assert Or(LessThan("foo", 5), GreaterThan("foo", 10)) == parser.parse("foo < 5 or 10 < foo")


def test_and_or_without_parens() -> None:
    assert Or(And(NotNull("foo"), LessThan("foo", 5)), GreaterThan("foo", 10)) == parser.parse(
        "foo is not null and foo < 5 or 10 < foo"
    )
    assert Or(IsNull("foo"), And(GreaterThanOrEqual("foo", 5), LessThan("foo", 10))) == parser.parse(
        "foo is null or 5 <= foo and foo < 10"
    )


def test_and_or_with_parens() -> None:
    assert And(NotNull("foo"), Or(LessThan("foo", 5), GreaterThan("foo", 10))) == parser.parse(
        "foo is not null and (foo < 5 or 10 < foo)"
    )
    assert Or(IsNull("foo"), And(GreaterThanOrEqual("foo", 5), Not(LessThan("foo", 10)))) == parser.parse(
        "(foo is null) or (5 <= foo) and not(foo < 10)"
    )


def test_starts_with() -> None:
    assert StartsWith("foo", "data") == parser.parse("foo LIKE 'data'")


def test_not_starts_with() -> None:
    assert NotStartsWith("foo", "data") == parser.parse("foo NOT LIKE 'data'")

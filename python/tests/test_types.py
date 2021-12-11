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

import math

import pytest

from iceberg.types import (
    UUID,
    Binary,
    Boolean,
    Date,
    Double,
    Float,
    Integer,
    Long,
    String,
    Time,
    Timestamp,
    Timestamptz,
)


# https://iceberg.apache.org/#spec/#appendix-b-32-bit-hash-requirements
@pytest.mark.parametrize(
    "instance,expected",
    [
        (UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"), 1488055340),
        (Boolean(True), 1392991556),
        (Integer(34), 2017239379),
        (Long(34), 2017239379),
        (Float(1), -142385009),
        (Double(1), -142385009),
        (String("iceberg"), 1210000089),
        (Binary(b"\x00\x01\x02\x03"), -188683207),
        (Date("2017-11-16"), -653330422),
        (Time(22, 31, 8), -662762989),
        (Timestamp("2017-11-16T14:31:08-08:00"), -2047944441),
        (Timestamptz("2017-11-16T14:31:08-08:00"), -2047944441),
    ],
)
def test_hashing(instance, expected):
    assert hash(instance) == expected


def test_integer_under_overflows():
    with pytest.raises(ValueError):
        Integer(2 ** 31)
    with pytest.raises(ValueError):
        Integer(-(2 ** 31) - 1)


@pytest.mark.parametrize(
    "_from, to, coerce",
    [
        (Integer(5), Long, False),
        (Integer(5), Double, True),
        (Integer(5), Float, True),
        (Float(3.14), Double, True),
    ],
)
def test_number_casting_succeeds(_from, to, coerce):
    assert _from.to(to, coerce)


@pytest.mark.parametrize(
    "_from, to",
    [
        (Integer(5), Double),
        (Integer(5), Float),
        (Long(5), Double),
        (Long(5), Float),
    ],
)
def test_number_casting_fails(_from, to):
    with pytest.raises(TypeError):
        _from.to(to)


@pytest.mark.parametrize(
    "operation, result",
    [
        (Integer(5) <= Integer(5), True),
        (Long(5) <= Long(5), True),
        (Integer(5) < Integer(5), False),
        (Long(5) < Long(5), False),
        (Long(5) < Integer(5), False),
        (Double(5) < Float(5), False),
        (Long(2) - Long(1), Long(1)),
        (
            Double(math.pi) == Float(math.pi),
            False,
        ),  # False is the correct value based on ieee754 representation going from 32 to 64 bits
        (Double(math.pi) ** 2, Double(math.pi ** 2)),
        (abs(Float(-6) ** Float(2)), Float(36)),
        ((Float(5) ** 4) % Float(4), Float(1)),
    ],
)
def test_number_arithmetic(operation, result):
    assert operation == result


@pytest.mark.parametrize(
    "order",
    [
        Float("-nan")
        < Float("-inf")
        < Float(-4e10)
        < Float(-24)
        < Float("-0")
        < Float(0)
        < Float(24)
        < Float(4e10)
        < Float("nan")
        < Float("inf"),
        Float("-nan")
        <= Float("-inf")
        <= Float(-4e10)
        <= Float(-24)
        <= Float("-0")
        <= Float(0)
        <= Float(24)
        <= Float(4e10)
        <= Float("nan")
        <= Float("inf"),
        Double("-nan")
        < Double("-inf")
        < Double(-4e10)
        < Double(-24)
        < Double("-0")
        < Double(0)
        < Double(24)
        < Double(4e10)
        < Double("nan")
        < Double("inf"),
        Double("-nan")
        <= Double("-inf")
        <= Double(-4e10)
        <= Double(-24)
        <= Double("-0")
        <= Double(0)
        <= Double(24)
        <= Double(4e10)
        <= Double("nan")
        <= Double("inf"),
        not Float("-nan")
        > Float("-inf")
        > Float(-4e10)
        > Float(-24)
        > Float("-0")
        > Float(0)
        > Float(24)
        > Float(4e10)
        > Float("nan")
        > Float("inf"),
        not Float("-nan")
        >= Float("-inf")
        >= Float(-4e10)
        >= Float(-24)
        >= Float("-0")
        >= Float(0)
        >= Float(24)
        >= Float(4e10)
        >= Float("nan")
        >= Float("inf"),
        not Double("-nan")
        > Double("-inf")
        > Double(-4e10)
        > Double(-24)
        > Double("-0")
        > Double(0)
        > Double(24)
        > Double(4e10)
        > Double("nan")
        > Double("inf"),
        not Double("-nan")
        >= Double("-inf")
        >= Double(-4e10)
        >= Double(-24)
        >= Double("-0")
        >= Double(0)
        >= Double(24)
        >= Double(4e10)
        >= Double("nan")
        >= Double("inf"),
    ],
)
def test_floating_sort_order(order):
    assert order


def test_sort_order_fails():
    with pytest.raises(TypeError):
        Float(5) < Integer(6)
    with pytest.raises(TypeError):
        Float(5) < int

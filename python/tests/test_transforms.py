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

import pytest

from iceberg import transforms
from iceberg.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
)


@pytest.mark.parametrize(
    "test_input,test_type,expected",
    [
        (1, IntegerType, 1392991556),
        (34, IntegerType, 2017239379),
        (34, LongType, 2017239379),
        (1, FloatType, -142385009),
        (1, DoubleType, -142385009),
    ],
)
def test_spec_values_int(test_input, test_type, expected):
    assert transforms.Bucket._FUNCTIONS_MAP[test_type][0](test_input) == expected


@pytest.mark.parametrize("type_var", [IntegerType, LongType])
@pytest.mark.parametrize(
    "input_var,expected",
    [(1, 0), (5, 0), (9, 0), (10, 10), (11, 10), (-1, -10), (-10, -10), (-12, -20)],
)
def test_truncate_integer(type_var, input_var, expected):
    trunc = transforms.truncate(type_var, 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize(
    "input_var,expected",
    [
        (Decimal(12.34).quantize(Decimal(".01")), Decimal("12.30")),
        (Decimal(12.30).quantize(Decimal(".01")), Decimal("12.30")),
        (Decimal(12.20).quantize(Decimal(".01")), Decimal("12.20")),
        (Decimal(0.05).quantize(Decimal(".01")), Decimal("0.00")),
        (Decimal(-0.05).quantize(Decimal(".01")), Decimal("-0.10")),
    ],
)
def test_truncate_decimal(input_var, expected):
    trunc = transforms.truncate(DecimalType(9, 2), 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize("input_var,expected", [("abcdefg", "abcde"), ("abc", "abc")])
def test_truncate_string(input_var, expected):
    trunc = transforms.truncate(StringType, 5)
    assert trunc.apply(input_var) == expected

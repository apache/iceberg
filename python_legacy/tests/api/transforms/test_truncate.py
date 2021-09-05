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

from iceberg.api.transforms import Truncate
from iceberg.api.types import (DecimalType,
                               IntegerType,
                               LongType,
                               StringType)
import pytest


@pytest.mark.parametrize("type_var", [IntegerType.get(), LongType.get()])
@pytest.mark.parametrize("input_var,expected", [
    (1, 0),
    (5, 0),
    (9, 0),
    (10, 10),
    (11, 10),
    (-1, -10),
    (-10, -10),
    (-12, -20)])
def test_truncate_integer(type_var, input_var, expected):
    trunc = Truncate.get(type_var, 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize("input_var,expected", [
    (Decimal(12.34).quantize(Decimal(".01")), Decimal("12.30")),
    (Decimal(12.30).quantize(Decimal(".01")), Decimal("12.30")),
    (Decimal(12.20).quantize(Decimal(".01")), Decimal("12.20")),
    (Decimal(0.05).quantize(Decimal(".01")), Decimal("0.00")),
    (Decimal(-0.05).quantize(Decimal(".01")), Decimal("-0.10"))])
def test_truncate_decimal(input_var, expected):
    trunc = Truncate.get(DecimalType.of(9, 2), 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize("input_var,expected", [
    ("abcdefg", "abcde"),
    ("abc", "abc")])
def test_truncate_string(input_var, expected):
    trunc = Truncate.get(StringType.get(), 5)
    assert trunc.apply(input_var) == expected

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
import math

from iceberg.api.expressions import Literal
from iceberg.api.types import (DecimalType,
                               DoubleType,
                               FloatType,
                               IntegerType,
                               LongType)


def test_integer_to_long_conversion():
    lit = Literal.of(34)
    long_lit = lit.to(LongType.get())

    assert lit.value == long_lit.value


def test_integer_to_float_conversion():
    lit = Literal.of(34)
    float_lit = lit.to(FloatType.get())

    assert math.isclose(lit.value, float_lit.value)


def test_integer_to_double_conversion():
    lit = Literal.of(34)
    dbl_lit = lit.to(DoubleType.get())

    assert math.isclose(lit.value, dbl_lit.value)


def test_integer_to_decimal_conversion(type_val_tuples):
    lit = Literal.of(34)

    assert lit.to(type_val_tuples[0]).value.as_tuple() == Decimal(type_val_tuples[1]).as_tuple()


def test_long_to_integer():
    lit = Literal.of(34).to(LongType.get())
    int_lit = lit.to(IntegerType.get())

    assert lit.value == int_lit.value


def test_long_to_float_conversion():
    lit = Literal.of(34).to(LongType.get())
    float_lit = lit.to(FloatType.get())

    assert math.isclose(lit.value, float_lit.value)


def test_long_to_double_conversion():
    lit = Literal.of(34).to(LongType.get())
    dbl_lit = lit.to(DoubleType.get())

    assert math.isclose(lit.value, dbl_lit.value)


def test_long_to_decimal_conversion(type_val_tuples):
    lit = Literal.of(34).to(LongType.get())

    assert lit.to(type_val_tuples[0]).value.as_tuple() == Decimal(type_val_tuples[1]).as_tuple()


def test_float_to_double():
    lit = Literal.of(34.56)
    dbl_lit = lit.to(DoubleType.get())

    assert math.isclose(lit.value, dbl_lit.value)


def test_float_to_decimal_conversion(float_type_val_tuples):
    lit = Literal.of(34.56)

    assert lit.to(float_type_val_tuples[0]).value.as_tuple() == Decimal(float_type_val_tuples[1]).as_tuple()


def test_double_to_float():
    lit = Literal.of(34.56).to(DoubleType.get())
    float_lit = lit.to(FloatType.get())

    assert math.isclose(lit.value, float_lit.value)


def test_double_to_decimal_conversion(float_type_val_tuples):
    lit = Literal.of(34.56).to(DoubleType.get())

    assert lit.to(float_type_val_tuples[0]).value.as_tuple() == Decimal(float_type_val_tuples[1]).as_tuple()


def test_decimal_to_decimal_conversion():
    lit = Literal.of(Decimal("34.11").quantize(Decimal(".01")))

    assert lit.value.as_tuple() == lit.to(DecimalType.of(9, 2)).value.as_tuple()
    assert lit.value.as_tuple() == lit.to(DecimalType.of(11, 2)).value.as_tuple()
    assert lit.to(DecimalType.of(9, 0)) is None
    assert lit.to(DecimalType.of(9, 1)) is None
    assert lit.to(DecimalType.of(9, 3)) is None

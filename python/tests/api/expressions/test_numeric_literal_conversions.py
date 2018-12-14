# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from decimal import Decimal
import unittest

from iceberg.api.expressions import Literal
from iceberg.api.types import (DecimalType,
                               DoubleType,
                               FloatType,
                               IntegerType,
                               LongType)


class TestNumericLiteralConversions(unittest.TestCase):

    def test_integer_to_long_conversion(self):
        lit = Literal.of(34)
        long_lit = lit.to(LongType.get())

        self.assertEqual(lit.value, long_lit.value)

    def test_integer_to_float_conversion(self):
        lit = Literal.of(34)
        float_lit = lit.to(FloatType.get())

        self.assertAlmostEqual(lit.value, float_lit.value)

    def test_integer_to_double_conversion(self):
        lit = Literal.of(34)
        dbl_lit = lit.to(DoubleType.get())

        self.assertAlmostEqual(lit.value, dbl_lit.value)

    def test_integer_to_decimal_conversion(self):
        lit = Literal.of(34)

        self.assertEqual(lit.to(DecimalType.of(9, 0)).value.as_tuple(), Decimal("34").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 2)).value.as_tuple(), Decimal("34.00").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 4)).value.as_tuple(), Decimal("34.0000").as_tuple())

    def test_long_to_integer(self):
        lit = Literal.of(34).to(LongType.get())
        int_lit = lit.to(IntegerType.get())

        self.assertEqual(lit.value, int_lit.value)

    def test_long_to_float_conversion(self):
        lit = Literal.of(34).to(LongType.get())
        float_lit = lit.to(FloatType.get())

        self.assertAlmostEqual(lit.value, float_lit.value)

    def test_long_to_double_conversion(self):
        lit = Literal.of(34).to(LongType.get())
        dbl_lit = lit.to(DoubleType.get())

        self.assertAlmostEqual(lit.value, dbl_lit.value)

    def test_long_to_decimal_conversion(self):
        lit = Literal.of(34).to(LongType.get())

        self.assertEqual(lit.to(DecimalType.of(9, 0)).value.as_tuple(), Decimal("34").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 2)).value.as_tuple(), Decimal("34.00").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 4)).value.as_tuple(), Decimal("34.0000").as_tuple())

    def test_float_to_double(self):
        lit = Literal.of(34.56)
        dbl_lit = lit.to(DoubleType.get())

        self.assertAlmostEqual(lit.value, dbl_lit.value)

    def test_float_to_decimal_conversion(self):
        lit = Literal.of(34.56)

        self.assertEqual(lit.to(DecimalType.of(9, 1)).value.as_tuple(), Decimal("34.6").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 2)).value.as_tuple(), Decimal("34.56").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 4)).value.as_tuple(), Decimal("34.5600").as_tuple())

    def test_double_to_float(self):
        lit = Literal.of(34.56).to(DoubleType.get())
        float_lit = lit.to(FloatType.get())

        self.assertAlmostEqual(lit.value, float_lit.value)

    def test_double_to_decimal_conversion(self):
        lit = Literal.of(34.56).to(DoubleType.get())

        self.assertEqual(lit.to(DecimalType.of(9, 1)).value.as_tuple(), Decimal("34.6").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 2)).value.as_tuple(), Decimal("34.56").as_tuple())
        self.assertEqual(lit.to(DecimalType.of(9, 4)).value.as_tuple(), Decimal("34.5600").as_tuple())

    def test_decimal_to_decimal_conversion(self):
        lit = Literal.of(Decimal("34.11").quantize(Decimal(".01")))

        self.assertEqual(lit.value.as_tuple(), lit.to(DecimalType.of(9, 2)).value.as_tuple())
        self.assertEqual(lit.value.as_tuple(), lit.to(DecimalType.of(11, 2)).value.as_tuple())
        self.assertIsNone(lit.to(DecimalType.of(9, 0)))
        self.assertIsNone(lit.to(DecimalType.of(9, 1)))
        self.assertIsNone(lit.to(DecimalType.of(9, 3)))

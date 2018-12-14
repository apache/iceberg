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
import sys
import unittest
import uuid


from iceberg.api.expressions import Literal
from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DecimalType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)


class TestMiscLiteralConversions(unittest.TestCase):

    def test_identity_conversions(self):
        pairs = [(Literal.of(True), BooleanType.get()),
                 (Literal.of(34), IntegerType.get()),
                 (Literal.of(3400000000), LongType.get()),
                 (Literal.of(34.11), FloatType.get()),
                 (Literal.of(34.11), DoubleType.get()),
                 (Literal.of(Decimal(34.55).quantize(Decimal("0.01"))), DecimalType.of(9, 2)),
                 (Literal.of("2017-08-18"), DateType.get()),
                 (Literal.of("14:21:01.919"), TimeType.get()),
                 (Literal.of("2017-08-18T14:21:01.919"), TimestampType.without_timezone()),
                 (Literal.of("abc"), StringType.get()),
                 (Literal.of(uuid.uuid4()), UUIDType.get())
                 ]

        if sys.version_info[0] >= 3:
            pairs = pairs + [(Literal.of(bytes([0x01, 0x02, 0x03])), FixedType.of_length(3)),
                             (Literal.of(bytearray([0x03, 0x04, 0x05, 0x06])), BinaryType.get())]

        for pair in pairs:
            expected = pair[0].to(pair[1])
            self.assertIs(expected, expected.to(pair[1]))

    def test_binary_to_fixed(self):
        if sys.version_info[0] >= 3:
            lit = Literal.of(bytearray([0x00, 0x01, 0x02]))
            fixed_lit = lit.to(FixedType.of_length(3))
            self.assertIsNotNone(fixed_lit)
            self.assertEqual(lit.value, fixed_lit.value)
            self.assertIsNone(lit.to(FixedType.of_length(4)))
            self.assertIsNone(lit.to(FixedType.of_length(2)))

    def test_fixed_to_binary(self):
        if sys.version_info[0] >= 3:
            lit = Literal.of(bytes([0x00, 0x01, 0x02]))
            binary_lit = lit.to(BinaryType.get())
            self.assertIsNotNone(binary_lit)
            self.assertEqual(lit.value, binary_lit.value)

    def test_invalid_boolean_conversions(self):
        self.assert_invalid_conversions(Literal.of(True), [IntegerType.get(),
                                                           LongType.get(),
                                                           FloatType.get(),
                                                           DoubleType.get(),
                                                           DateType.get(),
                                                           TimeType.get(),
                                                           TimestampType.with_timezone(),
                                                           TimestampType.without_timezone(),
                                                           DecimalType.of(9, 2),
                                                           StringType.get(),
                                                           UUIDType.get(),
                                                           FixedType.of_length(1),
                                                           BinaryType.get()])

    def test_invalid_integer_conversions(self):
        self.assert_invalid_conversions(Literal.of(34), [BooleanType.get(),
                                                         TimeType.get(),
                                                         TimestampType.with_timezone(),
                                                         TimestampType.without_timezone(),
                                                         StringType.get(),
                                                         UUIDType.get(),
                                                         FixedType.of_length(1),
                                                         BinaryType.get()])

    def test_invalid_long_conversions(self):
        self.assert_invalid_conversions(Literal.of(34).to(LongType.get()), [BooleanType.get(),
                                                                            DateType.get(),
                                                                            StringType.get(),
                                                                            UUIDType.get(),
                                                                            FixedType.of_length(1),
                                                                            BinaryType.get()])

    def test_invalid_float_conversions(self):
        self.assert_invalid_conversions(Literal.of(34.11), [BooleanType.get(),
                                                            IntegerType.get(),
                                                            LongType.get(),
                                                            DateType.get(),
                                                            TimeType.get(),
                                                            TimestampType.with_timezone(),
                                                            TimestampType.without_timezone(),
                                                            StringType.get(),
                                                            UUIDType.get(),
                                                            FixedType.of_length(1),
                                                            BinaryType.get()])

    def test_invalid_double_conversions(self):
        self.assert_invalid_conversions(Literal.of(34.11).to(DoubleType.get()), [BooleanType.get(),
                                                                                 IntegerType.get(),
                                                                                 LongType.get(),
                                                                                 DateType.get(),
                                                                                 TimeType.get(),
                                                                                 TimestampType.with_timezone(),
                                                                                 TimestampType.without_timezone(),
                                                                                 StringType.get(),
                                                                                 UUIDType.get(),
                                                                                 FixedType.of_length(1),
                                                                                 BinaryType.get()])

    def test_invalid_date_conversions(self):
        self.assert_invalid_conversions(Literal.of("2017-08-18").to(DateType.get()), [BooleanType.get(),
                                                                                      IntegerType.get(),
                                                                                      LongType.get(),
                                                                                      FloatType.get(),
                                                                                      DoubleType.get(),
                                                                                      TimeType.get(),
                                                                                      TimestampType.with_timezone(),
                                                                                      TimestampType.without_timezone(),
                                                                                      DecimalType.of(9, 2),
                                                                                      StringType.get(),
                                                                                      UUIDType.get(),
                                                                                      FixedType.of_length(1),
                                                                                      BinaryType.get()])

    def test_invalid_time_conversions(self):
        self.assert_invalid_conversions(Literal.of("14:21:01.919")
                                        .to(TimeType.get()), [BooleanType.get(),
                                                              IntegerType.get(),
                                                              LongType.get(),
                                                              FloatType.get(),
                                                              DoubleType.get(),
                                                              DateType.get(),
                                                              TimestampType.with_timezone(),
                                                              TimestampType.without_timezone(),
                                                              DecimalType.of(9, 2),
                                                              StringType.get(),
                                                              UUIDType.get(),
                                                              FixedType.of_length(1),
                                                              BinaryType.get()])

    def test_invalid_timestamp_conversions(self):
        self.assert_invalid_conversions(Literal.of("2017-08-18T14:21:01.919")
                                        .to(TimestampType.without_timezone()), [BooleanType.get(),
                                                                                IntegerType.get(),
                                                                                LongType.get(),
                                                                                FloatType.get(),
                                                                                DoubleType.get(),
                                                                                TimeType.get(),
                                                                                DecimalType.of(9, 2),
                                                                                StringType.get(),
                                                                                UUIDType.get(),
                                                                                FixedType.of_length(1),
                                                                                BinaryType.get()])

    def test_invalid_decimal_conversions(self):
        self.assert_invalid_conversions(Literal.of(Decimal("34.11")), [BooleanType.get(),
                                                                       IntegerType.get(),
                                                                       LongType.get(),
                                                                       FloatType.get(),
                                                                       DoubleType.get(),
                                                                       DateType.get(),
                                                                       TimeType.get(),
                                                                       TimestampType.with_timezone(),
                                                                       TimestampType.without_timezone(),
                                                                       DecimalType.of(9, 4),
                                                                       StringType.get(),
                                                                       UUIDType.get(),
                                                                       FixedType.of_length(1),
                                                                       BinaryType.get()])

    def test_invalid_string_conversions(self):
        self.assert_invalid_conversions(Literal.of("abc"), [BooleanType.get(),
                                                            IntegerType.get(),
                                                            LongType.get(),
                                                            FloatType.get(),
                                                            DoubleType.get(),
                                                            FixedType.of_length(1),
                                                            BinaryType.get()])

    def test_invalid_uuid_conversions(self):
        self.assert_invalid_conversions(Literal.of(uuid.uuid4()), [BooleanType.get(),
                                                                   IntegerType.get(),
                                                                   LongType.get(),
                                                                   FloatType.get(),
                                                                   DoubleType.get(),
                                                                   DateType.get(),
                                                                   TimeType.get(),
                                                                   TimestampType.with_timezone(),
                                                                   TimestampType.without_timezone(),
                                                                   DecimalType.of(9, 2),
                                                                   StringType.get(),
                                                                   FixedType.of_length(1),
                                                                   BinaryType.get()])

    def test_invalid_fixed_conversions(self):
        if sys.version_info[0] >= 3:
            self.assert_invalid_conversions(Literal.of(bytes([0x00, 0x01, 0x02])), [BooleanType.get(),
                                                                                    IntegerType.get(),
                                                                                    LongType.get(),
                                                                                    FloatType.get(),
                                                                                    DoubleType.get(),
                                                                                    DateType.get(),
                                                                                    TimeType.get(),
                                                                                    TimestampType.with_timezone(),
                                                                                    TimestampType.without_timezone(),
                                                                                    DecimalType.of(9, 2),
                                                                                    StringType.get(),
                                                                                    UUIDType.get(),
                                                                                    FixedType.of_length(1)])

    def test_invalid_binary_conversions(self):
        if sys.version_info[0] >= 3:
            self.assert_invalid_conversions(Literal.of(bytearray([0x00, 0x01, 0x02])), [BooleanType.get(),
                                                                                        IntegerType.get(),
                                                                                        LongType.get(),
                                                                                        FloatType.get(),
                                                                                        DoubleType.get(),
                                                                                        DateType.get(),
                                                                                        TimeType.get(),
                                                                                        TimestampType.with_timezone(),
                                                                                        TimestampType.without_timezone(),
                                                                                        DecimalType.of(9, 2),
                                                                                        StringType.get(),
                                                                                        UUIDType.get(),
                                                                                        FixedType.of_length(1)])

    def assert_invalid_conversions(self, lit, types=None):
        for type_var in types:
            self.assertIsNone(lit.to(type_var))

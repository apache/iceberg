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
import uuid

from iceberg.api.expressions import Literal
from iceberg.api.types import (BinaryType,
                               DateType,
                               FixedType,
                               TimestampType,
                               TimeType)
from tests.api.test_helpers import TestHelpers


class TestLiteralSerialization(unittest.TestCase):

    def test_literals(self):
        literals = [Literal.of(False),
                    Literal.of(34),
                    Literal.of(35),
                    Literal.of(36.75),
                    Literal.of(8.75),
                    Literal.of("2017-11-29").to(DateType.get()),
                    Literal.of("11:30:0").to(TimeType.get()),
                    Literal.of("2017-11-29T11:30:07.123").to(TimestampType.without_timezone()),
                    Literal.of("2017-11-29T11:30:07.123+01:00").to(TimestampType.with_timezone()),
                    Literal.of("abc"),
                    Literal.of(uuid.uuid4()),
                    Literal.of(bytes([0x01, 0x02, 0x03])).to(FixedType.of_length(3)),
                    Literal.of(bytes([0x03, 0x04, 0x05, 0x06])).to(BinaryType.get()),
                    Literal.of(Decimal(122.50).quantize(Decimal(".01")))]

        for literal in literals:
            self.assertTrue(TestLiteralSerialization.check_literal(literal))

    @staticmethod
    def check_literal(lit):
        copy = TestHelpers.round_trip_serialize(lit)
        return copy == lit

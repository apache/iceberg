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

import unittest

from iceberg.api.expressions import Literal
from iceberg.api.types import (DateType,
                               DoubleType,
                               IntegerType,
                               LongType,
                               StringType)
from iceberg.api.types.conversions import Conversions


class TestConversions(unittest.TestCase):

    def test_from_bytes(self):
        self.assertEqual(1234, Conversions.from_byte_buffer(IntegerType.get(),
                                                            b'\xd2\x04\x00\x00'))
        self.assertEqual(1234, Conversions.from_byte_buffer(LongType.get(),
                                                            b'\xd2\x04\x00\x00\x00\x00\x00\x00'))
        self.assertAlmostEqual(1.2345, Conversions.from_byte_buffer(DoubleType.get(),
                                                                    b'\x8d\x97\x6e\x12\x83\xc0\xf3\x3f'))
        self.assertEqual("foo", Conversions.from_byte_buffer(StringType.get(), b'foo'))

    def test_to_bytes(self):
        self.assertEqual(b'\x00\x00', Literal.of(False).to_byte_buffer())
        self.assertEqual(b'\x01\x00', Literal.of(True).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of("foo").to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00', Literal.of(1234).to_byte_buffer())
        self.assertEqual(b'\xe8\x03\x00\x00', Literal.of(1000).to(DateType.get()).to_byte_buffer())

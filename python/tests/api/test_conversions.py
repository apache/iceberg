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
import uuid

from iceberg.api.expressions import Literal
from iceberg.api.types import (BinaryType,
                               BooleanType,
                               DateType,
                               DoubleType,
                               FixedType,
                               FloatType,
                               IntegerType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)
from iceberg.api.types.conversions import Conversions


class TestConversions(unittest.TestCase):

    def test_from_bytes(self):
        self.assertEqual(False, Conversions.from_byte_buffer(BooleanType.get(), b'\x00\x00'))
        self.assertEqual(True, Conversions.from_byte_buffer(BooleanType.get(), b'\x01\x00'))
        self.assertEqual(1234, Conversions.from_byte_buffer(IntegerType.get(),
                                                            b'\xd2\x04\x00\x00'))
        self.assertEqual(1234, Conversions.from_byte_buffer(LongType.get(),
                                                            b'\xd2\x04\x00\x00\x00\x00\x00\x00'))
        self.assertAlmostEqual(1.2345, Conversions.from_byte_buffer(FloatType.get(),
                                                                    b'\x19\x04\x9e?'), places=5)
        self.assertAlmostEqual(1.2345, Conversions.from_byte_buffer(DoubleType.get(),
                                                                    b'\x8d\x97\x6e\x12\x83\xc0\xf3\x3f'))
        self.assertEqual(1234, Conversions.from_byte_buffer(DateType.get(),
                                                            b'\xd2\x04\x00\x00'))
        self.assertEqual(100000000000, Conversions.from_byte_buffer(TimeType.get(),
                                                                    b'\x00\xe8vH\x17\x00\x00\x00'))
        self.assertEqual(100000000000, Conversions.from_byte_buffer(TimestampType.with_timezone(),
                                                                    b'\x00\xe8vH\x17\x00\x00\x00'))
        self.assertEqual(100000000000, Conversions.from_byte_buffer(TimestampType.without_timezone(),
                                                                    b'\x00\xe8vH\x17\x00\x00\x00'))
        self.assertEqual("foo", Conversions.from_byte_buffer(StringType.get(), b'foo'))
        self.assertEqual(uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
                         Conversions.from_byte_buffer(UUIDType.get(), b'\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7'))
        self.assertEqual(b'foo', Conversions.from_byte_buffer(FixedType.of_length(3), b'foo'))
        self.assertEqual(b'foo', Conversions.from_byte_buffer(BinaryType.get(), b'foo'))

    def test_to_bytes(self):
        self.assertEqual(b'\x00\x00', Literal.of(False).to_byte_buffer())
        self.assertEqual(b'\x01\x00', Literal.of(True).to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00', Literal.of(1234).to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00\x00\x00\x00\x00', Literal.of(1234).to(LongType.get()).to_byte_buffer())
        self.assertEqual(b'\x19\x04\x9e?', Literal.of(1.2345).to_byte_buffer())
        self.assertEqual(b'\x8d\x97\x6e\x12\x83\xc0\xf3\x3f', Literal.of(1.2345).to(DoubleType.get()).to_byte_buffer())
        self.assertEqual(b'\xd2\x04\x00\x00', Literal.of(1234).to(DateType.get()).to_byte_buffer())
        self.assertEqual(b'\x00\xe8vH\x17\x00\x00\x00', Literal.of(100000000000).to(TimeType.get()).to_byte_buffer())
        self.assertEqual(b'\x00\xe8vH\x17\x00\x00\x00',
                         Literal.of(100000000000).to(TimestampType.with_timezone()).to_byte_buffer())
        self.assertEqual(b'\x00\xe8vH\x17\x00\x00\x00',
                         Literal.of(100000000000).to(TimestampType.without_timezone()).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of("foo").to_byte_buffer())
        self.assertEqual(b'\xf7\x9c>\tg|K\xbd\xa4y?4\x9c\xb7\x85\xe7',
                         Literal.of(uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of(bytes(b'foo')).to_byte_buffer())
        self.assertEqual(b'foo', Literal.of(bytearray(b'foo')).to_byte_buffer())

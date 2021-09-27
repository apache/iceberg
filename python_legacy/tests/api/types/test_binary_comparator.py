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

from iceberg.api.expressions import Literal
from iceberg.api.types import FixedType


def test_binary_unsigned_comparator():
    b1 = bytearray([0x01, 0x01, 0x02])
    b2 = bytearray([0x01, 0xFF, 0x01])

    assert Literal.of(b2) > Literal.of(b1)


def test_fixed_unsigned_comparator():
    b1 = bytearray([0x01, 0x01, 0x02])
    b2 = bytearray([0x01, 0xFF, 0x01])

    assert Literal.of(b2) > Literal.of(b1).to(FixedType.of_length(3))


def test_null_handling():
    b1 = bytearray([0x01])

    assert None < Literal.of(b1)
    assert Literal.of(b1) > None
    assert Literal.of(b1).to(FixedType.of_length(3)) == Literal.of(b1).to(FixedType.of_length(4))

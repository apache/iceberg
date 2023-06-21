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
from __future__ import annotations

import datetime
import io
import struct
from decimal import Decimal

from pyiceberg.avro.encoder import BinaryEncoder


def zigzag_encode(datum: int) -> bytes:
    result = []
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
        result.append(struct.pack("B", (datum & 0x7F) | 0x80))
        datum >>= 7
    result.append(struct.pack("B", datum))
    return b"".join(result)


def test_write() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = b"\x12\x34\x56"

    encoder.write(_input)

    assert output.getbuffer() == _input


def test_write_boolean() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    encoder.write_boolean(True)
    encoder.write_boolean(False)

    assert output.getbuffer() == struct.pack("??", True, False)


def test_write_int() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _1byte_input = 2
    _2byte_input = 7466
    _3byte_input = 523490
    _4byte_input = 86561570
    _5byte_input = 2510416930
    _6byte_input = 734929016866
    _7byte_input = 135081528772642
    _8byte_input = 35124861473277986

    encoder.write_int(_1byte_input)
    encoder.write_int(_2byte_input)
    encoder.write_int(_3byte_input)
    encoder.write_int(_4byte_input)
    encoder.write_int(_5byte_input)
    encoder.write_int(_6byte_input)
    encoder.write_int(_7byte_input)
    encoder.write_int(_8byte_input)

    buffer = output.getbuffer()

    assert buffer[0:1] == zigzag_encode(_1byte_input)
    assert buffer[1:3] == zigzag_encode(_2byte_input)
    assert buffer[3:6] == zigzag_encode(_3byte_input)
    assert buffer[6:10] == zigzag_encode(_4byte_input)
    assert buffer[10:15] == zigzag_encode(_5byte_input)
    assert buffer[15:21] == zigzag_encode(_6byte_input)
    assert buffer[21:28] == zigzag_encode(_7byte_input)
    assert buffer[28:36] == zigzag_encode(_8byte_input)


def test_write_float() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = 3.14159265359

    encoder.write_float(_input)

    assert output.getbuffer() == struct.pack("<f", _input)


def test_write_double() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = 3.14159265359

    encoder.write_double(_input)

    assert output.getbuffer() == struct.pack("<d", _input)


def test_write_decimal_bytes() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = Decimal("3.14159265359")

    encoder.write_decimal_bytes(_input)

    assert output.getbuffer() == b"\x0a\x49\x25\x59\xf6\x4f"


def test_write_decimal_fixed() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = Decimal("3.14159265359")

    encoder.write_decimal_fixed(_input, 8)

    assert output.getbuffer() == b"\x00\x00\x00\x49\x25\x59\xf6\x4f"


def test_write_bytes() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = b"\x12\x34\x56"

    encoder.write_bytes(_input)

    assert output.getbuffer() == b"".join([zigzag_encode(len(_input)), _input])


def test_write_bytes_fixed() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = b"\x12\x34\x56"

    encoder.write_bytes_fixed(_input)

    assert output.getbuffer() == _input


def test_write_utf8() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = "That, my liege, is how we know the Earth to be banana-shaped."
    bin_input = _input.encode()

    encoder.write_utf8(_input)

    assert output.getbuffer() == b"".join([zigzag_encode(len(bin_input)), bin_input])


def test_write_date_int() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = datetime.date(1970, 1, 2)

    days = (_input - datetime.date(1970, 1, 1)).days

    encoder.write_date_int(_input)

    assert output.getbuffer() == zigzag_encode(days)


def test_write_time_millis_int() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = datetime.time(1, 2, 3, 456000)
    reference = int((1 * 60 * 60 * 1e6 + 2 * 60 * 1e6 + 3 * 1e6 + 456000) / 1000)

    encoder.write_time_millis_int(_input)

    assert output.getbuffer() == zigzag_encode(reference)


def test_write_time_micros_long() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = datetime.time(1, 2, 3, 456000)
    reference = int(1 * 60 * 60 * 1e6 + 2 * 60 * 1e6 + 3 * 1e6 + 456000)

    encoder.write_time_micros_long(_input)

    assert output.getbuffer() == zigzag_encode(reference)


def test_write_timestamp_millis_long() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = datetime.datetime(2023, 1, 1, 1, 2, 3)

    millis = int((_input - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds() * 1e3)

    encoder.write_timestamp_millis_long(_input)

    assert output.getbuffer() == zigzag_encode(millis)


def test_write_timestamp_micros_long() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = datetime.datetime(2023, 1, 1, 1, 2, 3)

    micros = int((_input - datetime.datetime(1970, 1, 1, 0, 0, 0)).total_seconds() * 1e6)

    encoder.write_timestamp_micros_long(_input)

    assert output.getbuffer() == zigzag_encode(micros)

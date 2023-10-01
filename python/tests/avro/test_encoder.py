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

import io
import struct
import uuid

from pyiceberg.avro.encoder import BinaryEncoder


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

    assert buffer[0:1] == b"\x04"
    assert buffer[1:3] == b"\xd4\x74"
    assert buffer[3:6] == b"\xc4\xf3\x3f"
    assert buffer[6:10] == b"\xc4\xcc\xc6\x52"
    assert buffer[10:15] == b"\xc4\xb0\x8f\xda\x12"
    assert buffer[15:21] == b"\xc4\xe0\xf6\xd2\xe3\x2a"
    assert buffer[21:28] == b"\xc4\xa0\xce\xe8\xe3\xb6\x3d"
    assert buffer[28:36] == b"\xc4\xa0\xb2\xae\x83\xf8\xe4\x7c"


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


def test_write_bytes() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = b"\x12\x34\x56"

    encoder.write_bytes(_input)

    assert output.getbuffer() == b"".join([b"\x06", _input])


def test_write_utf8() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = "That, my liege, is how we know the Earth to be banana-shaped."
    bin_input = _input.encode()
    encoder.write_utf8(_input)

    assert output.getbuffer() == b"".join([b"\x7a", bin_input])


def test_write_uuid() -> None:
    output = io.BytesIO()
    encoder = BinaryEncoder(output)

    _input = uuid.UUID("12345678-1234-5678-1234-567812345678")
    encoder.write_uuid(_input)

    buf = output.getbuffer()
    assert len(buf) == 16
    assert buf.tobytes() == b"\x124Vx\x124Vx\x124Vx\x124Vx"

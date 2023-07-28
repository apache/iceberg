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
from datetime import datetime, timezone
from decimal import Decimal
from io import SEEK_SET
from types import TracebackType
from typing import Optional, Type
from uuid import UUID

import pytest

from pyiceberg.avro.decoder import BinaryDecoder, InMemoryBinaryDecoder, StreamingBinaryDecoder
from pyiceberg.avro.resolver import resolve
from pyiceberg.io import InputStream
from pyiceberg.types import DoubleType, FloatType

AVAILABLE_DECODERS = [StreamingBinaryDecoder, InMemoryBinaryDecoder]


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_decimal_from_fixed(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00\x00\x00\x05\x6A\x48\x1C\xFB\x2C\x7C\x50\x00")
    decoder = decoder_class(mis)
    actual = decoder.read_decimal_from_fixed(28, 15, 12)
    expected = Decimal("99892.123400000000000")
    assert actual == expected


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_boolean_true(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x01")
    decoder = decoder_class(mis)
    assert decoder.read_boolean() is True


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_boolean_false(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00")
    decoder = decoder_class(mis)
    assert decoder.read_boolean() is False


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_skip_boolean(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00")
    decoder = decoder_class(mis)
    assert decoder.tell() == 0
    decoder.skip_boolean()
    assert decoder.tell() == 1


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_int(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x18")
    decoder = decoder_class(mis)
    assert decoder.read_int() == 12


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_skip_int(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x18")
    decoder = decoder_class(mis)
    assert decoder.tell() == 0
    decoder.skip_int()
    assert decoder.tell() == 1


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_decimal(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x18\x00\x00\x00\x05\x6A\x48\x1C\xFB\x2C\x7C\x50\x00")
    decoder = decoder_class(mis)
    actual = decoder.read_decimal_from_bytes(28, 15)
    expected = Decimal("99892.123400000000000")
    assert actual == expected


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_decimal_from_fixed_big(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x0E\xC2\x02\xE9\x06\x16\x33\x49\x77\x67\xA8\x00")
    decoder = decoder_class(mis)
    actual = decoder.read_decimal_from_fixed(28, 15, 12)
    expected = Decimal("4567335489766.998340000000000")
    assert actual == expected


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_negative_bytes(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"")
    decoder = decoder_class(mis)

    with pytest.raises(ValueError) as exc_info:
        decoder.read(-1)

    assert "Requested -1 bytes to read, expected positive integer." in str(exc_info.value)


class OneByteAtATimeInputStream(InputStream):
    """
    Fake input stream that just returns a single byte at the time
    """

    pos = 0

    def read(self, size: int = 0) -> bytes:
        self.pos += 1
        return self.pos.to_bytes(1, byteorder="little")

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        self.pos = offset
        return self.pos

    def tell(self) -> int:
        return self.pos

    def close(self) -> None:
        pass

    def __enter__(self) -> OneByteAtATimeInputStream:
        return self

    def __exit__(
        self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException], exctb: Optional[TracebackType]
    ) -> None:
        self.close()


# InMemoryBinaryDecoder doesn't work for a byte at a time reading
@pytest.mark.parametrize("decoder_class", [StreamingBinaryDecoder])
def test_read_single_byte_at_the_time(decoder_class: Type[BinaryDecoder]) -> None:
    decoder = decoder_class(OneByteAtATimeInputStream())
    assert decoder.read(2) == b"\x01\x02"


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_float(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00\x00\x9A\x41")
    decoder = decoder_class(mis)
    assert decoder.read_float() == 19.25


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_skip_float(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00\x00\x9A\x41")
    decoder = decoder_class(mis)
    assert decoder.tell() == 0
    decoder.skip_float()
    assert decoder.tell() == 4


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_double(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00\x00\x00\x00\x00\x40\x33\x40")
    decoder = decoder_class(mis)
    assert decoder.read_double() == 19.25


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_skip_double(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00\x00\x00\x00\x00\x40\x33\x40")
    decoder = decoder_class(mis)
    assert decoder.tell() == 0
    decoder.skip_double()
    assert decoder.tell() == 8


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_uuid_from_fixed(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x12\x34\x56\x78" * 4)
    decoder = decoder_class(mis)
    assert decoder.read_uuid_from_fixed() == UUID("{12345678-1234-5678-1234-567812345678}")


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_time_millis(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\xBC\x7D")
    decoder = decoder_class(mis)
    assert decoder.read_time_millis().microsecond == 30000


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_time_micros(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\xBC\x7D")
    decoder = decoder_class(mis)
    assert decoder.read_time_micros().microsecond == 8030


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_timestamp_micros(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\xBC\x7D")
    decoder = decoder_class(mis)
    assert decoder.read_timestamp_micros() == datetime(1970, 1, 1, 0, 0, 0, 8030)


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_timestamptz_micros(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\xBC\x7D")
    decoder = decoder_class(mis)
    assert decoder.read_timestamptz_micros() == datetime(1970, 1, 1, 0, 0, 0, 8030, tzinfo=timezone.utc)


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_bytes(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x08\x01\x02\x03\x04")
    decoder = decoder_class(mis)
    actual = decoder.read_bytes()
    assert actual == b"\x01\x02\x03\x04"


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_utf8(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x04\x76\x6F")
    decoder = decoder_class(mis)
    assert decoder.read_utf8() == "vo"


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_skip_utf8(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x04\x76\x6F")
    decoder = decoder_class(mis)
    assert decoder.tell() == 0
    decoder.skip_utf8()
    assert decoder.tell() == 3


@pytest.mark.parametrize("decoder_class", AVAILABLE_DECODERS)
def test_read_int_as_float(decoder_class: Type[BinaryDecoder]) -> None:
    mis = io.BytesIO(b"\x00\x00\x9A\x41")
    decoder = decoder_class(mis)
    reader = resolve(FloatType(), DoubleType())
    assert reader.read(decoder) == 19.25

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import pytest

from pyiceberg.avro.codecs import DeflateCodec
from pyiceberg.avro.file import AvroFileHeader


def get_deflate_compressor() -> None:
    header = AvroFileHeader(bytes(0), {"avro.codec": "deflate"}, bytes(16))
    assert header.compression_codec() == DeflateCodec


def get_null_compressor() -> None:
    header = AvroFileHeader(bytes(0), {"avro.codec": "null"}, bytes(16))
    assert header.compression_codec() is None


def test_unknown_codec() -> None:
    header = AvroFileHeader(bytes(0), {"avro.codec": "unknown"}, bytes(16))

    with pytest.raises(ValueError) as exc_info:
        header.compression_codec()

    assert "Unsupported codec: unknown" in str(exc_info.value)


def test_missing_schema() -> None:
    header = AvroFileHeader(bytes(0), {}, bytes(16))

    with pytest.raises(ValueError) as exc_info:
        header.get_schema()

    assert "No schema found in Avro file headers" in str(exc_info.value)

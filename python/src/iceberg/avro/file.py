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
# pylint: disable=W0621
"""
Avro reader for reading Avro files
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from io import SEEK_SET

from iceberg.avro.codec import KNOWN_CODECS, Codec, NullCodec
from iceberg.avro.decoder import BinaryDecoder
from iceberg.avro.reader import AvroStruct, ConstructReader
from iceberg.io.base import InputFile, InputStream
from iceberg.schema import Schema, visit
from iceberg.types import (
    BinaryType,
    FixedType,
    MapType,
    NestedField,
    StringType,
    StructType,
)
from iceberg.utils.schema_conversion import AvroSchemaConversion

VERSION = 1
MAGIC = bytes(b"Obj" + bytearray([VERSION]))
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
META_SCHEMA = StructType(
    NestedField(name="magic", field_id=100, field_type=FixedType(length=MAGIC_SIZE), required=True),
    NestedField(
        field_id=200,
        name="meta",
        field_type=MapType(key_id=201, key_type=StringType(), value_id=202, value_type=BinaryType(), value_required=True),
        required=True,
    ),
    NestedField(field_id=300, name="sync", field_type=FixedType(length=SYNC_SIZE), required=True),
)


@dataclass(frozen=True)
class AvroFileHeader:
    magic: bytes
    meta: dict[str, str]
    sync: bytes

    def get_compression_codec(self) -> type[Codec]:
        """Get the file's compression codec algorithm from the file's metadata."""
        codec_key = "avro.codec"
        if codec_key in self.meta:
            codec_name = self.meta[codec_key]

            if codec_name not in KNOWN_CODECS:
                raise ValueError(f"Unsupported codec: {codec_name}. (Is it installed?)")

            return KNOWN_CODECS[codec_name]
        else:
            return NullCodec

    def get_schema(self) -> Schema:
        schema_key = "avro.schema"
        if schema_key in self.meta:
            avro_schema_string = self.meta[schema_key]
            avro_schema = json.loads(avro_schema_string)
            return AvroSchemaConversion().avro_to_iceberg(avro_schema)
        else:
            raise ValueError("No schema found in Avro file headers")


class AvroFile:
    input_file: InputFile
    input_stream: InputStream
    header: AvroFileHeader
    schema: Schema
    file_length: int

    decoder: BinaryDecoder
    block_decoder: BinaryDecoder

    block_records: int = -1
    block_position: int = 0
    block: int = 0

    def __init__(self, input_file: InputFile) -> None:
        self.input_file = input_file

    def __enter__(self):
        """
        Opens the file and reads the header and generates
        a reader tree to start reading the payload

        Returns:
            A generator returning the AvroStructs
        """
        self.input_stream = self.input_file.open()
        self.decoder = BinaryDecoder(self.input_stream)
        self.header = self._read_header()
        self.schema = self.header.get_schema()
        self.file_length = len(self.input_file)
        self.reader = visit(self.schema, ConstructReader())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.input_stream.close()

    def __iter__(self) -> AvroFile:
        return self

    def _read_block(self) -> int:
        if self.block > 0:
            sync_marker_len = len(self.header.sync)
            sync_marker = self.decoder.read(sync_marker_len)
            if sync_marker != self.header.sync:
                raise ValueError(f"Expected sync bytes {self.header.sync!r}, but got {sync_marker!r}")
            if self.is_EOF():
                raise StopIteration
        self.block = self.block + 1
        self.block_records = self.decoder.read_long()
        self.block_decoder = self.header.get_compression_codec().decompress(self.decoder)
        self.block_position = 0
        return self.block_records

    def __next__(self) -> AvroStruct:
        if self.block_position < self.block_records:
            self.block_position = self.block_position + 1
            return self.reader.read(self.block_decoder)

        new_block = self._read_block()

        if new_block > 0:
            return self.__next__()
        else:
            raise StopIteration

    def _read_header(self) -> AvroFileHeader:
        self.input_stream.seek(0, SEEK_SET)
        reader = visit(META_SCHEMA, ConstructReader())
        _header = reader.read(self.decoder)
        meta = {k: v.decode("utf-8") for k, v in _header.get(1).items()}
        return AvroFileHeader(magic=_header.get(0), meta=meta, sync=_header.get(2))

    def is_EOF(self) -> bool:
        return self.input_stream.tell() == self.file_length

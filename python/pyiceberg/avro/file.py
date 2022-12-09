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
from io import SEEK_SET, BufferedReader
from types import TracebackType
from typing import Optional, Type

from pyiceberg.avro.codecs import KNOWN_CODECS, Codec
from pyiceberg.avro.decoder import BinaryDecoder
from pyiceberg.avro.reader import AvroStruct, ConstructReader, Reader
from pyiceberg.avro.resolver import resolve
from pyiceberg.io import InputFile, InputStream
from pyiceberg.io.memory import MemoryInputStream
from pyiceberg.schema import Schema, visit
from pyiceberg.types import (
    FixedType,
    MapType,
    NestedField,
    StringType,
    StructType,
)
from pyiceberg.utils.schema_conversion import AvroSchemaConversion

VERSION = 1
MAGIC = bytes(b"Obj" + bytearray([VERSION]))
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
META_SCHEMA = StructType(
    NestedField(name="magic", field_id=100, field_type=FixedType(length=MAGIC_SIZE), required=True),
    NestedField(
        field_id=200,
        name="meta",
        field_type=MapType(key_id=201, key_type=StringType(), value_id=202, value_type=StringType(), value_required=True),
        required=True,
    ),
    NestedField(field_id=300, name="sync", field_type=FixedType(length=SYNC_SIZE), required=True),
)

_CODEC_KEY = "avro.codec"
_SCHEMA_KEY = "avro.schema"


@dataclass(frozen=True)
class AvroFileHeader:
    magic: bytes
    meta: dict[str, str]
    sync: bytes

    def compression_codec(self) -> Optional[Type[Codec]]:
        """Get the file's compression codec algorithm from the file's metadata.

        In the case of a null codec, we return a None indicating that we
        don't need to compress/decompress
        """
        codec_name = self.meta.get(_CODEC_KEY, "null")
        if codec_name not in KNOWN_CODECS:
            raise ValueError(f"Unsupported codec: {codec_name}")

        return KNOWN_CODECS[codec_name]

    def get_schema(self) -> Schema:
        if _SCHEMA_KEY in self.meta:
            avro_schema_string = self.meta[_SCHEMA_KEY]
            avro_schema = json.loads(avro_schema_string)
            return AvroSchemaConversion().avro_to_iceberg(avro_schema)
        else:
            raise ValueError("No schema found in Avro file headers")


@dataclass
class Block:
    reader: Reader
    block_records: int
    block_decoder: BinaryDecoder
    position: int = 0

    def __iter__(self) -> Block:
        return self

    def has_next(self) -> bool:
        return self.position < self.block_records

    def __next__(self) -> AvroStruct:
        if self.has_next():
            self.position += 1
            return self.reader.read(self.block_decoder)
        raise StopIteration


class AvroFile:
    input_file: InputFile
    read_schema: Optional[Schema]
    input_stream: InputStream
    header: AvroFileHeader
    schema: Schema
    reader: Reader

    decoder: BinaryDecoder
    block: Optional[Block] = None

    def __init__(self, input_file: InputFile, read_schema: Optional[Schema] = None) -> None:
        self.input_file = input_file
        self.read_schema = read_schema

    def __enter__(self) -> AvroFile:
        """
        Opens the file and reads the header and generates
        a reader tree to start reading the payload

        Returns:
            A generator returning the AvroStructs
        """
        self.input_stream = BufferedReader(self.input_file.open())  # type: ignore
        self.decoder = BinaryDecoder(self.input_stream)
        self.header = self._read_header()
        self.schema = self.header.get_schema()
        if not self.read_schema:
            self.reader = visit(self.schema, ConstructReader())
        else:
            self.reader = resolve(self.schema, self.read_schema)

        return self

    def __exit__(
        self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException], exctb: Optional[TracebackType]
    ) -> None:
        self.input_stream.close()

    def __iter__(self) -> AvroFile:
        return self

    def _read_block(self) -> int:
        # If there is already a block, we'll have the sync bytes
        if self.block:
            sync_marker = self.decoder.read(SYNC_SIZE)
            if sync_marker != self.header.sync:
                raise ValueError(f"Expected sync bytes {self.header.sync!r}, but got {sync_marker!r}")
        block_records = self.decoder.read_int()

        block_bytes_len = self.decoder.read_int()
        block_bytes = self.decoder.read(block_bytes_len)
        if codec := self.header.compression_codec():
            block_bytes = codec.decompress(block_bytes)

        self.block = Block(
            reader=self.reader, block_records=block_records, block_decoder=BinaryDecoder(MemoryInputStream(block_bytes))
        )
        return block_records

    def __next__(self) -> AvroStruct:
        if self.block and self.block.has_next():
            return next(self.block)

        try:
            new_block = self._read_block()
        except EOFError as exc:
            raise StopIteration from exc

        if new_block > 0:
            return self.__next__()
        raise StopIteration

    def _read_header(self) -> AvroFileHeader:
        self.input_stream.seek(0, SEEK_SET)
        reader = visit(META_SCHEMA, ConstructReader())
        _header = reader.read(self.decoder)
        return AvroFileHeader(magic=_header.get(0), meta=_header.get(1), sync=_header.get(2))

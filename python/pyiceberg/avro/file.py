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
"""Avro reader for reading Avro files."""
from __future__ import annotations

import io
import json
import os
from dataclasses import dataclass
from enum import Enum
from types import TracebackType
from typing import (
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)

from pyiceberg.avro.codecs import KNOWN_CODECS, Codec
from pyiceberg.avro.decoder import BinaryDecoder, new_decoder
from pyiceberg.avro.encoder import BinaryEncoder
from pyiceberg.avro.reader import Reader
from pyiceberg.avro.resolver import construct_reader, construct_writer, resolve
from pyiceberg.avro.writer import Writer
from pyiceberg.io import InputFile, OutputFile, OutputStream
from pyiceberg.schema import Schema
from pyiceberg.typedef import EMPTY_DICT, Record, StructProtocol
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


class AvroFileHeader(Record):
    __slots__ = ("magic", "meta", "sync")
    magic: bytes
    meta: Dict[str, str]
    sync: bytes

    def compression_codec(self) -> Optional[Type[Codec]]:
        """Get the file's compression codec algorithm from the file's metadata.

        In the case of a null codec, we return a None indicating that we
        don't need to compress/decompress.
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


D = TypeVar("D", bound=StructProtocol)


@dataclass
class Block(Generic[D]):
    reader: Reader
    block_records: int
    block_decoder: BinaryDecoder
    position: int = 0

    def __iter__(self) -> Block[D]:
        """Return an iterator for the Block class."""
        return self

    def has_next(self) -> bool:
        return self.position < self.block_records

    def __next__(self) -> D:
        """Return the next item when iterating over the Block class."""
        if self.has_next():
            self.position += 1
            return self.reader.read(self.block_decoder)
        raise StopIteration


class AvroFile(Generic[D]):
    __slots__ = (
        "input_file",
        "read_schema",
        "read_types",
        "read_enums",
        "header",
        "schema",
        "reader",
        "decoder",
        "block",
    )
    input_file: InputFile
    read_schema: Optional[Schema]
    read_types: Dict[int, Callable[..., StructProtocol]]
    read_enums: Dict[int, Callable[..., Enum]]
    header: AvroFileHeader
    schema: Schema
    reader: Reader

    decoder: BinaryDecoder
    block: Optional[Block[D]]

    def __init__(
        self,
        input_file: InputFile,
        read_schema: Optional[Schema] = None,
        read_types: Dict[int, Callable[..., StructProtocol]] = EMPTY_DICT,
        read_enums: Dict[int, Callable[..., Enum]] = EMPTY_DICT,
    ) -> None:
        self.input_file = input_file
        self.read_schema = read_schema
        self.read_types = read_types
        self.read_enums = read_enums
        self.block = None

    def __enter__(self) -> AvroFile[D]:
        """Generate a reader tree for the payload within an avro file.

        Return:
            A generator returning the AvroStructs.
        """
        with self.input_file.open() as f:
            self.decoder = new_decoder(f.read())
        self.header = self._read_header()
        self.schema = self.header.get_schema()
        if not self.read_schema:
            self.read_schema = self.schema

        self.reader = resolve(self.schema, self.read_schema, self.read_types, self.read_enums)

        return self

    def __exit__(
        self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException], exctb: Optional[TracebackType]
    ) -> None:
        """Perform cleanup when exiting the scope of a 'with' statement."""

    def __iter__(self) -> AvroFile[D]:
        """Return an iterator for the AvroFile class."""
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

        self.block = Block(reader=self.reader, block_records=block_records, block_decoder=new_decoder(block_bytes))
        return block_records

    def __next__(self) -> D:
        """Return the next item when iterating over the AvroFile class."""
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
        return construct_reader(META_SCHEMA, {-1: AvroFileHeader}).read(self.decoder)


class AvroOutputFile(Generic[D]):
    output_file: OutputFile
    output_stream: OutputStream
    schema: Schema
    schema_name: str
    encoder: BinaryEncoder
    sync_bytes: bytes
    writer: Writer

    def __init__(self, output_file: OutputFile, schema: Schema, schema_name: str, metadata: Dict[str, str] = EMPTY_DICT) -> None:
        self.output_file = output_file
        self.schema = schema
        self.schema_name = schema_name
        self.sync_bytes = os.urandom(SYNC_SIZE)
        self.writer = construct_writer(self.schema)
        self.metadata = metadata

    def __enter__(self) -> AvroOutputFile[D]:
        """
        Open the file and writes the header.

        Returns:
            The file object to write records to
        """
        self.output_stream = self.output_file.create(overwrite=True)
        self.encoder = BinaryEncoder(self.output_stream)

        self._write_header()
        self.writer = construct_writer(self.schema)

        return self

    def __exit__(
        self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException], exctb: Optional[TracebackType]
    ) -> None:
        """Perform cleanup when exiting the scope of a 'with' statement."""
        self.output_stream.close()

    def _write_header(self) -> None:
        json_schema = json.dumps(AvroSchemaConversion().iceberg_to_avro(self.schema, schema_name=self.schema_name))
        meta = {**self.metadata, _SCHEMA_KEY: json_schema, _CODEC_KEY: "null"}
        header = AvroFileHeader(magic=MAGIC, meta=meta, sync=self.sync_bytes)
        construct_writer(META_SCHEMA).write(self.encoder, header)

    def write_block(self, objects: List[D]) -> None:
        in_memory = io.BytesIO()
        block_content_encoder = BinaryEncoder(output_stream=in_memory)
        for obj in objects:
            self.writer.write(block_content_encoder, obj)
        block_content = in_memory.getvalue()

        self.encoder.write_int(len(objects))
        self.encoder.write_int(len(block_content))
        self.encoder.write(block_content)
        self.encoder.write(self.sync_bytes)

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
from dataclasses import dataclass, field
from functools import singledispatchmethod
from io import SEEK_SET
from typing import Any, Union

from iceberg.avro.codec import KNOWN_CODECS, Codec, NullCodec
from iceberg.avro.decoder import BinaryDecoder
from iceberg.files import StructProtocol
from iceberg.io.base import InputFile, InputStream
from iceberg.schema import Schema, SchemaVisitor, visit
from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)
from iceberg.utils.schema_conversion import AvroSchemaConversion

VERSION = 1
MAGIC = bytes(b"Obj" + bytearray([VERSION]))
MAGIC_SIZE = len(MAGIC)
SYNC_SIZE = 16
META_SCHEMA = StructType(
    NestedField(name="magic", field_id=100, field_type=FixedType(length=MAGIC_SIZE), is_optional=False),
    NestedField(
        field_id=200,
        name="meta",
        field_type=MapType(key_id=201, key_type=StringType(), value_id=202, value_type=BinaryType()),
        is_optional=False,
    ),
    NestedField(field_id=300, name="sync", field_type=FixedType(length=SYNC_SIZE), is_optional=False),
)


@dataclass(frozen=True)
class AvroStruct(StructProtocol):
    _data: list[Any | StructProtocol] = field(default_factory=list)

    def set(self, pos: int, value: Any) -> None:
        self._data[pos] = value

    def get(self, pos: int) -> Any:
        return self._data[pos]


class _AvroReader(SchemaVisitor[Union[AvroStruct, Any]]):
    _skip: bool = False

    def __init__(self, decoder: BinaryDecoder):
        self._decoder = decoder

    def schema(self, schema: Schema, struct_result: AvroStruct | Any) -> AvroStruct | Any:
        return struct_result

    def struct(self, struct: StructType, field_results: list[AvroStruct | Any]) -> AvroStruct | Any:
        return AvroStruct(field_results)

    def before_field(self, field: NestedField) -> None:
        if field.is_optional:
            pos = self._decoder.read_long()
            # We now assume that null is first (which is often the case)
            if int(pos) == 0:
                self._skip = True

    def field(self, field: NestedField, field_result: AvroStruct | Any) -> AvroStruct | Any:
        return field_result

    def before_list_element(self, element: NestedField) -> None:
        self._skip = True

    def list(self, list_type: ListType, element_result: AvroStruct | Any) -> AvroStruct | Any:
        read_items = []
        block_count = self._decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                # We ignore the block size for now
                _ = self._decoder.read_long()
            for _ in range(block_count):
                read_items.append(visit(list_type.element_type, self))
            block_count = self._decoder.read_long()
        return read_items

    def before_map_key(self, key: NestedField) -> None:
        self._skip = True

    def before_map_value(self, value: NestedField) -> None:
        self._skip = True

    def map(self, map_type: MapType, key_result: AvroStruct | Any, value_result: AvroStruct | Any) -> AvroStruct | Any:
        read_items = {}

        block_count = self._decoder.read_long()
        if block_count < 0:
            block_count = -block_count
            # We ignore the block size for now
            _ = self._decoder.read_long()

        # The Iceberg non-string implementation with an array of records:
        while block_count != 0:
            for _ in range(block_count):
                key = visit(map_type.key_type, self)
                read_items[key] = visit(map_type.value_type, self)
            block_count = self._decoder.read_long()

        return read_items

    @singledispatchmethod
    def read(self, primitive: PrimitiveType):
        raise ValueError(f"Unknown type: {primitive}")

    @read.register(FixedType)
    def _(self, primitive: FixedType) -> Any:
        return self._decoder.read(primitive.length)

    @read.register(DecimalType)
    def _(self, primitive: DecimalType) -> Any:
        return self._decoder.read_decimal_from_bytes(primitive.scale, primitive.precision)

    @read.register(BooleanType)
    def _(self, primitive: BooleanType) -> Any:
        return self._decoder.read_boolean()

    @read.register(IntegerType)
    def _(self, primitive: IntegerType) -> Any:
        return self._decoder.read_int()

    @read.register(LongType)
    def _(self, primitive: LongType) -> Any:
        return self._decoder.read_long()

    @read.register(FloatType)
    def _(self, primitive: FloatType) -> Any:
        return self._decoder.read_float()

    @read.register(DoubleType)
    def _(self, primitive: DoubleType) -> Any:
        return self._decoder.read_double()

    @read.register(DateType)
    def _(self, primitive: DateType) -> Any:
        return self._decoder.read_date_from_int()

    @read.register(TimeType)
    def _(self, primitive: TimeType) -> Any:
        return self._decoder.read_time_micros_from_long()

    @read.register(TimestampType)
    def _(self, primitive: TimestampType) -> Any:
        return self._decoder.read_timestamp_micros_from_long()

    @read.register(TimestamptzType)
    def _(self, primitive: TimestamptzType) -> Any:
        return self._decoder.read_timestamp_micros_from_long()

    @read.register(StringType)
    def _(self, primitive: StringType) -> Any:
        return self._decoder.read_utf8()

    @read.register(BinaryType)
    def _(self, primitive: StringType) -> Any:
        return self._decoder.read_bytes()

    def primitive(self, primitive: PrimitiveType) -> AvroStruct | Any:
        if self._skip:
            self._skip = False
            return None
        else:
            return self.read(primitive)


@dataclass(frozen=True)
class AvroHeader:
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


class DataFileReader:
    input_file: InputFile
    input_stream: InputStream
    header: AvroHeader
    schema: Schema
    file_length: int

    decoder: BinaryDecoder
    block_decoder: BinaryDecoder

    block_records: int = -1
    block_position: int = 0
    block: int = 0

    def __init__(self, input_file: InputFile) -> None:
        """
        Opens the file
        """
        self.input_file = input_file

    def __enter__(self):
        self.input_stream = self.input_file.open()
        self.decoder = BinaryDecoder(self.input_stream)
        self.header = self._read_header()
        self.schema = self.header.get_schema()
        self.file_length = len(self.input_file)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.input_stream.close()

    def __iter__(self) -> DataFileReader:
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
            return visit(self.schema, _AvroReader(self.block_decoder))

        new_block = self._read_block()

        if new_block > 0:
            return self.__next__()
        else:
            raise StopIteration

    def _read_header(self) -> AvroHeader:
        self.input_stream.seek(0, SEEK_SET)
        _header = visit(META_SCHEMA, _AvroReader(self.decoder))
        meta = {k: v.decode("utf-8") for k, v in _header.get(1).items()}
        return AvroHeader(magic=_header.get(0), meta=meta, sync=_header.get(2))

    def is_EOF(self) -> bool:
        return self.input_stream.tell() == self.file_length

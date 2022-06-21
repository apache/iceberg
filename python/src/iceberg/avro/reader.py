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
"""
Classes for building the Reader tree

Constructing a reader tree from the schema makes it easy
to decouple the reader implementation from the schema.

The reader tree can be changed in such a way that the
read schema is different, while respecting the read schema
"""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field as dataclassfield
from datetime import date, datetime, time
from decimal import Decimal
from functools import singledispatch
from typing import Any
from uuid import UUID

from iceberg.avro.decoder import BinaryDecoder
from iceberg.files import StructProtocol
from iceberg.schema import Schema, SchemaVisitor
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
from iceberg.utils.singleton import Singleton


@dataclass(frozen=True)
class AvroStruct(StructProtocol):
    _data: list[Any | StructProtocol] = dataclassfield()

    def set(self, pos: int, value: Any) -> None:
        self._data[pos] = value

    def get(self, pos: int) -> Any:
        return self._data[pos]


class Reader(Singleton):
    @abstractmethod
    def read(self, decoder: BinaryDecoder) -> Any:
        ...


class NoneReader(Reader):
    def read(self, _: BinaryDecoder) -> None:
        return None


class BooleanReader(Reader):
    def read(self, decoder: BinaryDecoder) -> bool:
        return decoder.read_boolean()


class IntegerReader(Reader):
    def read(self, decoder: BinaryDecoder) -> int:
        return decoder.read_int()


class LongReader(Reader):
    def read(self, decoder: BinaryDecoder) -> int:
        return decoder.read_long()


class FloatReader(Reader):
    def read(self, decoder: BinaryDecoder) -> float:
        return decoder.read_float()


class DoubleReader(Reader):
    def read(self, decoder: BinaryDecoder) -> float:
        return decoder.read_double()


class DateReader(Reader):
    def read(self, decoder: BinaryDecoder) -> date:
        return decoder.read_date_from_int()


class TimeReader(Reader):
    def read(self, decoder: BinaryDecoder) -> time:
        return decoder.read_time_micros()


class TimestampReader(Reader):
    def read(self, decoder: BinaryDecoder) -> datetime:
        return decoder.read_timestamp_micros()


class TimestamptzReader(Reader):
    def read(self, decoder: BinaryDecoder) -> datetime:
        return decoder.read_timestamptz_micros()


class StringReader(Reader):
    def read(self, decoder: BinaryDecoder) -> str:
        return decoder.read_utf8()


class UUIDReader(Reader):
    def read(self, decoder: BinaryDecoder) -> UUID:
        return UUID(decoder.read_utf8())


@dataclass(frozen=True)
class FixedReader(Reader):
    length: int = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> bytes:
        return decoder.read(self.length)


class BinaryReader(Reader):
    def read(self, decoder: BinaryDecoder) -> bytes:
        return decoder.read_bytes()


@dataclass(frozen=True)
class DecimalReader(Reader):
    precision: int = dataclassfield()
    scale: int = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> Decimal:
        return decoder.read_decimal_from_bytes(self.precision, self.scale)


@dataclass(frozen=True)
class OptionReader(Reader):
    option: Reader = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> Any | None:
        # For the Iceberg spec it is required to set the default value to null
        # From https://iceberg.apache.org/spec/#avro
        # Optional fields must always set the Avro field default value to null.
        #
        # This means that null has to come first:
        # https://avro.apache.org/docs/current/spec.html
        # type of the default value must match the first element of the union.
        # This is enforced in the schema conversion, which happens prior
        # to building the reader tree
        if decoder.read_int() > 0:
            return self.option.read(decoder)
        return None


@dataclass(frozen=True)
class StructReader(Reader):
    fields: tuple[Reader, ...] = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> AvroStruct:
        return AvroStruct([field.read(decoder) for field in self.fields])


@dataclass(frozen=True)
class ListReader(Reader):
    element: Reader

    def read(self, decoder: BinaryDecoder) -> list:
        read_items = []
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                # We ignore the block size for now
                _ = decoder.read_long()
            for _ in range(block_count):
                read_items.append(self.element.read(decoder))
            block_count = decoder.read_long()
        return read_items


@dataclass(frozen=True)
class MapReader(Reader):
    key: Reader
    value: Reader

    def read(self, decoder: BinaryDecoder) -> dict:
        read_items = {}
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                # We ignore the block size for now
                _ = decoder.read_long()
            for _ in range(block_count):
                key = self.key.read(decoder)
                read_items[key] = self.value.read(decoder)
            block_count = decoder.read_long()

        return read_items


class ConstructReader(SchemaVisitor[Reader]):
    def schema(self, schema: Schema, struct_result: Reader) -> Reader:
        return struct_result

    def struct(self, struct: StructType, field_results: list[Reader]) -> Reader:
        return StructReader(tuple(field_results))

    def field(self, field: NestedField, field_result: Reader) -> Reader:
        return field_result if field.required else OptionReader(field_result)

    def list(self, list_type: ListType, element_result: Reader) -> Reader:
        element_reader = element_result if list_type.element_required else OptionReader(element_result)
        return ListReader(element_reader)

    def map(self, map_type: MapType, key_result: Reader, value_result: Reader) -> Reader:
        value_reader = value_result if map_type.value_required else OptionReader(value_result)
        return MapReader(key_result, value_reader)

    def primitive(self, primitive: PrimitiveType) -> Reader:
        return primitive_reader(primitive)


@singledispatch
def primitive_reader(primitive: PrimitiveType) -> Reader:
    raise ValueError(f"Unknown type: {primitive}")


@primitive_reader.register(FixedType)
def _(primitive: FixedType) -> Reader:
    return FixedReader(primitive.length)


@primitive_reader.register(DecimalType)
def _(primitive: DecimalType) -> Reader:
    return DecimalReader(primitive.precision, primitive.scale)


@primitive_reader.register(BooleanType)
def _(_: BooleanType) -> Reader:
    return BooleanReader()


@primitive_reader.register(IntegerType)
def _(_: IntegerType) -> Reader:
    return IntegerReader()


@primitive_reader.register(LongType)
def _(_: LongType) -> Reader:
    return LongReader()


@primitive_reader.register(FloatType)
def _(_: FloatType) -> Reader:
    return FloatReader()


@primitive_reader.register(DoubleType)
def _(_: DoubleType) -> Reader:
    return DoubleReader()


@primitive_reader.register(DateType)
def _(_: DateType) -> Reader:
    return DateReader()


@primitive_reader.register(TimeType)
def _(_: TimeType) -> Reader:
    return TimeReader()


@primitive_reader.register(TimestampType)
def _(_: TimestampType) -> Reader:
    return TimestampReader()


@primitive_reader.register(TimestamptzType)
def _(_: TimestamptzType) -> Reader:
    return TimestamptzReader()


@primitive_reader.register(StringType)
def _(_: StringType) -> Reader:
    return StringReader()


@primitive_reader.register(BinaryType)
def _(_: StringType) -> Reader:
    return BinaryReader()

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
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from uuid import UUID

from pyiceberg.avro.decoder import BinaryDecoder
from pyiceberg.schema import Schema, SchemaVisitor, SchemaVisitorPerPrimitiveType, T
from pyiceberg.typedef import StructProtocol
from pyiceberg.types import (
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
    UUIDType,
)
from pyiceberg.utils.singleton import Singleton


def _skip_map_array(decoder: BinaryDecoder, skip_entry: Callable[[], None]) -> None:
    """Skips over an array or map

    Both the array and map are encoded similar, and we can re-use
    the logic of skipping in an efficient way.

    From the Avro spec:

    Maps (and arrays) are encoded as a series of blocks.
    Each block consists of a long count value, followed by that many key/value pairs in the case of a map,
    and followed by that many array items in the case of an array. A block with count zero indicates the
    end of the map. Each item is encoded per the map's value schema.

    If a block's count is negative, its absolute value is used, and the count is followed immediately by a
    long block size indicating the number of bytes in the block. This block size permits fast skipping
    through data, e.g., when projecting a record to a subset of its fields.

    Args:
        decoder:
            The decoder that reads the types from the underlying data
        skip_entry:
            Function to skip over the underlying data, element in case of an array, and the
            key/value in the case of a map
    """
    block_count = decoder.read_int()
    while block_count != 0:
        if block_count < 0:
            # The length in bytes in encoded, so we can skip over it right away
            block_size = decoder.read_int()
            decoder.skip(block_size)
        else:
            for _ in range(block_count):
                skip_entry()
        block_count = decoder.read_int()


@dataclass(frozen=True)
class AvroStruct(StructProtocol):
    _data: List[Union[Any, StructProtocol]] = dataclassfield()

    def set(self, pos: int, value: Any) -> None:
        self._data[pos] = value

    def get(self, pos: int) -> Any:
        return self._data[pos]


class Reader(Singleton):
    @abstractmethod
    def read(self, decoder: BinaryDecoder) -> Any:
        ...

    @abstractmethod
    def skip(self, decoder: BinaryDecoder) -> None:
        ...


class NoneReader(Reader):
    def read(self, _: BinaryDecoder) -> None:
        return None

    def skip(self, decoder: BinaryDecoder) -> None:
        return None


class BooleanReader(Reader):
    def read(self, decoder: BinaryDecoder) -> bool:
        return decoder.read_boolean()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_boolean()


class IntegerReader(Reader):
    """Longs and ints are encoded the same way, and there is no long in Python"""

    def read(self, decoder: BinaryDecoder) -> int:
        return decoder.read_int()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_int()


class FloatReader(Reader):
    def read(self, decoder: BinaryDecoder) -> float:
        return decoder.read_float()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_float()


class DoubleReader(Reader):
    def read(self, decoder: BinaryDecoder) -> float:
        return decoder.read_double()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_double()


class DateReader(Reader):
    def read(self, decoder: BinaryDecoder) -> date:
        return decoder.read_date_from_int()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_int()


class TimeReader(Reader):
    def read(self, decoder: BinaryDecoder) -> time:
        return decoder.read_time_micros()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_int()


class TimestampReader(Reader):
    def read(self, decoder: BinaryDecoder) -> datetime:
        return decoder.read_timestamp_micros()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_int()


class TimestamptzReader(Reader):
    def read(self, decoder: BinaryDecoder) -> datetime:
        return decoder.read_timestamptz_micros()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_int()


class StringReader(Reader):
    def read(self, decoder: BinaryDecoder) -> str:
        return decoder.read_utf8()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_utf8()


class UUIDFixedReader(Reader):
    def read(self, decoder: BinaryDecoder) -> UUID:
        return decoder.read_uuid_from_fixed()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip(16)


@dataclass(frozen=True)
class FixedReader(Reader):
    _len: int = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> bytes:
        return decoder.read(len(self))

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip(len(self))

    def __len__(self) -> int:
        return self._len


class BinaryReader(Reader):
    def read(self, decoder: BinaryDecoder) -> bytes:
        return decoder.read_bytes()

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_bytes()


@dataclass(frozen=True)
class DecimalReader(Reader):
    precision: int = dataclassfield()
    scale: int = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> Decimal:
        return decoder.read_decimal_from_bytes(self.precision, self.scale)

    def skip(self, decoder: BinaryDecoder) -> None:
        decoder.skip_bytes()


@dataclass(frozen=True)
class OptionReader(Reader):
    option: Reader = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> Optional[Any]:
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

    def skip(self, decoder: BinaryDecoder) -> None:
        if decoder.read_int() > 0:
            return self.option.skip(decoder)


@dataclass(frozen=True)
class StructReader(Reader):
    fields: Tuple[Tuple[Optional[int], Reader], ...] = dataclassfield()

    def read(self, decoder: BinaryDecoder) -> AvroStruct:
        result: List[Union[Any, StructProtocol]] = [None] * len(self.fields)
        for (pos, field) in self.fields:
            if pos is not None:
                result[pos] = field.read(decoder)
            else:
                field.skip(decoder)

        return AvroStruct(result)

    def skip(self, decoder: BinaryDecoder) -> None:
        for _, field in self.fields:
            field.skip(decoder)


@dataclass(frozen=True)
class ListReader(Reader):
    element: Reader

    def read(self, decoder: BinaryDecoder) -> List[Any]:
        read_items = []
        block_count = decoder.read_int()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                _ = decoder.read_int()
            for _ in range(block_count):
                read_items.append(self.element.read(decoder))
            block_count = decoder.read_int()
        return read_items

    def skip(self, decoder: BinaryDecoder) -> None:
        _skip_map_array(decoder, lambda: self.element.skip(decoder))


@dataclass(frozen=True)
class MapReader(Reader):
    key: Reader
    value: Reader

    def read(self, decoder: BinaryDecoder) -> Dict[Any, Any]:
        read_items = {}
        block_count = decoder.read_int()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                # We ignore the block size for now
                _ = decoder.read_int()
            for _ in range(block_count):
                key = self.key.read(decoder)
                read_items[key] = self.value.read(decoder)
            block_count = decoder.read_int()

        return read_items

    def skip(self, decoder: BinaryDecoder) -> None:
        def skip() -> None:
            self.key.skip(decoder)
            self.value.skip(decoder)

        _skip_map_array(decoder, skip)


class ConstructReader(SchemaVisitorPerPrimitiveType[Reader]):

    def schema(self, schema: Schema, struct_result: Reader) -> Reader:
        return struct_result

    def struct(self, struct: StructType, field_results: List[Reader]) -> Reader:
        return StructReader(tuple(enumerate(field_results)))

    def field(self, field: NestedField, field_result: Reader) -> Reader:
        return field_result if field.required else OptionReader(field_result)

    def list(self, list_type: ListType, element_result: Reader) -> Reader:
        element_reader = element_result if list_type.element_required else OptionReader(element_result)
        return ListReader(element_reader)

    def map(self, map_type: MapType, key_result: Reader, value_result: Reader) -> Reader:
        value_reader = value_result if map_type.value_required else OptionReader(value_result)
        return MapReader(key_result, value_reader)

    def visit_fixed(self, fixed_type: FixedType) -> T:
        return FixedReader(len(fixed_type))

    def visit_decimal(self, decimal_type: DecimalType) -> T:
        return DecimalReader(decimal_type.precision, decimal_type.scale)

    def visit_boolean(self, boolean_type: BooleanType) -> T:
        return BooleanReader()

    def visit_integer(self, integer_type: IntegerType) -> T:
        return IntegerReader()

    def visit_long(self, long_type: LongType) -> T:
        return IntegerReader()

    def visit_float(self, float_type: FloatType) -> T:
        return FloatReader()

    def visit_double(self, double_type: DoubleType) -> T:
        return DoubleReader()

    def visit_date(self, date_type: DateType) -> T:
        return DateReader()

    def visit_time(self, time_type: TimeType) -> T:
        return TimeReader()

    def visit_timestamp(self, timestamp_type: TimestampType) -> T:
        return TimestampReader()

    def visit_timestampz(self, timestamptz_type: TimestamptzType) -> T:
        return TimestamptzReader()

    def visit_string(self, string_type: StringType) -> T:
        return StringReader()

    def visit_uuid(self, uuid_type: UUIDType) -> T:
        return UUIDReader()

    def visit_binary(self, binary_ype: BinaryType) -> T:
        return BinaryReader()

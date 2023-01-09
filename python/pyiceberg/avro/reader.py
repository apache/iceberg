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
from datetime import datetime, time
from decimal import Decimal
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
)
from uuid import UUID

from pyiceberg.avro.decoder import BinaryDecoder
from pyiceberg.typedef import Record, StructProtocol
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
    def read(self, decoder: BinaryDecoder) -> int:
        return decoder.read_int()

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


class UUIDReader(Reader):
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


class StructProtocolReader(Reader):
    create_struct: Callable[[], StructProtocol]
    fields: Tuple[Tuple[Optional[int], Reader], ...]

    def __init__(self, fields: Tuple[Tuple[Optional[int], Reader], ...], create_struct: Callable[[], StructProtocol]):
        self.create_struct = create_struct
        self.fields = fields

    def create_or_reuse(self, reuse: Optional[StructProtocol]) -> StructProtocol:
        if reuse:
            return reuse
        else:
            return self.create_struct()

    def read(self, decoder: BinaryDecoder) -> Any:
        struct = self.create_or_reuse(None)

        for (pos, field) in self.fields:
            if pos is not None:
                struct.set(pos, field.read(decoder))  # later: pass reuse in here
            else:
                field.skip(decoder)

        return struct

    def skip(self, decoder: BinaryDecoder) -> None:
        for _, field in self.fields:
            field.skip(decoder)


class StructReader(StructProtocolReader):
    fields: Tuple[Tuple[Optional[int], Reader], ...]

    def __init__(self, fields: Tuple[Tuple[Optional[int], Reader], ...]):
        super().__init__(fields, lambda: Record.of(len(fields)))


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

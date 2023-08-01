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
Classes for building the Writer tree.

Constructing a writer tree from the schema makes it easy
to decouple the writing implementation from the schema.
"""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field as dataclassfield
from datetime import datetime, time
from typing import (
    Any,
    Dict,
    List,
    Tuple,
)
from uuid import UUID

from pyiceberg.avro.encoder import BinaryEncoder
from pyiceberg.types import StructType
from pyiceberg.utils.singleton import Singleton


class Writer(Singleton):
    @abstractmethod
    def write(self, encoder: BinaryEncoder, val: Any) -> Any:
        ...

    def __repr__(self) -> str:
        """Returns string representation of this object."""
        return f"{self.__class__.__name__}()"


class NoneWriter(Writer):
    def write(self, _: BinaryEncoder, __: Any) -> None:
        pass


class BooleanWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: bool) -> None:
        encoder.write_boolean(val)


class IntegerWriter(Writer):
    """Longs and ints are encoded the same way, and there is no long in Python."""

    def write(self, encoder: BinaryEncoder, val: int) -> None:
        encoder.write_int(val)


class FloatWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: float) -> None:
        encoder.write_float(val)


class DoubleWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: float) -> None:
        encoder.write_double(val)


class DateWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: Any) -> None:
        encoder.write_date_int(val)


class TimeWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: time) -> None:
        encoder.write_time_micros_long(val)


class TimestampWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: datetime) -> None:
        encoder.write_timestamp_micros_long(val)


class TimestamptzWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: datetime) -> None:
        encoder.write_timestamp_micros_long(val)


class StringWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: Any) -> None:
        encoder.write_utf8(val)


class UUIDWriter(Writer):
    def write(self, encoder: BinaryEncoder, val: UUID) -> None:
        uuid_bytes = val.bytes

        if len(uuid_bytes) != 16:
            raise ValueError(f"Expected UUID to be 16 bytes, got: {len(uuid_bytes)}")

        encoder.write_bytes_fixed(uuid_bytes)


@dataclass(frozen=True)
class FixedWriter(Writer):
    _len: int = dataclassfield()

    def write(self, encoder: BinaryEncoder, val: bytes) -> None:
        encoder.write(val)

    def __len__(self) -> int:
        """Returns the length of this object."""
        return self._len

    def __repr__(self) -> str:
        """Returns string representation of this object."""
        return f"FixedWriter({self._len})"


class BinaryWriter(Writer):
    """Variable byte length writer."""

    def write(self, encoder: BinaryEncoder, val: Any) -> None:
        encoder.write_bytes(val)


@dataclass(frozen=True)
class DecimalWriter(Writer):
    precision: int = dataclassfield()
    scale: int = dataclassfield()

    def write(self, encoder: BinaryEncoder, val: Any) -> None:
        return encoder.write_decimal_bytes(val)

    def __repr__(self) -> str:
        """Returns string representation of this object."""
        return f"DecimalWriter({self.precision}, {self.scale})"


@dataclass(frozen=True)
class OptionWriter(Writer):
    option: Writer = dataclassfield()

    def write(self, encoder: BinaryEncoder, val: Any) -> None:
        if val is not None:
            encoder.write_int(1)
            self.option.write(encoder, val)
        else:
            encoder.write_int(0)


@dataclass(frozen=True)
class StructWriter(Writer):
    field_writers: Tuple[Writer, ...] = dataclassfield()

    def write(self, encoder: BinaryEncoder, val: StructType) -> None:
        for writer, value in zip(self.field_writers, val.record_fields()):
            writer.write(encoder, value)

    def __eq__(self, other: Any) -> bool:
        """Implements the equality operator for this object."""
        return self.field_writers == other.field_writers if isinstance(other, StructWriter) else False

    def __repr__(self) -> str:
        """Returns string representation of this object."""
        return f"StructWriter({','.join(repr(field) for field in self.field_writers)})"

    def __hash__(self) -> int:
        """Returns the hash of the writer as hash of this object."""
        return hash(self.field_writers)


@dataclass(frozen=True)
class ListWriter(Writer):
    element_writer: Writer

    def write(self, encoder: BinaryEncoder, val: List[Any]) -> None:
        encoder.write_int(len(val))
        for v in val:
            self.element_writer.write(encoder, v)
        if len(val) > 0:
            encoder.write_int(0)


@dataclass(frozen=True)
class MapWriter(Writer):
    key_writer: Writer
    value_writer: Writer

    def write(self, encoder: BinaryEncoder, val: Dict[Any, Any]) -> None:
        encoder.write_int(len(val))
        for k, v in val.items():
            self.key_writer.write(encoder, k)
            self.value_writer.write(encoder, v)
        if len(val) > 0:
            encoder.write_int(0)

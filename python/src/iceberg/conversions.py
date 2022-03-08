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
"""Utility module for various conversions around PrimitiveType implementations

This module enables:
    - Converting partition strings to built-in python objects
    - Converting a value to a byte buffer
    - Converting a byte buffer to a value

Note:
    Conversion logic varies based on the PrimitiveType implementation. Therefore conversion functions
    are defined here as generic functions using the @singledispatch decorator. For each PrimitiveType
    implementation, a concrete function is registered for each generic conversion function. For PrimitiveType
    implementations that share the same conversion logic, registrations can be stacked.

Example (registering `from_partition_to_py` logic for BooleanType):
    >>> @from_partition_value_to_py.register(BooleanType)
    >>> def _(primitive_type, partition_value_as_str: str):
            if partition_value_as_str is None:
                return False
            return partition_value_as_str.lower() == "true"

Example (stacking registrations):
   >>> @from_partition_value_to_py.register(FloatType)
   >>> @from_partition_value_to_py.register(DoubleType)
   >>> def _(primitive_type, partition_value_as_str: str):
           return float(partition_value_as_str)
"""
import struct
import uuid
from decimal import ROUND_HALF_DOWN, Decimal, localcontext
from functools import singledispatch

from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


@singledispatch
def from_partition_value_to_py(primitive_type, partition_value_as_str: str):
    """A generic function function which converts a partition string to a python built-in

    Args:
        primitive_type(PrimitiveType): An implementation of the PrimitiveType base class
        partition_value_as_str(str): A string representation of a partition value
    """
    if partition_value_as_str is None:
        return None
    raise TypeError(
        f"Cannot convert partition string to python built-in, type {primitive_type} not supported: '{partition_value_as_str}'"
    )


@from_partition_value_to_py.register(BooleanType)
def _(primitive_type, partition_value_as_str: str):
    if partition_value_as_str is None:
        return False
    return partition_value_as_str.lower() == "true"


@from_partition_value_to_py.register(IntegerType)
@from_partition_value_to_py.register(LongType)
def _(primitive_type, partition_value_as_str: str):
    return int(float(partition_value_as_str))


@from_partition_value_to_py.register(FloatType)
@from_partition_value_to_py.register(DoubleType)
def _(primitive_type, partition_value_as_str: str):
    return float(partition_value_as_str)


@from_partition_value_to_py.register(StringType)
def _(primitive_type, partition_value_as_str: str):
    return partition_value_as_str


@from_partition_value_to_py.register(UUIDType)
def _(primitive_type, partition_value_as_str: str):
    return uuid.UUID(partition_value_as_str)


@from_partition_value_to_py.register(FixedType)
def _(primitive_type, partition_value_as_str: str):
    return bytearray(bytes(partition_value_as_str, "UTF-8"))


@from_partition_value_to_py.register(BinaryType)
def _(primitive_type, partition_value_as_str: str):
    return bytes(partition_value_as_str, "UTF-8")


@from_partition_value_to_py.register(DecimalType)
def _(primitive_type, partition_value_as_str: str):
    return Decimal(partition_value_as_str)


@singledispatch
def to_bytes(primitive_type, value) -> bytes:
    """A generic function which converts a built-in python value to bytes

    Args:
        primitive_type(PrimitiveType): An implementation of the PrimitiveType base class
        value: The value to convert to bytes (The type of this value depends on which dispatched function is
            used--check dispatchable functions for typehints)
    """
    raise TypeError(f"Cannot serialize value, type {primitive_type} not supported: '{value}'")


@to_bytes.register(BooleanType)
def _(primitive_type, value: bool) -> bytes:
    return struct.pack("<?", 1 if value else 0)


@to_bytes.register(IntegerType)
@to_bytes.register(DateType)
def _(primitive_type, value: int) -> bytes:
    return struct.pack("<i", value)


@to_bytes.register(LongType)
@to_bytes.register(TimeType)
@to_bytes.register(TimestampType)
@to_bytes.register(TimestamptzType)
def _(primitive_type, value: int) -> bytes:
    return struct.pack("<q", value)


@to_bytes.register(FloatType)
def _(primitive_type, value: float) -> bytes:
    return struct.pack("<f", value)


@to_bytes.register(DoubleType)
def _(primitive_type, value: float) -> bytes:
    return struct.pack("<d", value)


@to_bytes.register(StringType)
def _(primitive_type, value: str) -> bytes:
    return value.encode("UTF-8")


@to_bytes.register(UUIDType)
def _(primitive_type, value: uuid.UUID) -> bytes:
    return struct.pack(">QQ", (value.int >> 64) & 0xFFFFFFFFFFFFFFFF, value.int & 0xFFFFFFFFFFFFFFFF)


@to_bytes.register(BinaryType)
@to_bytes.register(FixedType)
def _(primitive_type, value: bytes) -> bytes:
    return value


@to_bytes.register(DecimalType)
def _(primitive_type, value: Decimal) -> bytes:
    value_as_tuple = value.as_tuple()
    value_precision = len(value_as_tuple.digits)
    value_scale = -value_as_tuple.exponent

    if value_scale != primitive_type.scale:
        raise ValueError(f"Cannot serialize value, scale of value does not match type {primitive_type}: {value_scale}")
    elif value_precision != primitive_type.precision:
        raise ValueError(f"Cannot serialize value, precision of value does not match type {primitive_type}: {value_precision}")

    with localcontext() as ctx:
        ctx.prec = primitive_type.precision
        unscaled_value = int((value * 10**primitive_type.scale).to_integral_value(rounding=ROUND_HALF_DOWN))
        min_num_bytes = ((unscaled_value).bit_length() + 7) // 8
        return unscaled_value.to_bytes(min_num_bytes, "big", signed=True)


@singledispatch
def from_bytes(primitive_type, b: bytes):
    """A generic function which converts bytes to a built-in python value

    Args:
        primitive_type(PrimitiveType): An implementation of the PrimitiveType base class
        b(bytes): The bytes to convert
    """
    raise TypeError(f"Cannot deserialize bytes, type {primitive_type} not supported: {str(b)}")


@from_bytes.register(BooleanType)
def _(primitive_type, b: bytes):
    return struct.unpack("<?", b)[0] != 0


@from_bytes.register(IntegerType)
@from_bytes.register(DateType)
def _(primitive_type, b: bytes):
    return struct.unpack("<i", b)[0]


@from_bytes.register(LongType)
@from_bytes.register(TimeType)
@from_bytes.register(TimestampType)
@from_bytes.register(TimestamptzType)
def _(primitive_type, b: bytes):
    return struct.unpack("<q", b)[0]


@from_bytes.register(FloatType)
def _(primitive_type, b: bytes):
    return struct.unpack("<f", b)[0]


@from_bytes.register(DoubleType)
def _(primitive_type, b: bytes):
    return struct.unpack("<d", b)[0]


@from_bytes.register(StringType)
def _(primitive_type, b: bytes):
    return bytes(b).decode("utf-8")


@from_bytes.register(UUIDType)
def _(primitive_type, b: bytes):
    return uuid.UUID(int=struct.unpack(">QQ", b)[0] << 64 | struct.unpack(">QQ", b)[1])


@from_bytes.register(BinaryType)
@from_bytes.register(FixedType)
def _(primitive_type, b: bytes):
    return b


@from_bytes.register(DecimalType)
def _(primitive_type, b: bytes):
    integer_representation = int.from_bytes(b, "big", signed=True)
    with localcontext() as ctx:
        ctx.prec = primitive_type.precision
        return Decimal(integer_representation) * (Decimal(10) ** Decimal(-primitive_type.scale))

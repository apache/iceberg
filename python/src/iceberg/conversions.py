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
"""
import struct
import uuid
from decimal import Decimal, localcontext
from functools import singledispatch
from typing import Union

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
    PrimitiveType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def convert_decimal_to_unscaled_value(decimal_type_var: DecimalType, value: Decimal) -> int:
    """Get an unscaled value given a Decimal value and a DecimalType instance with defined precision and scale

    Args:
        decimal_type_var (DecimalType): A DecimalType instance with precision and scale
        value (Decimal): A Decimal instance

    Raises:
        ValueError: If either the scale or precision of `value` does not match that defined in the DecimalType instance provided as `decimal_type_var`

    Returns:
        int: The unscaled value
    """
    value_as_tuple = value.as_tuple()
    value_precision = len(value_as_tuple.digits)
    value_scale = -value_as_tuple.exponent

    if value_scale != decimal_type_var.scale:
        raise ValueError(f"Cannot serialize value, scale of value does not match type {decimal_type_var}: {value_scale}")
    elif value_precision > decimal_type_var.precision:
        raise ValueError(
            f"Cannot serialize value, precision of value is greater than precision of type {decimal_type_var}: {value_precision}"
        )

    with localcontext() as ctx:
        ctx.prec = decimal_type_var.precision
        sign, digits, exponent = value.as_tuple()
        value_w_adjusted_scale = Decimal((sign, digits, exponent + decimal_type_var.scale))
        return int(value_w_adjusted_scale.to_integral_value())


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
        return None
    return partition_value_as_str.lower() == "true"


@from_partition_value_to_py.register(IntegerType)
@from_partition_value_to_py.register(LongType)
def _(primitive_type, partition_value_as_str: str):
    """
    Note: Before attempting to cast the value to string, a validation happens to ensure that there are no fractional digits. For
    example, an invalid value such as "123.45" will be converted to a decimal with digits (1, 2, 3, 4, 5) with an exponent of -2 and the last
    2 digits will be inspected for non-zero values. An example of a valid value is "123.00".
    """
    if partition_value_as_str is None:
        return None
    _, digits, exponent = Decimal(partition_value_as_str).as_tuple()
    if exponent != 0 and any(digits[exponent:]):  # If there are digits to the right of the exponent, raise if any are not 0
        raise ValueError(f"Cannot convert partition value, value cannot have fractional digits for {primitive_type} partition")
    return int(float(partition_value_as_str))


@from_partition_value_to_py.register(FloatType)
@from_partition_value_to_py.register(DoubleType)
def _(primitive_type, partition_value_as_str: str):
    if partition_value_as_str is None:
        return None
    return float(partition_value_as_str)


@from_partition_value_to_py.register(StringType)
def _(primitive_type, partition_value_as_str: str):
    if partition_value_as_str is None:
        return None
    return partition_value_as_str


@from_partition_value_to_py.register(UUIDType)
def _(primitive_type, partition_value_as_str: str):
    if partition_value_as_str is None:
        return None
    return uuid.UUID(partition_value_as_str)


@from_partition_value_to_py.register(FixedType)
@from_partition_value_to_py.register(BinaryType)
def _(primitive_type, partition_value_as_str: str):
    if partition_value_as_str is None:
        return None
    return bytes(partition_value_as_str, "UTF-8")


@from_partition_value_to_py.register(DecimalType)
def _(primitive_type, partition_value_as_str: str):
    if partition_value_as_str is None:
        return None
    return Decimal(partition_value_as_str)


@singledispatch
def to_bytes(primitive_type: PrimitiveType, value: Union[bool, bytes, Decimal, float, int, str, uuid.UUID]) -> bytes:
    """A generic function which converts a built-in python value to bytes

    This conversion follows the serialization scheme for storing single values as individual binary values defined in the Iceberg specification that
    can be found at https://iceberg.apache.org/spec/#appendix-d-single-value-serialization

    Args:
        primitive_type(PrimitiveType): An implementation of the PrimitiveType base class
        value: The value to convert to bytes (The type of this value depends on which dispatched function is
            used--check dispatchable functions for typehints)
    """
    raise TypeError(f"Cannot serialize value, type {primitive_type} not supported: '{str(value)}'")


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
    """
    Note: float in python is implemented using a double in C. Therefore this involves a conversion of a 32-bit (single precision)
    float to a 64-bit (double precision) float which introduces some imprecision.
    """
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
    unscaled_value = convert_decimal_to_unscaled_value(decimal_type_var=primitive_type, value=value)
    min_num_bytes = ((unscaled_value).bit_length() + 7) // 8
    return unscaled_value.to_bytes(min_num_bytes, "big", signed=True)


@singledispatch
def from_bytes(primitive_type: PrimitiveType, b: bytes) -> Union[bool, bytes, Decimal, float, int, str, uuid.UUID]:
    """A generic function which converts bytes to a built-in python value

    Args:
        primitive_type(PrimitiveType): An implementation of the PrimitiveType base class
        b(bytes): The bytes to convert
    """
    raise TypeError(f"Cannot deserialize bytes, type {primitive_type} not supported: {str(b)}")


@from_bytes.register(BooleanType)
def _(primitive_type, b: bytes) -> bool:
    return struct.unpack("<?", b)[0] != 0


@from_bytes.register(IntegerType)
@from_bytes.register(DateType)
def _(primitive_type, b: bytes) -> int:
    return struct.unpack("<i", b)[0]


@from_bytes.register(LongType)
@from_bytes.register(TimeType)
@from_bytes.register(TimestampType)
@from_bytes.register(TimestamptzType)
def _(primitive_type, b: bytes) -> int:
    return struct.unpack("<q", b)[0]


@from_bytes.register(FloatType)
def _(primitive_type, b: bytes):
    return struct.unpack("<f", b)[0]


@from_bytes.register(DoubleType)
def _(primitive_type, b: bytes) -> float:
    return struct.unpack("<d", b)[0]


@from_bytes.register(StringType)
def _(primitive_type: PrimitiveType, b: bytes) -> str:
    return bytes(b).decode("utf-8")


@from_bytes.register(UUIDType)
def _(primitive_type, b: bytes) -> uuid.UUID:
    unpacked_bytes = struct.unpack(">QQ", b)
    return uuid.UUID(int=unpacked_bytes[0] << 64 | unpacked_bytes[1])


@from_bytes.register(BinaryType)
@from_bytes.register(FixedType)
def _(primitive_type, b: bytes) -> bytes:
    return b


@from_bytes.register(DecimalType)
def _(primitive_type, b: bytes) -> Decimal:
    integer_representation = int.from_bytes(b, "big", signed=True)
    with localcontext() as ctx:
        ctx.prec = primitive_type.precision
        return Decimal(integer_representation) * Decimal((0, (1,), -primitive_type.scale))

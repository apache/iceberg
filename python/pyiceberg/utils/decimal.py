#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Helper methods for working with Python Decimals"""
from decimal import Decimal
from typing import Union


def decimal_to_unscaled(value: Decimal) -> int:
    """Get an unscaled value given a Decimal value

    Args:
        value (Decimal): A Decimal instance

    Returns:
        int: The unscaled value
    """
    sign, digits, _ = value.as_tuple()
    return int(Decimal((sign, digits, 0)).to_integral_value())


def unscaled_to_decimal(unscaled: int, scale: int) -> Decimal:
    """Get a scaled Decimal value given an unscaled value and a scale

    Args:
        unscaled (int): An unscaled value
        scale (int): A scale to set for the returned Decimal instance

    Returns:
        Decimal: A scaled Decimal instance
    """
    sign, digits, _ = Decimal(unscaled).as_tuple()
    return Decimal((sign, digits, -scale))


def bytes_required(value: Union[int, Decimal]) -> int:
    """Returns the minimum number of bytes needed to serialize a decimal or unscaled value

    Args:
        value (int | Decimal): a Decimal value or unscaled int value

    Returns:
        int: the minimum number of bytes needed to serialize the value
    """
    if isinstance(value, int):
        return (value.bit_length() + 7) // 8
    elif isinstance(value, Decimal):
        return (decimal_to_unscaled(value).bit_length() + 7) // 8

    raise ValueError(f"Unsupported value: {value}")


def decimal_to_bytes(value: Decimal) -> bytes:
    """Returns a byte representation of a decimal

    Args:
        value (Decimal): a decimal value
    Returns:
        bytes: the unscaled value of the Decimal as bytes
    """
    unscaled_value = decimal_to_unscaled(value)
    return unscaled_value.to_bytes(bytes_required(unscaled_value), byteorder="big", signed=True)


def truncate_decimal(value: Decimal, width: int) -> Decimal:
    """Get a truncated Decimal value given a decimal value and a width
    Args:
        value (Decimal): a decimal value
        width (int): A width for the returned Decimal instance
    Returns:
        Decimal: A truncated Decimal instance
    """
    unscaled_value = decimal_to_unscaled(value)
    applied_value = unscaled_value - (((unscaled_value % width) + width) % width)
    return unscaled_to_decimal(applied_value, abs(int(value.as_tuple().exponent)))

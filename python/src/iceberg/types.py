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

import decimal
import math
import struct
from base64 import b64encode
from datetime import date, datetime, time
from decimal import Decimal as PythonDecimal
from typing import Any, Dict
from typing import List as PythonList
from typing import Optional, Tuple, Type, Union
from uuid import UUID as PythonUUID

import mmh3
from numpy import float32, float64, isinf, isnan


class IcebergType:
    """Base type for all Iceberg Types"""

    def __setattr__(self, key, value):
        if key in getattr(self, "_frozen_attrs", set()) or key in getattr(
            type(self), "_always_frozen", set()
        ):
            raise AttributeError(f"{key} may not be altered on isntance {self}")
        object.__setattr__(self, key, value)

    # freeze deleting of generic attributes
    def __delattr__(self, key):
        if key in getattr(self, "_frozen_attrs", set()) or key in getattr(
            type(self), "_always_frozen", set()
        ):
            raise AttributeError(f"{key} may not be deleted on instance {self}")
        object.__delattr__(self, key)

    @classmethod
    def can_cast(cls, _type: Type["IcebergType"]):
        return cls == _type


class PrimitiveType(IcebergType):
    """
    base type for primitives `IcebergType`s
    Primitives include an instance attribute `value` which is used as the underlying value to work with the type
    a `PrimitiveType` should type the instance `value` most specific to that type
    """

    value: Union[bytes, bool, int, float32, float64, PythonDecimal, str, dict]

    def __init__(self, value):

        if issubclass(type(value), PrimitiveType):
            try:
                self.value = value.to(type(self)).value
            except AttributeError:
                raise TypeError(f"Cannot convert {value} to type {type(self)}")
        else:
            self.value = value

    def __repr__(self) -> str:
        return f"{repr(type(self))}(value={self.value})"

    def __str__(self) -> str:
        return f"{str(type(self))}({self.value})"

    def __bool__(self) -> bool:
        return bool(self.value)

    def __eq__(self, other) -> bool:
        return type(other) == type(self) and self.value == other.value

    def __bytes__(self) -> bytes:
        return type(self).to_bytes(self.value)

    @classmethod
    def to_bytes(cls, value):
        return bytes(value)

    def __hash__(self) -> int:
        """https://iceberg.apache.org/#spec/#appendix-b-32-bit-hash-requirements"""
        return type(self).hash(self.value)

    @classmethod
    def hash(cls, value):
        return mmh3.hash(cls.to_bytes(value))

    def to(self, _type: Type["PrimitiveType"], coerce: bool = False):
        if type(self).can_cast(_type) or coerce:
            return _type(self.value)
        raise TypeError(f"Cannot cast {type(self)} to {_type}.")


class Number(PrimitiveType):
    """
    base `PrimitiveType` for `IcebergType`s for numeric types
    per https://iceberg.apache.org/#spec/#primitive-types these include int, long, float, double, decimal
    """

    value: Union[int, float32, float64, PythonDecimal]

    def __float__(self) -> int:
        return float(self.value)

    def __int__(self) -> float:
        return int(self.value)

    def __math(self, op, other=None):
        op_f = getattr(self.value, op)
        try:
            if op in ("__add__", "__sub__", "__div__", "__mul__",):
                other = other.to(type(self))
                return type(self)(op_f(other.value))
            if op in ("__pow__", "__mod__",):
                other = type(self)(other)
                return type(self)(op_f(other.value))
            if op in ("__lt__", "__eq__"):
                other = other.to(type(self))
                return op_f(other.value)
        except TypeError:
            raise TypeError(
                f"Cannot compare {self} with {other}. Perhaps try coercing to the appropriate type as {other}.to({type(self)}, coerce=True)."
            )
        except AttributeError:
            raise TypeError(
                f"Cannot compare {self} with {other}. Ensure try creating an appropriate type {type(self)}({other})."
            )

        if op in ("__neg__", "__abs__"):
            return type(self)(op_f())

    def __add__(self, other: "Number") -> "Number":
        return self.__math("__add__", other)

    def __sub__(self, other: "Number") -> "Number":
        return self.__math("__sub__", other)

    def __mul__(self, other: "Number") -> "Number":
        return self.__math("__mul__", other)

    def __div__(self, other: "Number") -> "Number":
        return self.__math("__div__", other)

    def __neg__(self) -> "Number":
        return self.__math("__neg__")

    def __abs__(self) -> "Number":
        return self.__math("__abs__")

    def __pow__(self, other: "Number", mod: Optional["Number"] = None) -> "Number":
        return self.__math("__pow__", other)

    def __mod__(self, other: "Number") -> "Number":
        return self.__math("__mod__", other)

    def __lt__(self, other) -> bool:
        return self.__math("__lt__", other)

    def __eq__(self, other) -> bool:
        return self.__math("__eq__", other) and self._neg == other._neg

    def __gt__(self, other) -> bool:
        return not self.__le__(other)

    def __le__(self, other) -> bool:
        return self.__lt__(other) or self.__eq__(other)

    def __ge__(self, other) -> bool:
        return self.__gt__(other) or self.__eq__(other)

    def __hash__(self) -> int:
        return super().__hash__()


class Integral(Number):
    """base class for integral types Integer, Long

    Note:
       for internal iceberg use only

    Examples:
        Can be used in place of typing for Integer and Long
    """

    value: int
    _neg: bool
    _frozen_attrs = {"min", "max", "_neg"}

    def __init__(self, value: Union[str, float, int]):
        super().__init__(value)

        if isinstance(self.value, Number):
            self.value = int(self.value.value)
        else:
            self.value = int(self.value)
        self._check()
        object.__setattr__(self, "_neg", self.value < 0)

    def _check(self) -> "Integral":
        """
        helper method for `Integal` specific `_check` to ensure value is within spec
        """
        if self.value > self.max:
            raise ValueError(f"{type(self)} must be less than or equal to {self.max}")

        if self.value < self.min:
            raise ValueError(
                f"{type(self)} must be greater than or equal to {self.min}"
            )

        return self

    @classmethod
    def to_bytes(cls, value) -> bytes:
        return struct.pack("q", value)


class Floating(Number):
    """base class for floating types Float, Double

    Note:
       for internal iceberg use only

    Examples:
        Can be used in place of typing for Float and Double
    """

    _neg: bool
    _frozen_attrs = {"_neg"}

    def __init__(self, float_t, value: Union[float, str, int]):
        super().__init__(value)
        object.__setattr__(self, "_neg", str(self.value).strip().startswith("-"))
        if isinstance(self.value, Number):
            self.value = float_t(self.value.value)
        else:
            self.value = float_t(self.value)

    @classmethod
    def to_bytes(cls, value) -> bytes:
        return struct.pack("d", value)

    def __repr__(self) -> str:
        ret = super().__repr__()
        if self._neg and isnan(self.value):
            return ret.replace("nan", "-nan")
        return ret

    def is_nan(self) -> bool:
        return isnan(self.value)

    def is_inf(self) -> bool:
        return isinf(self.value)

    def __str__(self) -> str:
        ret = super().__str__()
        if self._neg and isnan(self.value):
            return ret.replace("nan", "-nan")
        if self._neg and self.value == 0.0:
            return ret.replace("0.0", "-0.0")
        return ret

    def __lt__(self, other: "Floating") -> bool:
        try:
            other = other.to(type(self))
        except TypeError:
            raise TypeError(
                f"Cannot compare {self} with {other}. Perhaps try coercing to the appropriate type as {other}.to({type(self)}, coerce=True)."
            )
        except AttributeError:
            raise TypeError(
                f"Cannot compare {self} with {other}. Ensure try creating an appropriate type {type(self)}({other})."
            )

        def get_key(x):
            if x.is_nan():
                ret = "nan"
            elif x.is_inf():
                ret = "inf"
            else:
                return "value"
            return ("-" if x._neg else "") + ret

        return {
            ("inf", "value"): False,
            ("nan", "nan"): False,
            ("-inf", "-inf"): False,
            ("value", "inf"): True,
            ("-inf", "-nan"): False,
            ("-nan", "-nan"): False,
            ("value", "-nan"): False,
            ("-nan", "-inf"): True,
            ("-inf", "inf"): True,
            ("-nan", "nan"): True,
            ("nan", "value"): False,
            ("nan", "-nan"): False,
            ("inf", "nan"): False,
            ("-nan", "inf"): True,
            ("inf", "inf"): False,
            ("nan", "-inf"): False,
            ("value", "value"): (self._neg and not other._neg)
            or (self.value < other.value),
            ("-nan", "value"): True,
            ("value", "nan"): True,
            ("-inf", "value"): True,
            ("-inf", "nan"): True,
            ("inf", "-inf"): False,
            ("nan", "inf"): True,
            ("value", "-inf"): False,
            ("inf", "-nan"): False,
        }[(get_key(self), get_key(other))]


class Integer(Integral):
    """32-bit signed integers: `int` from https://iceberg.apache.org/#spec/#primitive-types


    Args:
        value: value for which the integer will represent

    Attributes:
        value (int): the literal value contained by the `Integer`
        max (int): the maximum value `Integer` may take on
        min (int): the minimum value `Integer` may take on

    Examples:
        >>> Integer(5)
        Integer(value=5)

        >>> Integer('3.14')
        Integer(value=3)

        >>> Integer(3.14)
        Integer(value=3)

    """

    max: int = 2147483647
    min: int = -2147483648

    @classmethod
    def can_cast(cls, _type):
        return _type in (cls, Long)


class Long(Integral):
    """64-bit signed integers: `long` from https://iceberg.apache.org/#spec/#primitive-types


    Args:
        value: value for which the long will represent

    Attributes:
        value (int): the literal value contained by the `Long`
        max (int): the maximum value `Long` may take on
        min (int): the minimum value `Long` may take on

    Examples:
        >>> Long(5)
        Long(value=5)

        >>> Long('3.14')
        Long(value=3)

        >>> Long(3.14)
        Long(value=3)
    """

    max: int = 9223372036854775807
    min: int = -9223372036854775808


class Float(Floating):
    """32-bit IEEE 754 floating point: `float` from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        value: value for which the float will represent

     Examples:
        >>> Float(5)
        Float(value=5.0)

        >>> Float(3.14)
        Float(value=3)
    """

    # float32 ensures spec
    value: float32

    def __init__(self, value):
        super().__init__(float32, value)

    @classmethod
    def can_cast(cls, _type):
        return _type in (cls, Double)


class Double(Floating):
    """64-bit IEEE 754 floating point: `double` from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        value: value for which the double will represent

    Examples:
        >>> Double(5)
        Double(value=5.0)

        >>> Double(3.14)
        Double(value=3)

    """

    # float64 ensures spec
    value: float64

    def __init__(self, value):
        super().__init__(float64, value)


class Boolean(PrimitiveType):
    """`boolean` from https://iceberg.apache.org/#spec/#primitive-types

    Args:
            value (bool): value the boolean will represent

    Examples:
            >>>Boolean(True)
            Boolean(value=True)
    """

    value: bool

    def __bool__(self):
        return self.value

    @classmethod
    def to_bytes(self, value) -> bytes:
        return Integer.to_bytes(value)

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, other) -> bool:
        return isinstance(other, Boolean) and self.value == other.value


class String(PrimitiveType):
    """Arbitrary-length character sequences Encoded with UTF-8: `string` from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        value (str): value the string will represent

    Attributes:
        value (str): the literal value contained by the `String`

    Examples:
        >>> String("Hello")
        String(value='Hello')
    """

    value: str

    @classmethod
    def hash(cls, value):
        return mmh3.hash(value)


class UUID(PrimitiveType):
    """Universally unique identifiers: `uuid` from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        value: value the uuid will represent

    Attributes:
        value (uuid.UUID): literal value contained by the `UUID`

    Examples:
        >>> UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")
        UUID(value=f79c3e09-677c-4bbd-a479-3f349cb785e7)
    """

    value: PythonUUID

    def __init__(self, value: Union[str, PythonUUID]):
        super().__init__(value)
        if not isinstance(self.value, PythonUUID):
            self.value = PythonUUID(self.value)

    def __int__(self) -> int:
        return self.value.int

    @classmethod
    def to_bytes(cls, value) -> bytes:
        v = int(value.int)
        return struct.pack(
            ">QQ", (v >> 64) & 0xFFFFFFFFFFFFFFFF, v & 0xFFFFFFFFFFFFFFFF,
        )


class Binary(PrimitiveType):
    """Arbitrary-length byte array from  https://iceberg.apache.org/#spec/#primitive-types

    Args:
        value (bytes): bytes to hold in binary buffer

    Attributes:
        value (bytes): bytes to hold in binary buffer

    Examples:
        >>> Binary(b"\x00\x01\x02\x03")
        Binary(value=b'\x00\x01\x02\x03')
    """

    value: bytes

    def to_base64(self) -> str:
        return b64encode(self.value).decode("ISO-8859-1")


class Datetime(PrimitiveType):
    """base class for Date, Time, Timestamp, Timestamptz

    Note:
       for internal iceberg use only

    """

    epoch: datetime = datetime.utcfromtimestamp(0)
    _frozen_attrs = {"epoch"}
    value: Union[date, time, datetime]

    def __init__(self, dt_t, *args, **kwargs):
        try:
            self.value = dt_t.fromisoformat(*args, **kwargs)
        except TypeError:
            self.value = dt_t(*args, **kwargs)

    def __getattr__(self, attr):
        """
        pass attribute to `value` when the `Datetime` does not have it
        very convenient for datetime types
        """
        return getattr(self.value, attr)

    def __int__(self):
        return type(self).to_int(self.value)

    def __hash__(self) -> int:
        return type(self).hash(int(self))

    @classmethod
    def to_bytes(cls, value: int) -> bytes:
        return struct.pack("q", value)


class Date(Datetime):
    """`date` type from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        *args: can pass a string formatted in the isoformat or several `int` for year, month, day
        **kwargs: can pass kwargs for named year, month, day

    Attributes:
        value (datetime.date): literal date value Date holds

    Examples:
        >>> Date("2017-11-16")
        Date(value=2017-11-16)
    """

    value: date

    def __init__(self, *args, **kwargs):
        super().__init__(date, *args, **kwargs)

    def __int__(self) -> int:
        """days from unix epoch"""
        return (self.value - Datetime.epoch.date()).days


class Time(Datetime):
    """`time` type from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        *args: can pass a string formatted in the isoformat or several `int` for hour[, minute[, second[, microsecond]
        **kwargs: can pass kwargs for named hour[, minute[, second[, microsecond]

    Attributes:
        value (datetime.time): literal time value Time holds

    Examples:
        >>> Time(22, 31, 8)
        Time(value=22:31:08)
    """

    value: time

    def __init__(self, *args, **kwargs):
        super().__init__(time, *args, **kwargs)

    def __int__(self) -> int:
        """microseconds from midnight"""
        return (
            (((self.value.hour * 60 + self.value.minute) * 60 + self.value.second))
        ) * 1000000 + self.value.microsecond


class Timestamp(Datetime):
    """`timestamp` type from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        *args: can pass a string formatted in the isoformat or several `int` for year, month, day[, hour[, minute[, second[, microsecond]]]]
        **kwargs: can pass kwargs for named year, month, day[, hour[, minute[, second[, microsecond]]]]

    Attributes:
        value (datetime.datetime): literal timestamp value Timestamp holds

    Examples:
        >>> Timestamp("2017-11-16T14:31:08-08:00")
       Timestamp(value=2017-11-16 14:31:08-08:00)
    """

    value: datetime

    def __init__(self, *args, **kwargs):
        super().__init__(datetime, *args, **kwargs)

    def __int__(self) -> int:
        """microseconds from epoch"""
        return int(self.value.timestamp()) * 1000000


class Timestamptz(Timestamp):
    """`timestamptz` type from https://iceberg.apache.org/#spec/#primitive-types

    Args:
        *args: can pass a string formatted in the isoformat or several `int` for year, month, day[, hour[, minute[, second[, microsecond[,tzinfo]]]]]
        **kwargs: can pass kwargs for named year, month, day[, hour[, minute[, second[, microsecond[,tzinfo]]]]]

    Attributes:
        value (datetime.datetime): literal timestamp value Timestamp holds

    Examples:
        >>> Timestamptz("2017-11-16T14:31:08-08:00")
       Timestamptz(value=2017-11-16 14:31:08-08:00)
    """

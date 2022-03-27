#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import struct
import sys
import uuid
from abc import ABC, abstractmethod
from decimal import ROUND_HALF_UP, Decimal
from functools import singledispatch
from typing import Generic, Optional, TypeVar, Union

if sys.version_info >= (3, 8):
    from functools import singledispatchmethod  # pragma: no cover
else:
    from singledispatch import singledispatchmethod  # pragma: no cover

import pytz

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
    Singleton,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

EPOCH = datetime.datetime.utcfromtimestamp(0)
T = TypeVar("T")


class Literal(Generic[T], ABC):
    """Literal which has a value and can be converted between types"""

    def __init__(self, value: T):
        if value is None:
            raise TypeError(f"Invalid literal value: {value}")
        self._value = value

    @property
    def value(self) -> T:
        return self._value

    @abstractmethod
    def to(self, type_var):
        ...  # pragma: no cover

    def __repr__(self):
        return f"{type(self).__name__}({self.value})"

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return self.value > other.value

    def __le__(self, other):
        return self.value <= other.value

    def __ge__(self, other):
        return self.value >= other.value


@singledispatch
def literal(value) -> Literal:
    """
    A generic Literal factory to construct an iceberg Literal based on python primitive data type
    using dynamic overloading

    Args:
        value(python primitive type): the value to be associated with literal

    Example:
        from iceberg.expressions.literals import literal
        >>> literal(123)
        IntegerLiteral(123)
    """
    raise TypeError(f"Unimplemented Type Literal for value: {str(value)}")


@literal.register(bool)
def _(value: bool) -> Literal[bool]:
    return BooleanLiteral(value)


@literal.register(int)
def _(value: int) -> Literal[int]:
    """
    Upgrade to long if python int is outside the JAVA_MIN_INT and JAVA_MAX_INT
    """
    if value < IntegerType.min or value > IntegerType.max:
        return LongLiteral(value)
    return IntegerLiteral(value)


@literal.register(float)
def _(value: float) -> Literal[float]:
    """
    Upgrade to double if python float is outside the JAVA_MIN_FLOAT and JAVA_MAX_FLOAT
    """
    if value < FloatType.min or value > FloatType.max:
        return DoubleLiteral(value)
    return FloatLiteral(value)


@literal.register(str)
def _(value: str) -> Literal[str]:
    return StringLiteral(value)


@literal.register(uuid.UUID)
def _(value: uuid.UUID) -> Literal[uuid.UUID]:
    return UUIDLiteral(value)


@literal.register(bytes)
def _(value: bytes) -> Literal[bytes]:
    return FixedLiteral(value)


@literal.register(bytearray)
def _(value: bytearray) -> Literal[bytes]:
    return BinaryLiteral(value)


@literal.register(Decimal)
def _(value: Decimal) -> Literal[Decimal]:
    return DecimalLiteral(value)


class AboveMax(Literal[None], Singleton):
    def __init__(self):
        pass

    def value(self):
        raise ValueError("AboveMax has no value")

    def to(self, type_var):
        raise TypeError("Cannot change the type of AboveMax")

    def __repr__(self):
        return "AboveMax()"

    def __str__(self):
        return "AboveMax"


class BelowMin(Literal[None], Singleton):
    def __init__(self):
        pass

    def value(self):
        raise ValueError("BelowMin has no value")

    def to(self, type_var):
        raise TypeError("Cannot change the type of BelowMin")

    def __repr__(self):
        return "BelowMin()"

    def __str__(self):
        return "BelowMin"


class BooleanLiteral(Literal[bool]):
    def __init__(self, value):
        self._value = value

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BooleanType)
    def _(self, type_var):
        return self


class IntegerLiteral(Literal[int]):
    def __init__(self, value):
        self._value = value

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(IntegerType)
    def _(self, type_var: IntegerType) -> "IntegerLiteral":
        return self

    @to.register(LongType)
    def _(self, type_var: LongType) -> "LongLiteral":
        return LongLiteral(self.value)

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> "FloatLiteral":
        return FloatLiteral(float(self.value))

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> "DoubleLiteral":
        return DoubleLiteral(self.value)

    @to.register(DateType)
    def _(self, type_var: DateType) -> "DateLiteral":
        return DateLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> "DecimalLiteral":
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value))
        else:
            return DecimalLiteral(
                Decimal(self.value).quantize(
                    Decimal("." + "".join(["0" for _ in range(1, type_var.scale)]) + "1"), rounding=ROUND_HALF_UP
                )
            )


class LongLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(LongType)
    def _(self, type_var: LongType) -> "LongLiteral":
        return self

    @to.register(IntegerType)
    def _(self, type_var: IntegerType) -> Union[AboveMax, BelowMin, IntegerLiteral]:
        if IntegerType.max < self.value:
            return AboveMax()
        elif IntegerType.min > self.value:
            return BelowMin()
        return IntegerLiteral(self.value)

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> "FloatLiteral":
        return FloatLiteral(self.value)

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> "DoubleLiteral":
        return DoubleLiteral(self.value)

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> "TimeLiteral":
        return TimeLiteral(self.value)

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> "TimestampLiteral":
        return TimestampLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> "DecimalLiteral":
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value))
        else:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class FloatLiteral(Literal[float]):
    def __init__(self, value: float):
        super().__init__(value=value)
        self._value32 = struct.unpack("<f", struct.pack("<f", value))[0]

    def __eq__(self, other):
        self._value32 == other

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> "FloatLiteral":
        return self

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> "DoubleLiteral":
        return DoubleLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> "DecimalLiteral":
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal("1."), rounding=ROUND_HALF_UP))
        else:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DoubleLiteral(Literal[float]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> "DoubleLiteral":
        return self

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> Union[AboveMax, BelowMin, FloatLiteral]:
        if FloatType.max < self.value:
            return AboveMax()
        elif FloatType.min > self.value:
            return BelowMin()
        return FloatLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> "DecimalLiteral":
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal("1."), rounding=ROUND_HALF_UP))
        else:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DateLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DateType)
    def _(self, type_var: DateType) -> "DateLiteral":
        return self


class TimeLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> "TimeLiteral":
        return self


class TimestampLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> "TimestampLiteral":
        return self

    @to.register(DateType)
    def _(self, type_var: DateType) -> "DateLiteral":
        return DateLiteral((datetime.datetime.fromtimestamp(self.value / 1000000) - EPOCH).days)


class DecimalLiteral(Literal[Decimal]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Optional["DecimalLiteral"]:
        if type_var.scale == abs(self.value.as_tuple().exponent):
            return self
        return None


class StringLiteral(Literal[str]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(StringType)
    def _(self, type_var: StringType) -> "StringLiteral":
        return self

    @to.register(DateType)
    def _(self, type_var: DateType) -> "DateLiteral":
        return DateLiteral((datetime.datetime.strptime(self.value, "%Y-%m-%d") - EPOCH).days)

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> "TimeLiteral":
        return TimeLiteral(
            int(
                (
                    datetime.datetime.strptime((EPOCH.strftime("%Y-%m-%d ") + self.value), "%Y-%m-%d %H:%M:%S.%f") - EPOCH
                ).total_seconds()
                * 1000000
            )
        )

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> TimestampLiteral:
        try:
            timestamp = datetime.datetime.strptime(self.value, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError as e:
            try:
                timestamp = datetime.datetime.strptime(self.value, "%Y-%m-%dT%H:%M:%S.%f%z")
                raise TypeError(f"Cannot convert String to Timestamp when timezone is included: {self.value}")
            except:
                raise

        return TimestampLiteral(int((timestamp - EPOCH).total_seconds() * 1_000_000))

    @to.register(TimestamptzType)
    def _(self, type_var: TimestamptzType) -> "TimestampLiteral":
        try:
            timestamp = datetime.datetime.strptime(self.value, "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError as e:
            try:
                timestamp = datetime.datetime.strptime(self.value, "%Y-%m-%dT%H:%M:%S.%f")
            except:
                raise e
            raise TypeError(f"Cannot convert String to Timestamptz, missing timezone: {self.value}")
        utc_epoch = EPOCH.replace(tzinfo=pytz.UTC)
        return TimestampLiteral(int((timestamp - utc_epoch).total_seconds() * 1_000_000))

    @to.register(UUIDType)
    def _(self, type_var: UUIDType) -> "UUIDLiteral":
        return UUIDLiteral(uuid.UUID(self.value))

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> DecimalLiteral:
        dec_val = Decimal(str(self.value))
        value_scale = abs(dec_val.as_tuple().exponent)
        if value_scale == type_var.scale:
            if type_var.scale == 0:
                return DecimalLiteral(Decimal(str(self.value)).quantize(Decimal("1."), rounding=ROUND_HALF_UP))
            else:
                return DecimalLiteral(
                    Decimal(str(self.value)).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP)
                )
        raise ValueError(f"Cannot cast string to decimal, incorrect scale: got {value_scale} expected {type_var.scale}")


class UUIDLiteral(Literal[uuid.UUID]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(UUIDType)
    def _(self, type_var: UUIDType) -> "UUIDLiteral":
        return self


class FixedLiteral(Literal[bytes]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(FixedType)
    def _(self, type_var: FixedType) -> "FixedLiteral":
        if len(self.value) == type_var.length:
            return self
        raise TypeError(f"Cannot cast to fixed with different length: {type_var.length}")

    @to.register(BinaryType)
    def _(self, type_var: BinaryType) -> "BinaryLiteral":
        return BinaryLiteral(self.value)


class BinaryLiteral(Literal[bytes]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BinaryType)
    def _(self, type_var: BinaryType) -> "BinaryLiteral":
        return self

    @to.register(FixedType)
    def _(self, type_var: FixedType) -> FixedLiteral:
        if type_var.length >= len(self.value):
            return FixedLiteral(self.value)
        raise ValueError(f"Cannot cast binary to fixed of smaller size: {type_var.length}")

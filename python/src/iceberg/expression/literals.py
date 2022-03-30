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

from datetime import datetime, date, time
import struct
import sys
import re
from uuid import UUID
from abc import ABC, abstractmethod
from decimal import ROUND_HALF_UP, Decimal
from functools import singledispatch
from typing import Generic, Optional, TypeVar, Union

if sys.version_info >= (3, 8):
    from functools import singledispatchmethod  # pragma: no cover
else:
    from singledispatch import singledispatchmethod  # pragma: no cover

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


EPOCH_DATE = date.fromisoformat("1970-01-01")
EPOCH_TIMESTAMP = datetime.fromisoformat("1970-01-01T00:00:00.000000")
ISO_TIMESTAMP = re.compile(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d{1,6})?")
EPOCH_TIMESTAMPTZ = datetime.fromisoformat("1970-01-01T00:00:00.000000+00:00")
ISO_TIMESTAMPTZ = re.compile(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d{1,6})?[-+]\d\d:\d\d")
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
        LongLiteral(123)
    """
    raise TypeError(f"Invalid literal value: {repr(value)}")


@literal.register(bool)
def _(value: bool) -> Literal[bool]:
    return BooleanLiteral(value)


@literal.register(int)
def _(value: int) -> Literal[int]:
    return LongLiteral(value)


@literal.register(float)
def _(value: float) -> Literal[float]:
    # expression binding can convert to FloatLiteral if needed
    return DoubleLiteral(value)


@literal.register(str)
def _(value: str) -> Literal[str]:
    return StringLiteral(value)


@literal.register(UUID)
def _(value: UUID) -> Literal[UUID]:
    return UUIDLiteral(value)


@literal.register(bytes)
def _(value: bytes) -> Literal[bytes]:
    # expression binding can convert to FixedLiteral if needed
    return BinaryLiteral(value)


@literal.register(bytearray)
def _(value: bytearray) -> Literal[bytes]:
    return BinaryLiteral(bytes(value))


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
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BooleanType)
    def _(self, type_var):
        return self


class LongLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(LongType)
    def _(self, type_var: LongType) -> Literal[int]:
        return self

    @to.register(IntegerType)
    def _(self, type_var: IntegerType) -> Union[AboveMax, BelowMin, Literal[int]]:
        if IntegerType.max < self.value:
            return AboveMax()
        elif IntegerType.min > self.value:
            return BelowMin()
        return self

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> Literal[float]:
        return FloatLiteral(float(self.value))

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> Literal[float]:
        return DoubleLiteral(float(self.value))

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[int]:
        return DateLiteral(self.value)

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> Literal[int]:
        return TimeLiteral(self.value)

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Literal[int]:
        return TimestampLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        unscaled = Decimal(self.value)
        if type_var.scale == 0:
            return DecimalLiteral(unscaled)
        else:
            sign, digits, _ = unscaled.as_tuple()
            zeros = (0,) * type_var.scale
            return DecimalLiteral(Decimal((sign, digits + zeros, -type_var.scale)))


class FloatLiteral(Literal[float]):
    def __init__(self, value: float):
        super().__init__(value=value)
        self._value32 = struct.unpack("<f", struct.pack("<f", value))[0]

    def __eq__(self, other):
        return self._value32 == other

    def __lt__(self, other):
        return self._value32 < other

    def __gt__(self, other):
        return self._value32 > other

    def __le__(self, other):
        return self._value32 <= other

    def __ge__(self, other):
        return self._value32 >= other

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> Literal[float]:
        return self

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> Literal[float]:
        return DoubleLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DoubleLiteral(Literal[float]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DoubleType)
    def _(self, type_var: DoubleType) -> Literal[float]:
        return self

    @to.register(FloatType)
    def _(self, type_var: FloatType) -> Union[AboveMax, BelowMin, Literal[float]]:
        if FloatType.max < self.value:
            return AboveMax()
        elif FloatType.min > self.value:
            return BelowMin()
        return FloatLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DateLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[int]:
        return self


class TimeLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> Literal[int]:
        return self


class TimestampLiteral(Literal[int]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Literal[int]:
        return self

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[int]:
        return DateLiteral((datetime.fromtimestamp(self.value / 1_000_000) - EPOCH_TIMESTAMP).days)


class DecimalLiteral(Literal[Decimal]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Optional[Literal[Decimal]]:
        if type_var.scale == abs(self.value.as_tuple().exponent):
            return self
        return None


class StringLiteral(Literal[str]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(StringType)
    def _(self, type_var: StringType) -> Literal[str]:
        return self

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[int]:
        return DateLiteral((date.fromisoformat(self.value) - EPOCH_DATE).days)

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> Literal[int]:
        t = time.fromisoformat(self.value)
        return TimeLiteral((((t.hour * 60 + t.minute) * 60) + t.second) * 1_000_000 + t.microsecond)

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Optional[Literal[int]]:
        if ISO_TIMESTAMP.fullmatch(self.value):
            try:
                delta = datetime.fromisoformat(self.value) - EPOCH_TIMESTAMP
                return TimestampLiteral((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
            except TypeError:
                return None
        return None

    @to.register(TimestamptzType)
    def _(self, type_var: TimestamptzType) -> Optional[Literal[int]]:
        if ISO_TIMESTAMPTZ.fullmatch(self.value):
            try:
                delta = datetime.fromisoformat(self.value) - EPOCH_TIMESTAMPTZ
                return TimestampLiteral((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
            except TypeError:
                return None
        return None

    @to.register(UUIDType)
    def _(self, type_var: UUIDType) -> Literal[UUID]:
        return UUIDLiteral(UUID(self.value))

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Optional[Literal[Decimal]]:
        dec = Decimal(self.value)
        if type_var.scale == abs(dec.as_tuple().exponent):
            return DecimalLiteral(dec)
        else:
            return None


class UUIDLiteral(Literal[UUID]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(UUIDType)
    def _(self, type_var: UUIDType) -> Literal[UUID]:
        return self


class FixedLiteral(Literal[bytes]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(FixedType)
    def _(self, type_var: FixedType) -> Optional[Literal[bytes]]:
        if len(self.value) == type_var.length:
            return self
        else:
            return None

    @to.register(BinaryType)
    def _(self, type_var: BinaryType) -> Literal[bytes]:
        return BinaryLiteral(self.value)


class BinaryLiteral(Literal[bytes]):
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BinaryType)
    def _(self, type_var: BinaryType) -> Literal[bytes]:
        return self

    @to.register(FixedType)
    def _(self, type_var: FixedType) -> Optional[Literal[bytes]]:
        if type_var.length == len(self.value):
            return FixedLiteral(self.value)
        else:
            return None

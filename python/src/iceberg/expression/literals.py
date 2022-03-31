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
        LongLiteral(123)
    """
    raise TypeError(f"Unimplemented Type Literal for value: {str(value)}")


@literal.register(bool)
def _(value: bool) -> Literal[bool]:
    return BooleanLiteral(value)


@literal.register(int)
def _(value: int) -> "LongLiteral":
    # expression binding can convert to IntegerLiteral if needed
    return LongLiteral(value)


@literal.register(float)
def _(value: float) -> "DoubleLiteral":
    # expression binding can convert to FloatLiteral if needed
    return DoubleLiteral(value)


@literal.register(str)
def _(value: str) -> Literal[str]:
    return StringLiteral(value)


@literal.register(uuid.UUID)
def _(value: uuid.UUID) -> Literal[uuid.UUID]:
    return UUIDLiteral(value)


@literal.register(bytes)
def _(value: bytes) -> Literal[bytes]:
    # expression binding can convert to FixedLiteral if needed
    return BinaryLiteral(value)


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
    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BooleanType)
    def _(self, type_var):
        return self


class IntegerLiteral(Literal[int]):
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
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


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
        return FloatLiteral(float(self.value))

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
        return DateLiteral((datetime.datetime.fromtimestamp(self.value / 1_000_000) - EPOCH).days)


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
        from datetime import date

        EPOCH_DATE = date.fromisoformat("1970-01-01")
        return DateLiteral((date.fromisoformat(self.value) - EPOCH_DATE).days)

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> "TimeLiteral":
        from datetime import time

        t = time.fromisoformat(self.value)
        return TimeLiteral((((t.hour * 60 + t.minute) * 60) + t.second) * 1_000_000 + t.microsecond)

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Optional[TimestampLiteral]:
        import re
        from datetime import datetime

        EPOCH_TIMESTAMP = datetime.fromisoformat("1970-01-01T00:00:00.000000")
        ISO_TIMESTAMP = re.compile(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d{1,6})?")
        if ISO_TIMESTAMP.fullmatch(self.value):
            try:
                delta = datetime.fromisoformat(self.value) - EPOCH_TIMESTAMP
                return TimestampLiteral((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
            except TypeError:
                return None
        return None

    @to.register(TimestamptzType)
    def _(self, type_var: TimestamptzType) -> Optional[TimestampLiteral]:
        import re
        from datetime import datetime

        EPOCH_TIMESTAMPTZ = datetime.fromisoformat("1970-01-01T00:00:00.000000+00:00")
        ISO_TIMESTAMPTZ = re.compile(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d{1,6})?[-+]\d\d:\d\d")
        ...
        if ISO_TIMESTAMPTZ.fullmatch(self.value):
            try:
                delta = datetime.fromisoformat(self.value) - EPOCH_TIMESTAMPTZ
                return TimestampLiteral((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
            except TypeError:
                return None
        return None

    @to.register(UUIDType)
    def _(self, type_var: UUIDType) -> "UUIDLiteral":
        return UUIDLiteral(uuid.UUID(self.value))

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> DecimalLiteral:
        dec_val = Decimal(self.value)
        value_scale = abs(dec_val.as_tuple().exponent)
        if value_scale == type_var.scale:
            return DecimalLiteral(Decimal(self.value))
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
    def _(self, type_var: FixedType) -> Optional["FixedLiteral"]:
        if len(self.value) == type_var.length:
            return self
        else:
            return None

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
    def _(self, type_var: FixedType) -> Optional["FixedLiteral"]:
        if type_var.length >= len(self.value):
            return FixedLiteral(self.value)
        else:
            return None

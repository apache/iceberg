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
# pylint: disable=W0613

import struct
from decimal import ROUND_HALF_UP, Decimal
from functools import singledispatch, singledispatchmethod
from typing import Optional, Union
from uuid import UUID

from pyiceberg.expressions import Literal
from pyiceberg.types import (
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
from pyiceberg.utils.datetime import (
    date_to_days,
    micros_to_days,
    time_to_micros,
    timestamp_to_micros,
    timestamptz_to_micros,
)
from pyiceberg.utils.singleton import Singleton


@singledispatch
def literal(value) -> Literal:
    """
    A generic Literal factory to construct an iceberg Literal based on python primitive data type
    using dynamic overloading

    Args:
        value(python primitive type): the value to be associated with literal

    Example:
        from pyiceberg.expression.expressions.literals import literal
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


class AboveMax(Singleton):
    @property
    def value(self):
        raise ValueError("AboveMax has no value")

    def to(self, type_var):
        raise TypeError("Cannot change the type of AboveMax")

    def __repr__(self):
        return "AboveMax()"

    def __str__(self):
        return "AboveMax"


class BelowMin(Singleton):
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
    def __init__(self, value: bool):
        super().__init__(value, bool)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BooleanType)
    def _(self, type_var):
        return self


class LongLiteral(Literal[int]):
    def __init__(self, value: int):
        super().__init__(value, int)

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
        super().__init__(value, float)
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
    def __init__(self, value: float):
        super().__init__(value, float)

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
    def __init__(self, value: int):
        super().__init__(value, int)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[int]:
        return self


class TimeLiteral(Literal[int]):
    def __init__(self, value: int):
        super().__init__(value, int)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> Literal[int]:
        return self


class TimestampLiteral(Literal[int]):
    def __init__(self, value: int):
        super().__init__(value, int)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Literal[int]:
        return self

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[int]:
        return DateLiteral(micros_to_days(self.value))


class DecimalLiteral(Literal[Decimal]):
    def __init__(self, value: Decimal):
        super().__init__(value, Decimal)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Optional[Literal[Decimal]]:
        if type_var.scale == abs(self.value.as_tuple().exponent):
            return self
        return None


class StringLiteral(Literal[str]):
    def __init__(self, value: str):
        super().__init__(value, str)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(StringType)
    def _(self, type_var: StringType) -> Literal[str]:
        return self

    @to.register(DateType)
    def _(self, type_var: DateType) -> Optional[Literal[int]]:
        try:
            return DateLiteral(date_to_days(self.value))
        except (TypeError, ValueError):
            return None

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> Optional[Literal[int]]:
        try:
            return TimeLiteral(time_to_micros(self.value))
        except (TypeError, ValueError):
            return None

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Optional[Literal[int]]:
        try:
            return TimestampLiteral(timestamp_to_micros(self.value))
        except (TypeError, ValueError):
            return None

    @to.register(TimestamptzType)
    def _(self, type_var: TimestamptzType) -> Optional[Literal[int]]:
        try:
            return TimestampLiteral(timestamptz_to_micros(self.value))
        except (TypeError, ValueError):
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
    def __init__(self, value: UUID):
        super().__init__(value, UUID)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(UUIDType)
    def _(self, type_var: UUIDType) -> Literal[UUID]:
        return self


class FixedLiteral(Literal[bytes]):
    def __init__(self, value: bytes):
        super().__init__(value, bytes)

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
    def __init__(self, value: bytes):
        super().__init__(value, bytes)

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

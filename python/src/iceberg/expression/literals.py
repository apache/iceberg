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
import sys
import uuid
from decimal import ROUND_HALF_UP, Decimal
from functools import singledispatch
from typing import Any

from iceberg.types import PrimitiveType

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

"""
Iceberg literal is wrapper class used in expressions, which return unbound predicates
It's being organized as below
Literal
|-- AboveMax
|-- BelowMin
|-- BaseLiteral
    |-- FixedLiteral
    |-- BinaryLiteral
    |-- ComparableLiteral
        |-- BooleanLiteral
        |-- IntegerLiteral
        |-- LongLiteral
        |-- FloatLiteral
        |-- DoubleLiteral
        |-- DateLiteral
        |-- StringLiteral
        |-- TimeLiteral
        |-- TimestampLiteral
        |-- DecimalLiteral
        |-- UUIDLiteral
"""


class Literal:
    def to(self, type_var: PrimitiveType):
        raise NotImplementedError()

    def __str__(self):
        return type(self).__name__


class BaseLiteral(Literal):
    """Base literal which has a value and can be converted between types"""

    def __init__(self, repr_string: str, value):
        self._repr_string = repr_string
        if value is None:
            raise TypeError("Cannot set value of BaseLiteral to None")
        self._value = value

    def to(self, type_var):
        raise NotImplementedError()

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return str(self._value)


@singledispatch
def literal(value: Any) -> BaseLiteral:
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
    raise TypeError(f"Unimplemented Type Literal for value: {value}")


@literal.register
def _(value: bool):
    return BooleanLiteral(value)


@literal.register
def _(value: int):
    """
    Upgrade to long if python int is outside the JAVA_MIN_INT and JAVA_MAX_INT
    """
    if value < IntegerType.min or value > IntegerType.max:
        return LongLiteral(value)
    return IntegerLiteral(value)


@literal.register
def _(value: float):
    """
    Upgrade to double if python float is outside the JAVA_MIN_FLOAT and JAVA_MAX_FLOAT
    """
    if value < FloatType.min or value > FloatType.max:
        return DoubleLiteral(value)
    return FloatLiteral(value)


@literal.register
def _(value: str):
    return StringLiteral(value)


@literal.register
def _(value: uuid.UUID):
    return UUIDLiteral(value)


@literal.register
def _(value: bytes):
    return FixedLiteral(value)


@literal.register
def _(value: bytearray):
    return BinaryLiteral(value)


@literal.register
def _(value: Decimal):
    return DecimalLiteral(value)


class ComparableLiteral(BaseLiteral):
    def __init__(self, repr_string: str, value):
        super(ComparableLiteral, self).__init__(repr_string, value)

    def to(self, type_var):
        raise NotImplementedError()

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


class AboveMax(Literal, Singleton):
    def value(self):
        raise RuntimeError("AboveMax has no value")

    def to(self, type_var):
        raise RuntimeError("Cannot change the type of AboveMax")


class BelowMin(Literal, Singleton):
    def value(self):
        raise RuntimeError("BelowMin has no value")

    def to(self, type_var):
        raise RuntimeError("Cannot change the type of BelowMin")


class BooleanLiteral(ComparableLiteral):
    def __init__(self, value):
        super(BooleanLiteral, self).__init__(f"BooleanLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BooleanType)
    def _(self, type_var):
        return self


class IntegerLiteral(ComparableLiteral):
    def __init__(self, value):
        super(IntegerLiteral, self).__init__(f"IntegerLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(IntegerType)
    def _(self, type_var):
        return self

    @to.register(LongType)
    def _(self, type_var):
        return LongLiteral(self.value)

    @to.register(FloatType)
    def _(self, type_var):
        return FloatLiteral(self.value)

    @to.register(DoubleType)
    def _(self, type_var):
        return DoubleLiteral(self.value)

    @to.register(DateType)
    def _(self, type_var):
        return DateLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var):
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value))
        else:
            return DecimalLiteral(
                Decimal(self.value).quantize(
                    Decimal("." + "".join(["0" for _ in range(1, type_var.scale)]) + "1"), rounding=ROUND_HALF_UP
                )
            )


class LongLiteral(ComparableLiteral):
    def __init__(self, value):
        super(LongLiteral, self).__init__(f"LongLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(LongType)
    def _(self, type_var):
        return self

    @to.register(IntegerType)
    def _(self, type_var):
        if IntegerType.max < self.value:
            return AboveMax()
        elif IntegerType.min > self.value:
            return BelowMin()
        return IntegerLiteral(self.value)

    @to.register(FloatType)
    def _(self, type_var):
        return FloatLiteral(self.value)

    @to.register(DoubleType)
    def _(self, type_var):
        return DoubleLiteral(self.value)

    @to.register(TimeType)
    def _(self, type_var):
        return TimeLiteral(self.value)

    @to.register(TimestampType)
    def _(self, type_var):
        return TimestampLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var):
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value))
        else:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class FloatLiteral(ComparableLiteral):
    def __init__(self, value):
        super(FloatLiteral, self).__init__(f"FloatLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(FloatType)
    def _(self, type_var):
        return self

    @to.register(DoubleType)
    def _(self, type_var):
        return DoubleLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var):
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal("1."), rounding=ROUND_HALF_UP))
        else:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DoubleLiteral(ComparableLiteral):
    def __init__(self, value):
        super(DoubleLiteral, self).__init__(f"DoubleLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DoubleType)
    def _(self, type_var):
        return self

    @to.register(FloatType)
    def _(self, type_var):
        if FloatType.max < self.value:
            return AboveMax()
        elif FloatType.min > self.value:
            return BelowMin()
        return FloatLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var):
        if type_var.scale == 0:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal("1."), rounding=ROUND_HALF_UP))
        else:
            return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DateLiteral(ComparableLiteral):
    def __init__(self, value):
        super(DateLiteral, self).__init__(f"DateLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DateType)
    def _(self, type_var):
        return self


class TimeLiteral(ComparableLiteral):
    def __init__(self, value):
        super(TimeLiteral, self).__init__(f"TimeLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimeType)
    def _(self, type_var):
        return self


class TimestampLiteral(ComparableLiteral):
    def __init__(self, value):
        super(TimestampLiteral, self).__init__(f"TimestampLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(TimestampType)
    def _(self, type_var):
        return self

    @to.register(DateType)
    def _(self, type_var):
        return DateLiteral((datetime.datetime.fromtimestamp(self.value / 1000000) - EPOCH).days)


class DecimalLiteral(ComparableLiteral):
    def __init__(self, value):
        super(DecimalLiteral, self).__init__(f"DecimalLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(DecimalType)
    def _(self, type_var):
        if type_var.scale == abs(self.value.as_tuple().exponent):
            return self
        return None


class StringLiteral(ComparableLiteral):
    def __init__(self, value):
        super(StringLiteral, self).__init__(f"StringLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(StringType)
    def _(self, type_var):
        return self

    @to.register(DateType)
    def _(self, type_var):
        import dateutil.parser

        return DateLiteral((dateutil.parser.parse(self.value) - EPOCH).days)

    @to.register(TimeType)
    def _(self, type_var):
        import dateutil.parser

        return TimeLiteral(
            int((dateutil.parser.parse(EPOCH.strftime("%Y-%m-%d ") + self.value) - EPOCH).total_seconds() * 1000000)
        )

    @to.register(TimestampType)
    def _(self, type_var):
        import dateutil.parser

        timestamp = dateutil.parser.parse(self.value)
        if bool(timestamp.tzinfo):
            raise RuntimeError(f"Cannot convert StringLiteral to {type_var} when timezone is included in {self.value}")
        return TimestampLiteral(int((timestamp - EPOCH).total_seconds() * 1_000_000))

    @to.register(TimestamptzType)
    def _(self, type_var):
        import dateutil.parser

        timestamp = dateutil.parser.parse(self.value)
        if not bool(timestamp.tzinfo):
            raise RuntimeError(f"Cannot convert StringLiteral to {type_var} when string {self.value} if miss timezones")
        utc_epoch = EPOCH.replace(tzinfo=pytz.UTC)
        return TimestampLiteral(int((timestamp - utc_epoch).total_seconds() * 1_000_000))

    @to.register(UUIDType)
    def _(self, type_var):
        return UUIDLiteral(uuid.UUID(self.value))

    @to.register(DecimalType)
    def _(self, type_var):
        dec_val = Decimal(str(self.value))
        if abs(dec_val.as_tuple().exponent) == type_var.scale:
            if type_var.scale == 0:
                return DecimalLiteral(Decimal(str(self.value)).quantize(Decimal("1."), rounding=ROUND_HALF_UP))
            else:
                return DecimalLiteral(
                    Decimal(str(self.value)).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP)
                )

    def __str__(self):
        return f'"{self.value}"'


class UUIDLiteral(ComparableLiteral):
    def __init__(self, value):
        super(UUIDLiteral, self).__init__(f"UUIDLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(UUIDType)
    def _(self, type_var):
        return self


class FixedLiteral(BaseLiteral):
    def __init__(self, value):
        super(FixedLiteral, self).__init__(f"FixedLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(FixedType)
    def _(self, type_var):
        if len(self.value) == type_var.length:
            return self
        return None

    @to.register(BinaryType)
    def _(self, type_var):
        return BinaryLiteral(self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if other is None:
            return False

        return self.value < other.value

    def __gt__(self, other):
        if other is None:
            return True

        return self.value > other.value

    def __le__(self, other):
        if other is None:
            return False

        return self.value <= other.value

    def __ge__(self, other):
        if other is None:
            return True

        return self.value >= other.value


class BinaryLiteral(BaseLiteral):
    def __init__(self, value):
        super(BinaryLiteral, self).__init__(f"BinaryLiteral({value})", value)

    @singledispatchmethod
    def to(self, type_var):
        return None

    @to.register(BinaryType)
    def _(self, type_var):
        return self

    @to.register(FixedType)
    def _(self, type_var):
        if type_var.length == len(self.value):
            return FixedLiteral(self.value)
        return None

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if other is None:
            return False

        return self.value < other.value

    def __gt__(self, other):
        if other is None:
            return True

        return self.value > other.value

    def __le__(self, other):
        if other is None:
            return False

        return self.value <= other.value

    def __ge__(self, other):
        if other is None:
            return True

        return self.value >= other.value

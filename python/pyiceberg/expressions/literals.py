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
from __future__ import annotations

import struct
from abc import ABC, abstractmethod
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from functools import singledispatchmethod
from typing import Generic, Type, Union, TypeVar, Any
from uuid import UUID

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils.datetime import (
    date_str_to_date,
    date_to_days,
    days_to_date,
    micros_to_date,
    time_to_micros,
    timestamp_to_micros,
    timestamptz_to_micros,
)
from pyiceberg.utils.singleton import Singleton

T = TypeVar('T')

class Literal(Generic[T], ABC):
    """Literal which has a value and can be converted between types"""

    _value: T

    def __init__(self, value: T, value_type: Type[T]):
        if value is None or not isinstance(value, value_type):
            raise TypeError(f"Invalid literal value: {value!r} (not a {value_type})")
        self._value = value

    @property
    def value(self) -> T:
        return self._value

    @singledispatchmethod
    @abstractmethod
    def to(self, type_var: IcebergType) -> Literal:
        ...  # pragma: no cover

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value!r})"

    def __str__(self) -> str:
        return str(self.value)

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other: Any) -> bool:
        return self.value == other.value

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __lt__(self, other) -> bool:
        return self.value < other.value

    def __gt__(self, other) -> bool:
        return self.value > other.value

    def __le__(self, other) -> bool:
        return self.value <= other.value

    def __ge__(self, other) -> bool:
        return self.value >= other.value


def literal(value: T) -> Literal[T]:
    """
    A generic Literal factory to construct an iceberg Literal based on python primitive data type

    Args:
        value(python primitive type): the value to be associated with literal

    Example:
        from pyiceberg.expressions.literals import literal
        >>> literal(123)
        LongLiteral(123)
    """
    # Issues with the TypeVar: https://github.com/python/mypy/issues/9424
    # Therefore we have to ignore the types, however they are annotated properly:
    #
    # reveal_type(literal(True)): Revealed type is "pyiceberg.expressions.literals.Literal[builtins.bool]"
    # reveal_type(literal("str")): note: Revealed type is "pyiceberg.expressions.literals.Literal[builtins.str]"
    # reveal_type(literal(123)): Revealed type is "pyiceberg.expressions.literals.Literal[builtins.int]"
    if isinstance(value, bool):
        return BooleanLiteral(value)  # type: ignore
    if isinstance(value, int):
        return LongLiteral(value)  # type: ignore
    if isinstance(value, float):
        return DoubleLiteral(value)  # type: ignore
    if isinstance(value, str):
        return StringLiteral(value)  # type: ignore
    if isinstance(value, UUID):
        return UUIDLiteral(value)  # type: ignore
    if isinstance(value, bytes):
        return BinaryLiteral(value)  # type: ignore
    if isinstance(value, Decimal):
        return DecimalLiteral(value)  # type: ignore
    if isinstance(value, date):
        return DateLiteral(value)  # type: ignore

    raise TypeError(f"Invalid literal value: {repr(value)}")


class FloatAboveMax(Literal[float], Singleton):
    def __init__(self):
        super().__init__(FloatType.max, float)

    def to(self, type_var: IcebergType) -> Literal:  # type: ignore
        raise TypeError("Cannot change the type of FloatAboveMax")

    def __repr__(self) -> str:
        return "FloatAboveMax()"

    def __str__(self) -> str:
        return "FloatAboveMax"


class FloatBelowMin(Literal[float], Singleton):
    def __init__(self):
        super().__init__(FloatType.min, float)

    def to(self, type_var: IcebergType) -> Literal:  # type: ignore
        raise TypeError("Cannot change the type of FloatBelowMin")

    def __repr__(self) -> str:
        return "FloatBelowMin()"

    def __str__(self) -> str:
        return "FloatBelowMin"


class IntAboveMax(Literal[int]):
    def __init__(self):
        super().__init__(IntegerType.max, int)

    def to(self, type_var: IcebergType) -> Literal:  # type: ignore
        raise TypeError("Cannot change the type of IntAboveMax")

    def __repr__(self) -> str:
        return "IntAboveMax()"

    def __str__(self) -> str:
        return "IntAboveMax"


class IntBelowMin(Literal[int]):
    def __init__(self):
        super().__init__(IntegerType.min, int)

    def to(self, type_var: IcebergType) -> Literal:  # type: ignore
        raise TypeError("Cannot change the type of IntBelowMin")

    def __repr__(self) -> str:
        return "IntBelowMin()"

    def __str__(self) -> str:
        return "IntBelowMin"


class BooleanLiteral(Literal[bool]):
    def __init__(self, value: bool):
        super().__init__(value, bool)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert BooleanLiteral into {type_var}")

    @to.register(BooleanType)
    def _(self, _: BooleanType) -> Literal[bool]:
        return self


class LongLiteral(Literal[int]):
    def __init__(self, value: int):
        super().__init__(value, int)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert LongLiteral into {type_var}")

    @to.register(LongType)
    def _(self, _: LongType) -> Literal[int]:
        return self

    @to.register(IntegerType)
    def _(self, _: IntegerType) -> Literal[int]:
        if IntegerType.max < self.value:
            return IntAboveMax()
        elif IntegerType.min > self.value:
            return IntBelowMin()
        return self

    @to.register(FloatType)
    def _(self, _: FloatType) -> Literal[float]:
        return FloatLiteral(float(self.value))

    @to.register(DoubleType)
    def _(self, _: DoubleType) -> Literal[float]:
        return DoubleLiteral(float(self.value))

    @to.register(DateType)
    def _(self, _: DateType) -> Literal[date]:
        return DateLiteral(days_to_date(self.value))

    @to.register(TimeType)
    def _(self, _: TimeType) -> Literal[int]:
        return TimeLiteral(self.value)

    @to.register(TimestampType)
    def _(self, _: TimestampType) -> Literal[int]:
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

    def __eq__(self, other) -> bool:
        return self._value32 == other

    def __lt__(self, other) -> bool:
        return self._value32 < other

    def __gt__(self, other) -> bool:
        return self._value32 > other

    def __le__(self, other) -> bool:
        return self._value32 <= other

    def __ge__(self, other) -> bool:
        return self._value32 >= other

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert FloatLiteral into {type_var}")

    @to.register(FloatType)
    def _(self, _: FloatType) -> Literal[float]:
        return self

    @to.register(DoubleType)
    def _(self, _: DoubleType) -> Literal[float]:
        return DoubleLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DoubleLiteral(Literal[float]):
    def __init__(self, value: float):
        super().__init__(value, float)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert DoubleLiteral into {type_var}")

    @to.register(DoubleType)
    def _(self, _: DoubleType) -> Literal[float]:
        return self

    @to.register(FloatType)
    def _(self, _: FloatType) -> Union[FloatAboveMax, FloatBelowMin, FloatLiteral]:
        if FloatType.max < self.value:
            return FloatAboveMax()
        elif FloatType.min > self.value:
            return FloatBelowMin()
        return FloatLiteral(self.value)

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        return DecimalLiteral(Decimal(self.value).quantize(Decimal((0, (1,), -type_var.scale)), rounding=ROUND_HALF_UP))


class DateLiteral(Literal[date]):
    def __init__(self, value: date):
        super().__init__(value, date)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert DateLiteral into {type_var}")

    @to.register(DateType)
    def _(self, _: DateType) -> Literal[date]:
        return self

    @to.register(IntegerType)
    def _(self, _: IntegerType) -> Literal[int]:
        return LongLiteral(date_to_days(self.value))

    @to.register(LongType)
    def _(self, _: LongType) -> Literal[int]:
        return LongLiteral(date_to_days(self.value))


class TimeLiteral(Literal[int]):
    def __init__(self, value: int):
        super().__init__(value, int)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert TimeLiteral into {type_var}")

    @to.register(TimeType)
    def _(self, _: TimeType) -> Literal[int]:
        return self


class TimestampLiteral(Literal[int]):
    def __init__(self, value: int):
        super().__init__(value, int)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert TimestampLiteral into {type_var}")

    @to.register(TimestampType)
    def _(self, _: TimestampType) -> Literal[int]:
        return self

    @to.register(DateType)
    def _(self, _: DateType) -> Literal[date]:
        return DateLiteral(micros_to_date(self.value))


class DecimalLiteral(Literal[Decimal]):
    def __init__(self, value: Decimal):
        super().__init__(value, Decimal)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert DecimalLiteral into {type_var}")

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        if type_var.scale == abs(self.value.as_tuple().exponent):
            return self
        raise ValueError(f"Could not convert {self.value} into a {type_var}")


class StringLiteral(Literal[str]):
    def __init__(self, value: str):
        super().__init__(value, str)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert StringLiteral into {type_var}")

    @to.register(StringType)
    def _(self, _: StringType) -> Literal[str]:
        return self

    @to.register(IntegerType)
    def _(self, type_var: IntegerType) -> Union[IntAboveMax, IntBelowMin, LongLiteral]:
        try:
            number = int(float(self.value))

            if IntegerType.max < number:
                return IntAboveMax()
            elif IntegerType.min > number:
                return IntBelowMin()
            return LongLiteral(number)
        except ValueError as e:
            raise ValueError(f"Could not convert {self.value} into a {type_var}") from e

    @to.register(LongType)
    def _(self, type_var: LongType) -> Literal[int]:
        try:
            return LongLiteral(int(float(self.value)))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Could not convert {self.value} into a {type_var}") from e

    @to.register(DateType)
    def _(self, type_var: DateType) -> Literal[date]:
        try:
            return DateLiteral(date_str_to_date(self.value))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Could not convert {self.value} into a {type_var}") from e

    @to.register(TimeType)
    def _(self, type_var: TimeType) -> Literal[int]:
        try:
            return TimeLiteral(time_to_micros(self.value))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Could not convert {self.value} into a {type_var}") from e

    @to.register(TimestampType)
    def _(self, type_var: TimestampType) -> Literal[int]:
        try:
            return TimestampLiteral(timestamp_to_micros(self.value))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Could not convert {self.value} into a {type_var}") from e

    @to.register(TimestamptzType)
    def _(self, _: TimestamptzType) -> Literal[int]:
        return TimestampLiteral(timestamptz_to_micros(self.value))

    @to.register(UUIDType)
    def _(self, _: UUIDType) -> Literal[UUID]:
        return UUIDLiteral(UUID(self.value))

    @to.register(DecimalType)
    def _(self, type_var: DecimalType) -> Literal[Decimal]:
        dec = Decimal(self.value)
        if type_var.scale == abs(dec.as_tuple().exponent):
            return DecimalLiteral(dec)
        else:
            raise ValueError(f"Could not convert {self.value} into a {type_var}")

    def __repr__(self) -> str:
        return f"literal({repr(self.value)})"


class UUIDLiteral(Literal[UUID]):
    def __init__(self, value: UUID):
        super().__init__(value, UUID)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert UUIDLiteral into {type_var}")

    @to.register(UUIDType)
    def _(self, _: UUIDType) -> Literal[UUID]:
        return self


class FixedLiteral(Literal[bytes]):
    def __init__(self, value: bytes):
        super().__init__(value, bytes)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert FixedLiteral into {type_var}")

    @to.register(FixedType)
    def _(self, type_var: FixedType) -> Literal[bytes]:
        if len(self.value) == len(type_var):
            return self
        else:
            raise ValueError(f"Could not convert {self.value!r} into a {type_var}")

    @to.register(BinaryType)
    def _(self, _: BinaryType) -> Literal[bytes]:
        return BinaryLiteral(self.value)


class BinaryLiteral(Literal[bytes]):
    def __init__(self, value: bytes):
        super().__init__(value, bytes)

    @singledispatchmethod
    def to(self, type_var: IcebergType) -> Literal:
        raise TypeError(f"Cannot convert BinaryLiteral into {type_var}")

    @to.register(BinaryType)
    def _(self, _: BinaryType) -> Literal[bytes]:
        return self

    @to.register(FixedType)
    def _(self, type_var: FixedType) -> Literal[bytes]:
        if len(type_var) == len(self.value):
            return FixedLiteral(self.value)
        else:
            raise TypeError(
                f"Cannot convert BinaryLiteral into {type_var}, different length: {len(type_var)} <> {len(self.value)}"
            )

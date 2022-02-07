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

import re
import struct
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional
from uuid import UUID

import mmh3  # type: ignore

from iceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    FixedType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    Truncatable,
    Type,
    UUIDType,
)
from iceberg.utils import transform_util


class Transform:
    """Transform base class for concrete transforms.

    A base class to transform values and project predicates on partition values.
    This class is not used directly. Instead, use one of module method to create the child classes.

    Args:
        transform_string (str): name of the transform type
        repr_string (str): string representation of a transform instance
    """

    def __init__(self, transform_string: str, repr_string: str):
        self._transform_string = transform_string
        self._repr_string = repr_string

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._transform_string

    def __call__(self, value):
        return self.apply(value)

    def apply(self, value):
        raise NotImplementedError()

    def can_transform(self, target: Type) -> bool:
        return False

    def result_type(self, source: Type) -> Type:
        return source

    def preserves_order(self) -> bool:
        return False

    def satisfies_order_of(self, other) -> bool:
        return self == other

    def to_human_string(self, value) -> str:
        if value is None:
            return "null"
        return str(value)

    def dedup_name(self) -> str:
        return self._transform_string


class BaseBucketTransform(Transform):
    """Base Transform class to transform a value into a bucket partition value

    Transforms are parameterized by a number of buckets. Bucket partition transforms use a 32-bit
    hash of the source value to produce a positive value by mod the bucket number.

    Args:
      source_type (Type): An Iceberg Type of IntegerType, LongType, DecimalType, DateType, TimeType,
      TimestampType, TimestamptzType, StringType, BinaryType, FixedType, UUIDType.
      num_buckets (int): The number of buckets.
    """

    def __init__(self, source_type: Type, num_buckets: int):
        super().__init__(
            f"bucket[{num_buckets}]",
            f"transforms.bucket(source_type={repr(source_type)}, num_buckets={num_buckets})",
        )
        self._num_buckets = num_buckets

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    def hash(self, value) -> int:
        raise NotImplementedError()

    def apply(self, value) -> Optional[int]:
        if value is None:
            return None

        return (self.hash(value) & IntegerType.max) % self._num_buckets

    def can_transform(self, target: Type) -> bool:
        raise NotImplementedError()

    def result_type(self, source: Type) -> Type:
        return IntegerType()


class BucketIntegerTransform(BaseBucketTransform):
    """Transforms a value of IntegerType or DateType into a bucket partition value

    Example:
        >>> transform = BucketIntegerTransform(100)
        >>> transform.apply(34)
        79
    """

    def can_transform(self, target: Type) -> bool:
        return type(target) in {IntegerType, DateType}

    def hash(self, value) -> int:
        return mmh3.hash(struct.pack("q", value))


class BucketLongTransform(BaseBucketTransform):
    """Transforms a value of LongType, TimeType, TimestampType, or TimestamptzType
    into a bucket partition value

    Example:
        >>> transform = BucketLongTransform(100)
        >>> transform.apply(81068000000)
        59
    """

    def can_transform(self, target: Type) -> bool:
        return type(target) in {LongType, TimeType, TimestampType, TimestamptzType}

    def hash(self, value) -> int:
        return mmh3.hash(struct.pack("q", value))


class BucketDoubleTransform(BaseBucketTransform):
    """Transforms a value of FloatType or DoubleType into a bucket partition value.

    Note that bucketing by Double is not allowed by the spec, but this has the hash implementation.

    Example:
        >>> transform = BucketDoubleTransform(8)
        >>> transform.hash(1.0)
        -142385009
    """

    def hash(self, value) -> int:
        return mmh3.hash(struct.pack("d", value))


class BucketDecimalTransform(BaseBucketTransform):
    """Transforms a value of DecimalType into a bucket partition value.

    Example:
        >>> transform = BucketDecimalTransform(100)
        >>> transform.apply(Decimal("14.20"))
        59
    """

    def can_transform(self, target: Type) -> bool:
        return isinstance(target, DecimalType)

    def hash(self, value: Decimal) -> int:
        return mmh3.hash(transform_util.decimal_to_bytes(value))


class BucketStringTransform(BaseBucketTransform):
    """Transforms a value of StringType into a bucket partition value.

    Example:
        >>> transform = BucketStringTransform(100)
        >>> transform.apply("iceberg")
        89
    """

    def can_transform(self, target: Type) -> bool:
        return isinstance(target, StringType)

    def hash(self, value: str) -> int:
        return mmh3.hash(value)


class BucketFixedTransform(BaseBucketTransform):
    """Transforms a value of FixedType into a bucket partition value.

    Example:
        >>> transform = BucketFixedTransform(128)
        >>> transform.apply(b"foo")
        32
    """

    def can_transform(self, target: Type) -> bool:
        return isinstance(target, FixedType)

    def hash(self, value: bytearray) -> int:
        return mmh3.hash(value)


class BucketBinaryTransform(BaseBucketTransform):
    """Transforms a value of BinaryType into a bucket partition value.

    Example:
        >>> transform = BucketBinaryTransform(128)
        >>> transform.apply(b"\x00\x01\x02\x03")
        57
    """

    def can_transform(self, target: Type) -> bool:
        return isinstance(target, BinaryType)

    def hash(self, value: bytes) -> int:
        return mmh3.hash(value)


class BucketUUIDTransform(BaseBucketTransform):
    """Transforms a value of UUIDType into a bucket partition value.

    Example:
        >>> transform = BucketUUIDTransform(100)
        >>> transform.apply(UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"))
        40
    """

    def can_transform(self, target: Type) -> bool:
        return isinstance(target, UUIDType)

    def hash(self, value: UUID) -> int:
        return mmh3.hash(
            struct.pack(
                ">QQ",
                (value.int >> 64) & 0xFFFFFFFFFFFFFFFF,
                value.int & 0xFFFFFFFFFFFFFFFF,
            )
        )


class DateTimeTransform(Transform):
    """Base transform class for transforms of DateType, TimestampType, and TimestamptzType."""

    class Granularity(Enum):
        def __init__(self, order: int, result_type: Type, human_string: Callable[[int], str]):
            self.order = order
            self.result_type = result_type
            self.human_string = human_string

        YEAR = 3, IntegerType(), transform_util.human_year
        MONTH = 2, IntegerType(), transform_util.human_month
        DAY = 1, DateType(), transform_util.human_day
        HOUR = 0, IntegerType(), transform_util.human_hour

    _DATE_APPLY_FUNCS = {
        Granularity.YEAR: transform_util.years_for_days,
        Granularity.MONTH: transform_util.months_for_days,
        Granularity.DAY: lambda d: d,
    }

    _TIMESTAMP_APPLY_FUNCS = {
        Granularity.YEAR: transform_util.years_for_ts,
        Granularity.MONTH: transform_util.months_for_ts,
        Granularity.DAY: transform_util.days_for_ts,
        Granularity.HOUR: transform_util.hours_for_ts,
    }

    def __init__(self, source_type: Type, name: str):
        super().__init__(name, f"transforms.{name}(source_type={repr(source_type)})")

        self._type = source_type
        try:
            self._granularity = DateTimeTransform.Granularity[name.upper()]

            if isinstance(source_type, DateType):
                self._apply = DateTimeTransform._DATE_APPLY_FUNCS[self._granularity]
            elif type(source_type) in {TimestampType, TimestamptzType}:
                self._apply = DateTimeTransform._TIMESTAMP_APPLY_FUNCS[self._granularity]
            else:
                raise KeyError
        except KeyError:
            raise ValueError(f"Cannot partition type {source_type} by {name}")

    def __eq__(self, other):
        if type(self) is type(other):
            return self._type == other._type and self._granularity == other._granularity
        return False

    def can_transform(self, target: Type) -> bool:
        if isinstance(self._type, DateType):
            return isinstance(target, DateType)
        else:  # self._type is either TimestampType or TimestamptzType
            return not isinstance(target, DateType)

    def apply(self, value: int) -> int:
        return self._apply(value)

    def result_type(self, source_type: Type) -> Type:
        return self._granularity.result_type

    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        if self == other:
            return True

        if isinstance(other, DateTimeTransform):
            return self._granularity.order <= other._granularity.order

        return False

    def to_human_string(self, value) -> str:
        if value is None:
            return "null"
        return self._granularity.human_string(value)

    def dedup_name(self) -> str:
        return "time"


class IdentityTransform(Transform):
    def __init__(self, source_type: Type):
        super().__init__(
            "identity",
            f"transforms.identity(source_type={repr(source_type)})",
        )
        self._type = source_type

    def apply(self, value):
        return value

    def can_transform(self, target: Type) -> bool:
        return target.is_primitive

    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        return other.preserves_order()

    def to_human_string(self, value) -> str:
        if value is None:
            return "null"
        elif isinstance(self._type, DateType):
            return transform_util.human_day(value)
        elif isinstance(self._type, TimeType):
            return transform_util.human_time(value)
        elif isinstance(self._type, TimestampType):
            return transform_util.human_timestamp(value)
        elif isinstance(self._type, TimestamptzType):
            return transform_util.human_timestamptz(value)
        elif isinstance(self._type, FixedType):
            return transform_util.base64encode(value)
        elif isinstance(self._type, BinaryType):
            return transform_util.base64encode(value)
        else:
            return str(value)


class TruncateTransform(Transform):
    """A transform for truncating a value to a specified width.

    Args:
      source_type (Type): An Iceberg Truncatable Type of IntegerType, LongType, StringType, BinaryType or DecimalType
      width (int): The truncate width

    Raises:
      ValueError: If a type is provided that is incompatible with a Truncate transform
    """

    def __init__(self, source_type: Type, width: int):
        if not isinstance(source_type, Truncatable):
            raise ValueError(f"Cannot truncate type: {source_type}")

        super().__init__(
            f"truncate[{width}]",
            f"transforms.truncate(source_type={repr(source_type)}, width={width})",
        )
        self._type = source_type
        self._width = width

    @property
    def width(self):
        return self._width

    def apply(self, value):
        if value is None:
            return None
        return self._type.truncate(value, self._width)

    def can_transform(self, target: Type) -> bool:
        return self._type == target

    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        if self == other:
            return True
        elif isinstance(self._type, StringType) and isinstance(other, TruncateTransform) and isinstance(other._type, StringType):
            return self._width >= other._width

        return False

    def to_human_string(self, value) -> str:
        if value is None:
            return "null"
        elif isinstance(self._type, BinaryType):
            return transform_util.base64encode(value)
        else:
            return str(value)


class UnknownTransform(Transform):
    """A transform that represents when an unknown transform is provided

    Args:
      source_type (Type): An Iceberg `Type`
      transform (str): A string name of a transform

    Raises:
      AttributeError: If the apply method is called.
    """

    def __init__(self, source_type: Type, transform: str):
        super().__init__(
            transform,
            f"UnknownTransform(source_type={repr(source_type)}, transform={repr(transform)})",
        )
        self._type = source_type
        self._transform = transform

    def apply(self, value):
        raise AttributeError(f"Cannot apply unsupported transform: {self}")

    def can_transform(self, target: Type) -> bool:
        return self._type == target

    def result_type(self, source: Type) -> Type:
        return StringType()


class VoidTransform(Transform):
    """A transform that always returns None"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VoidTransform, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        super().__init__("void", "transforms.always_null()")

    def apply(self, value):
        return None

    def can_transform(self, target: Type) -> bool:
        return True

    def to_human_string(self, value) -> str:
        return "null"


_HAS_WIDTH = re.compile("(\\w+)\\[(\\d+)\\]")


def from_string(source_type: Type, transform: str) -> Transform:
    transform_lower = transform.lower()
    match = _HAS_WIDTH.match(transform_lower)

    if match is not None:
        name = match.group(1)
        w = int(match.group(2))
        if name == "truncate":
            return TruncateTransform(source_type, w)
        elif name == "bucket":
            return BaseBucketTransform(source_type, w)

    if transform_lower == "identity":
        return identity(source_type)

    try:
        return DateTimeTransform(source_type, transform_lower)
    except (KeyError, ValueError) as e:
        pass  # fall through to return unknown transform

    if transform_lower == "void":
        return VoidTransform()

    return UnknownTransform(source_type, transform)


def identity(source_type: Type) -> IdentityTransform:
    return IdentityTransform(source_type)


def year(source_type: Type) -> Transform:
    return DateTimeTransform(source_type, "year")


def month(source_type: Type) -> Transform:
    return DateTimeTransform(source_type, "month")


def day(source_type: Type) -> Transform:
    return DateTimeTransform(source_type, "day")


def hour(source_type: Type) -> Transform:
    return DateTimeTransform(source_type, "hour")


def bucket(source_type: Type, num_buckets: int) -> BaseBucketTransform:
    if isinstance(source_type, IntegerType):
        return BucketIntegerTransform(source_type, num_buckets)
    elif isinstance(source_type, DecimalType):
        return BucketDecimalTransform(source_type, num_buckets)
    elif isinstance(source_type, DateType):
        return BucketIntegerTransform(source_type, num_buckets)
    elif type(source_type) in {LongType, TimeType, TimestampType, TimestamptzType}:
        return BucketLongTransform(source_type, num_buckets)
    elif isinstance(source_type, StringType):
        return BucketStringTransform(source_type, num_buckets)
    elif isinstance(source_type, BinaryType):
        return BucketBinaryTransform(source_type, num_buckets)
    elif isinstance(source_type, FixedType):
        return BucketFixedTransform(source_type, num_buckets)
    elif isinstance(source_type, UUIDType):
        return BucketUUIDTransform(source_type, num_buckets)
    else:
        raise ValueError(f"Cannot bucket by type: {source_type}")


def truncate(source_type: Type, width: int) -> TruncateTransform:
    return TruncateTransform(source_type, width)


def always_null() -> Transform:
    return VoidTransform()

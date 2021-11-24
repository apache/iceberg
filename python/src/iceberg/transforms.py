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
from typing import Any, Callable, Optional

import mmh3  # type: ignore

from iceberg.types import (
    BinaryType,
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
    Type,
    UUIDType,
)
from iceberg.utils import transform_util


class Transform:
    """Transform base class for concrete transforms."""

    def __init__(
        self,
        transform_string: str,
        repr_string: str,
        to_human_str: Callable[[Any], str] = str,
    ):
        self._transform_string = transform_string
        self._repr_string = repr_string
        self._to_human_string = to_human_str

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._transform_string

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
        return self._to_human_string(value)

    def dedup_name(self) -> str:
        return self._transform_string


class Bucket(Transform):
    _MAX_32_BITS_INT = 2147483647
    _INT_TRANSFORMABLE_TYPES = {
        IntegerType,
        DateType,
        LongType,
        TimeType,
        TimestampType,
        TimestamptzType,
    }
    _SAME_TRANSFORMABLE_TYPES = {
        StringType,
        BinaryType,
        UUIDType,
        FloatType,
        DoubleType,
    }

    def __init__(self, source_type: Type, num_buckets: int):
        super().__init__(
            f"bucket[{num_buckets}]",
            f"transforms.bucket(source_type={repr(source_type)}, num_buckets={num_buckets})",
        )
        self._type = source_type
        self._num_buckets = num_buckets

        if isinstance(self._type, FixedType) or isinstance(self._type, DecimalType):
            self._can_transform = lambda t: type(self._type) is type(t)
        elif self._type in Bucket._SAME_TRANSFORMABLE_TYPES:
            self._can_transform = lambda t: self._type == t
        elif self._type in Bucket._INT_TRANSFORMABLE_TYPES:
            self._can_transform = (
                lambda t: self._type in Bucket._INT_TRANSFORMABLE_TYPES
            )
        else:
            raise ValueError(f"Cannot bucket by type: {source_type}")

        if (
            isinstance(self._type, FixedType)
            or self._type == StringType
            or self._type == BinaryType
        ):
            self._hash_func = lambda v: mmh3.hash(v)
        elif isinstance(self._type, DecimalType):
            self._hash_func = lambda v: mmh3.hash(transform_util.decimal_to_bytes(v))
        elif self._type == FloatType or self._type == DoubleType:
            # bucketing by Float/Double is not allowed by the spec, but they have hash implementation
            self._hash_func = lambda v: mmh3.hash(struct.pack("d", v))
        elif self._type == UUIDType:
            self._hash_func = lambda v: mmh3.hash(
                struct.pack(
                    ">QQ",
                    (v.int >> 64) & 0xFFFFFFFFFFFFFFFF,
                    v.int & 0xFFFFFFFFFFFFFFFF,
                )
            )
        else:
            self._hash_func = lambda v: mmh3.hash(struct.pack("q", v))

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    def apply(self, value) -> Optional[int]:
        if value is None:
            return None

        return (self._hash_func(value) & Bucket._MAX_32_BITS_INT) % self._num_buckets

    def can_transform(self, target: Type) -> bool:
        return self._can_transform(target)

    def result_type(self, source: Type):
        return IntegerType


class TimeTransform(Transform):
    """Time class is for both Date transforms and Timestamp transforms."""

    _TIME_SATISFIED_ORDER = dict(year=3, month=2, day=1, hour=0)

    def __init__(self, source_type: Type, name: str, apply_func: Callable[[int], int]):
        super().__init__(
            name,
            f"transforms.{name}(source_type={repr(source_type)})",
            getattr(transform_util, f"human_{name}"),
        )
        self._type = source_type
        self._name = name
        self._apply = apply_func
        self._result_type = DateType if self._name == "day" else IntegerType

    def apply(self, value: int) -> int:
        return self._apply(value)

    def can_transform(self, target: Type) -> bool:
        if self._type == DateType:
            return target == DateType
        else:  # self._type is either TimestampType or TimestamptzType
            return target == TimestampType or target == TimestamptzType

    def result_type(self, source_type: Type) -> Type:
        return self._result_type

    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        if self == other:
            return True

        if isinstance(other, TimeTransform):
            return (
                TimeTransform._TIME_SATISFIED_ORDER[self._name]
                <= TimeTransform._TIME_SATISFIED_ORDER[other._name]
            )

        return False

    def dedup_name(self) -> str:
        return "time"


class Identity(Transform):
    def __init__(
        self,
        source_type: Type,
        human_str: Callable[[Any], str],
    ):
        super().__init__(
            "identity",
            f"transforms.identity(source_type={repr(source_type)})",
            human_str,
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


class Truncate(Transform):
    _VALID_TYPES = {IntegerType, LongType, StringType, BinaryType}
    _TO_HUMAN_STR = {BinaryType: transform_util.base64encode}

    def __init__(self, source_type: Type, width: int):
        if source_type not in Truncate._VALID_TYPES and not isinstance(
            source_type, DecimalType
        ):
            raise ValueError(f"Cannot truncate type: {source_type}")

        super().__init__(
            f"truncate[{width}]",
            f"transforms.truncate(source_type={repr(source_type)}, width={width})",
            Truncate._TO_HUMAN_STR.get(source_type, str),
        )
        self._type = source_type
        self._width = width

        if self._type == IntegerType or self._type == LongType:
            self._apply = lambda v, w: v - v % w
        elif self._type == StringType or self._type == BinaryType:
            self._apply = lambda v, w: v[0 : min(w, len(v))]
        else:  # decimal case
            self._apply = transform_util.truncate_decimal

    @property
    def width(self):
        return self._width

    def apply(self, value):
        if value is None:
            return None
        return self._apply(value, self._width)

    def can_transform(self, target: Type) -> bool:
        return self._type == target

    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        if self == other:
            return True
        elif (
            StringType == self._type
            and isinstance(other, Truncate)
            and StringType == other._type
        ):
            return self._width >= other._width

        return False


class UnknownTransform(Transform):
    def __init__(self, source_type: Type, transform: str):
        super().__init__(
            transform,
            f"UnknownTransform(source_type={repr(source_type)}, transform={repr(transform)})",
        )
        self._type = source_type
        self._transform = transform

    def apply(self, value):
        raise AttributeError(f"Cannot apply unsupported transform: {self.__str__()}")

    def can_transform(self, target: Type) -> bool:
        return repr(self._type) == repr(target)

    def result_type(self, source: Type) -> Type:
        return StringType


class VoidTransform(Transform):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VoidTransform, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        super().__init__("void", "transforms.always_null()", lambda v: "null")

    def apply(self, value):
        return None

    def can_transform(self, target: Type) -> bool:
        return True


_HAS_WIDTH = re.compile("(\\w+)\\[(\\d+)\\]")
_TIME_TRANSFORMS = {
    DateType: {
        "year": TimeTransform(DateType, "year", transform_util.years_for_days),
        "month": TimeTransform(DateType, "month", transform_util.months_for_days),
        "day": TimeTransform(DateType, "day", lambda d: d),
    },
    TimestampType: {
        "year": TimeTransform(TimestampType, "year", transform_util.years_for_ts),
        "month": TimeTransform(TimestampType, "month", transform_util.months_for_ts),
        "day": TimeTransform(TimestampType, "day", transform_util.days_for_ts),
        "hour": TimeTransform(TimestampType, "hour", transform_util.hours_for_ts),
    },
    TimestamptzType: {
        "year": TimeTransform(TimestamptzType, "year", transform_util.years_for_ts),
        "month": TimeTransform(TimestamptzType, "month", transform_util.months_for_ts),
        "day": TimeTransform(TimestamptzType, "day", transform_util.days_for_ts),
        "hour": TimeTransform(TimestamptzType, "hour", transform_util.hours_for_ts),
    },
}

_SPECIAL_IDENTITY_TRANSFORMS = {
    DateType: Identity(DateType, transform_util.human_day),
    TimeType: Identity(TimeType, transform_util.human_time),
    TimestampType: Identity(TimestampType, transform_util.human_timestamp),
    TimestamptzType: Identity(TimestamptzType, transform_util.human_timestamptz),
    BinaryType: Identity(BinaryType, transform_util.base64encode),
}


def from_string(source_type: Type, transform: str) -> Transform:
    transform_lower = transform.lower()
    match = _HAS_WIDTH.match(transform_lower)

    if match is not None:
        name = match.group(1)
        w = int(match.group(2))
        if name == "truncate":
            return Truncate(source_type, w)
        elif name == "bucket":
            return Bucket(source_type, w)

    if transform_lower == "identity":
        return identity(source_type)

    try:
        return _TIME_TRANSFORMS[source_type][transform_lower]
    except KeyError:
        pass  # fall through to return unknown transform

    if transform_lower == "void":
        return VoidTransform()

    return UnknownTransform(source_type, transform)


def identity(source_type: Type) -> Identity:
    if isinstance(source_type, FixedType):
        return Identity(source_type, transform_util.base64encode)
    else:
        return _SPECIAL_IDENTITY_TRANSFORMS.get(source_type, Identity(source_type, str))


def year(source_type: Type) -> Transform:
    try:
        return _TIME_TRANSFORMS[source_type]["year"]
    except KeyError:
        raise ValueError(f"Cannot partition type {source_type} by year")


def month(source_type: Type) -> Transform:
    try:
        return _TIME_TRANSFORMS[source_type]["month"]
    except KeyError:
        raise ValueError(f"Cannot partition type {source_type} by month")


def day(source_type: Type) -> Transform:
    try:
        return _TIME_TRANSFORMS[source_type]["day"]
    except KeyError:
        raise ValueError(f"Cannot partition type {source_type} by day")


def hour(source_type: Type) -> Transform:
    try:
        return _TIME_TRANSFORMS[source_type]["hour"]
    except KeyError:
        raise ValueError(f"Cannot partition type {source_type} by hour")


def bucket(source_type: Type, num_buckets: int) -> Bucket:
    return Bucket(source_type, num_buckets)


def truncate(source_type: Type, width: int) -> Truncate:
    return Truncate(source_type, width)


def always_null() -> Transform:
    return VoidTransform()

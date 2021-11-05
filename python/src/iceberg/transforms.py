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

import base64
import math
import re
import struct
from datetime import datetime, timedelta
from decimal import Decimal

import mmh3  # type: ignore
import pytz

from .types import (
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


class Transform(object):
    """
    Transform base class for concrete transforms. The default implementation is for VoidTransform.
    """

    _EPOCH = datetime.utcfromtimestamp(0)

    @staticmethod
    def _human_day(day_ordinal):
        time = Transform._EPOCH + timedelta(days=day_ordinal)
        return "{0:0=4d}-{1:0=2d}-{2:0=2d}".format(time.year, time.month, time.day)

    @staticmethod
    def _unscale_decimal(decimal_value: Decimal):
        value_tuple = decimal_value.as_tuple()
        return int(
            ("-" if value_tuple.sign else "")
            + "".join([str(d) for d in value_tuple.digits])
        )

    def __init__(self, transform_string: str, repr_string: str):
        self._transform_string = transform_string
        self._repr_string = repr_string

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._transform_string

    def apply(self, value):
        return None

    def can_transform(self, target: Type) -> bool:
        return True

    def get_result_type(self, source: Type) -> Type:
        return source

    def preserve_order(self) -> bool:
        return False

    def satisfy_order(self, other) -> bool:
        return self == other

    def to_human_string(self, value) -> str:
        return "null"

    def dedup_name(self) -> str:
        return self.__str__()


class Bucket(Transform):
    _MURMUR3 = mmh3
    _MAX_32_BITS_INT = 2147483647
    _FUNCTIONS_MAP = {  # [0] is hash function and [1] is can_transform check function
        DateType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("q", v)),
            lambda t: t in [IntegerType, DateType],
        ),
        IntegerType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("q", v)),
            lambda t: t in [IntegerType, DateType],
        ),
        TimeType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("q", v)),
            lambda t: t in {LongType, TimeType, TimestampType, TimestamptzType},
        ),
        TimestampType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("q", v)),
            lambda t: t in {LongType, TimeType, TimestampType, TimestamptzType},
        ),
        TimestamptzType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("q", v)),
            lambda t: t in {LongType, TimeType, TimestampType, TimestamptzType},
        ),
        LongType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("q", v)),
            lambda t: t in [LongType, TimeType, TimestampType, TimestamptzType],
        ),
        StringType: (
            lambda v: Bucket._MURMUR3.hash(v),
            lambda t: t == StringType,
        ),
        BinaryType: (
            lambda v: Bucket._MURMUR3.hash(v),
            lambda t: t == BinaryType or isinstance(t, FixedType),
        ),
        UUIDType: (
            lambda v: Bucket._MURMUR3.hash(
                struct.pack(
                    ">QQ",
                    (v.int >> 64) & 0xFFFFFFFFFFFFFFFF,
                    v.int & 0xFFFFFFFFFFFFFFFF,
                )
            ),
            lambda t: t == UUIDType,
        ),
        # bucketing by Float/Double is not allowed by the spec, but they have hash implementation
        FloatType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("d", v)),
            lambda t: t == FloatType,
        ),
        DoubleType: (
            lambda v: Bucket._MURMUR3.hash(struct.pack("d", v)),
            lambda t: t == DoubleType,
        ),
    }

    @staticmethod
    def _decimal_to_bytes(value: Decimal):
        unscaled_value = Transform._unscale_decimal(value)
        number_of_bytes = int(math.ceil(unscaled_value.bit_length() / 8))
        return unscaled_value.to_bytes(length=number_of_bytes, byteorder="big")

    def __init__(self, transform_type: Type, num_buckets: int):
        if (
            transform_type not in Bucket._FUNCTIONS_MAP
            and not isinstance(transform_type, FixedType)
            and not isinstance(transform_type, DecimalType)
        ):
            raise ValueError(f"Cannot bucket by type: {transform_type}")

        super().__init__(
            f"bucket[{num_buckets}",
            f"transforms.bucket(transform_type={repr(transform_type)}, num_buckets={num_buckets})",
        )
        self._type = transform_type
        self._num_buckets = num_buckets

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    def apply(self, value):
        if value is None:
            return None

        if isinstance(self._type, FixedType):
            return (
                Bucket._MURMUR3.hash(value) & Bucket._MAX_32_BITS_INT
            ) % self._num_buckets
        elif isinstance(self._type, DecimalType):
            return (
                Bucket._MURMUR3.hash(Bucket._decimal_to_bytes(value))
                & Bucket._MAX_32_BITS_INT
            ) % self._num_buckets

        return (
            Bucket._FUNCTIONS_MAP[self._type][0](value) & Bucket._MAX_32_BITS_INT
        ) % self._num_buckets

    def can_transform(self, target: Type) -> bool:
        if isinstance(self._type, FixedType):
            return target == BinaryType or isinstance(target, FixedType)
        elif isinstance(self._type, DecimalType):
            return isinstance(target, DecimalType)

        return Bucket._FUNCTIONS_MAP[self._type][1](target)

    def get_result_type(self, source: Type):
        return IntegerType

    def to_human_string(self, value):
        return str(value)


class Time(Transform):
    """
    Time class is for both Date transforms and Timestamp transforms.
    """

    _TIME_ORDER = datetime(year=3, month=2, day=1, hour=0)
    _VALID_TIME_GRANULARITY = {
        DateType: {"year", "month", "day"},
        TimestampType: {"year", "month", "day", "hour"},
        TimestamptzType: {"year", "month", "day", "hour"},
    }
    _DIFF_MAP = {
        "year": lambda t1, t2: (t1.year - t2.year)
        - (
            1
            if t1.month < t2.month or (t1.month == t2.month and t1.day < t2.day)
            else 0
        ),
        "month": lambda t1, t2: (t1.year - t2.year) * 12
        + (t1.month - t2.month)
        - (1 if t1.day < t2.day else 0),
        "day": lambda t1, t2: (t1 - t2).days,
        "hour": lambda t1, t2: int((t1 - t2).total_seconds() / 3600),
    }

    def __init__(self, transform_type: Type, name: str):
        if name not in Time._VALID_TIME_GRANULARITY.get(transform_type, {}):
            raise ValueError(f"Cannot transform type: {transform_type} by {name}")

        super().__init__(
            name, f"transforms.{name}(transform_type={repr(transform_type)})"
        )
        self._type = transform_type
        self._name = name

    def apply(self, value: int) -> int:
        if self._name == "day" and self._type == DateType:
            return value

        if self._type == DateType:
            value_time = datetime.utcfromtimestamp(value * 86400)
        else:
            value_time = datetime.utcfromtimestamp(value / 1000000)

        return Time._DIFF_MAP[self._name](value_time, Transform._EPOCH)

    def can_transform(self, target: Type) -> bool:
        if self._type == DateType:
            return target == DateType
        else:  # self._type is either TimestampType or TimestamptzType
            return target == TimestampType or target == TimestamptzType

    def get_result_type(self, source_type: Type) -> Type:
        if self._name == "day":
            return DateType

        return IntegerType

    def preserve_order(self) -> bool:
        return True

    def satisfy_order(self, other: Transform) -> bool:
        if self == other:
            return True

        if isinstance(other, Time):
            return getattr(Time._TIME_ORDER, self._name) <= getattr(
                Time._TIME_ORDER, other._name
            )

        return False

    def to_human_string(self, value: int) -> str:
        if value is None:
            return "null"

        if self._name == "year":
            return "{0:0=4d}".format(Transform._EPOCH.year + value)
        elif self._name == "month":
            return "{0:0=4d}-{1:0=2d}".format(
                Transform._EPOCH.year + int(value / 12), 1 + int(value % 12)
            )
        elif self._name == "day":
            return Transform._human_day(value)
        else:  # self._name == "hour":
            dt = Transform._EPOCH + timedelta(hours=value)
            return "{0:0=4d}-{1:0=2d}-{2:0=2d}-{3:0=2d}".format(
                dt.year, dt.month, dt.day, dt.hour
            )

    def dedup_name(self) -> str:
        return "time"


class Identity(Transform):
    _HUMAN_STRING_MAP = {
        DateType: lambda v: Transform._human_day(v),
        TimeType: lambda v: f"{(Transform._EPOCH + timedelta(microseconds=v)).time()}",
        TimestampType: lambda v: (
            Transform._EPOCH + timedelta(microseconds=v)
        ).isoformat(),
        TimestamptzType: lambda v: pytz.timezone("UTC")
        .localize(Transform._EPOCH + timedelta(microseconds=v))
        .strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        BinaryType: lambda v: base64.b64encode(v).decode("ISO-8859-1"),
    }

    def __init__(self, transform_type: Type):
        super().__init__(
            "identity", f"transforms.identity(transform_type={repr(transform_type)})"
        )
        self._type = transform_type

    def apply(self, value):
        return value

    def can_transform(self, target: Type) -> bool:
        return target.is_primitive

    def preserve_order(self) -> bool:
        return True

    def satisfy_order(self, other: Transform) -> bool:
        return other.preserve_order()

    def to_human_string(self, value) -> str:
        if value is None:
            return "null"

        if isinstance(self._type, FixedType):
            return base64.b64encode(value).decode("ISO-8859-1")

        return Identity._HUMAN_STRING_MAP.get(self._type, str)(value)


class Truncate(Transform):
    _VALID_TYPES = {IntegerType, LongType, StringType, BinaryType}

    def __init__(self, transform_type: Type, width: int):
        if transform_type not in Truncate._VALID_TYPES and not isinstance(
            transform_type, DecimalType
        ):
            raise ValueError(f"Cannot truncate type: {transform_type}")

        super().__init__(
            f"truncate[{width}]",
            f"transforms.truncate(transform_type={repr(transform_type)}, width={width})",
        )
        self._type = transform_type
        self._width = width

    @property
    def width(self):
        return self._width

    def apply(self, value):
        if value is None:
            return None
        if self._type == IntegerType or self._type == LongType:
            return value - (((value % self._width) + self._width) % self._width)
        elif self._type == StringType or self._type == BinaryType:
            return value[0 : min(self._width, len(value))]
        else:  # decimal case
            unscaled_value = Transform._unscale_decimal(value)
            applied_value = unscaled_value - (
                ((unscaled_value % self._width) + self._width) % self._width
            )
            return Decimal(f"{applied_value}e{value.as_tuple().exponent}")

    def can_transform(self, target: Type) -> bool:
        return self._type == target

    def preserve_order(self) -> bool:
        return True

    def satisfy_order(self, other: Transform) -> bool:
        if self == other:
            return True
        elif (
            StringType == self._type
            and isinstance(other, Truncate)
            and StringType == other._type
        ):
            return self._width >= other._width

        return False

    def to_human_string(self, value) -> str:
        if value is None:
            return "null"

        return (
            (base64.b64encode(value)).decode("ISO-8859-1")
            if self._type == BinaryType
            else str(value)
        )


class UnknownTransform(Transform):
    def __init__(self, transform_type: Type, transform: str):
        super().__init__(
            transform,
            f"UnknownTransform(transform_type={repr(transform_type)}, transform='{transform}')",
        )
        self._type = transform_type
        self._transform = transform

    def apply(self, value):
        raise AttributeError(f"Cannot apply unsupported transform: {self.__str__()}")

    def can_transform(self, target: Type) -> bool:
        return repr(self._type) == repr(target)

    def get_result_type(self, source: Type) -> Type:
        return StringType


VoidTransform = Transform("void", "transforms.always_null()")

_HAS_WIDTH = re.compile("(\\w+)\\[(\\d+)\\]")
_TIME_TRANSFORMS = {
    DateType: {
        "year": Time(DateType, "year"),
        "month": Time(DateType, "month"),
        "day": Time(DateType, "day"),
    },
    TimestampType: {
        "year": Time(TimestampType, "year"),
        "month": Time(TimestampType, "month"),
        "day": Time(TimestampType, "day"),
        "hour": Time(TimestampType, "hour"),
    },
    TimestamptzType: {
        "year": Time(TimestamptzType, "year"),
        "month": Time(TimestamptzType, "month"),
        "day": Time(TimestamptzType, "day"),
        "hour": Time(TimestamptzType, "hour"),
    },
}


def from_string(transform_type: Type, transform: str) -> Transform:
    match = _HAS_WIDTH.match(transform)

    if match is not None:
        name = match.group(1)
        w = int(match.group(2))
        if name.lower() == "truncate":
            return Truncate(transform_type, w)
        elif name.lower() == "bucket":
            return Bucket(transform_type, w)

    if transform.lower() == "identity":
        return Identity(transform_type)

    try:
        return _TIME_TRANSFORMS[transform_type][transform.lower()]
    except KeyError:
        pass  # fall through to return unknown transform

    if transform.lower() == "void":
        return VoidTransform

    return UnknownTransform(transform_type, transform)


def identity(transform_type: Type) -> Identity:
    return Identity(transform_type)


def year(transform_type: Type) -> Time:
    try:
        return _TIME_TRANSFORMS[transform_type]["year"]
    except KeyError:
        raise ValueError(f"Cannot partition type {transform_type} by year")


def month(transform_type: Type) -> Time:
    try:
        return _TIME_TRANSFORMS[transform_type]["month"]
    except KeyError:
        raise ValueError(f"Cannot partition type {transform_type} by month")


def day(transform_type: Type) -> Time:
    try:
        return _TIME_TRANSFORMS[transform_type]["day"]
    except KeyError:
        raise ValueError(f"Cannot partition type {transform_type} by day")


def hour(transform_type: Type) -> Time:
    try:
        return _TIME_TRANSFORMS[transform_type]["hour"]
    except KeyError:
        raise ValueError(f"Cannot partition type {transform_type} by hour")


def bucket(transform_type: Type, num_buckets: int) -> Bucket:
    return Bucket(transform_type, num_buckets)


def truncate(transform_type: Type, width: int) -> Truncate:
    return Truncate(transform_type, width)


def always_null() -> Transform:
    return VoidTransform

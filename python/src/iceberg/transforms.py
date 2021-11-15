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
from datetime import datetime
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
    """
    Transform base class for concrete transforms. The default implementation is for VoidTransform.
    """

    def __init__(
        self,
        transform_string: str,
        repr_string: str,
        to_human_str: Callable[[Any], str] = transform_util.to_string,
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
    _FUNCTIONS_MAP = {  # [0] is hash function and [1] is can_transform check function
        DateType: (
            lambda v: mmh3.hash(struct.pack("q", v)),
            lambda t: t in [IntegerType, DateType],
        ),
        IntegerType: (
            lambda v: mmh3.hash(struct.pack("q", v)),
            lambda t: t in [IntegerType, DateType],
        ),
        TimeType: (
            lambda v: mmh3.hash(struct.pack("q", v)),
            lambda t: t in {LongType, TimeType, TimestampType, TimestamptzType},
        ),
        TimestampType: (
            lambda v: mmh3.hash(struct.pack("q", v)),
            lambda t: t in {LongType, TimeType, TimestampType, TimestamptzType},
        ),
        TimestamptzType: (
            lambda v: mmh3.hash(struct.pack("q", v)),
            lambda t: t in {LongType, TimeType, TimestampType, TimestamptzType},
        ),
        LongType: (
            lambda v: mmh3.hash(struct.pack("q", v)),
            lambda t: t in [LongType, TimeType, TimestampType, TimestamptzType],
        ),
        StringType: (
            lambda v: mmh3.hash(v),
            lambda t: t == StringType,
        ),
        BinaryType: (
            lambda v: mmh3.hash(v),
            lambda t: t == BinaryType,
        ),
        UUIDType: (
            lambda v: mmh3.hash(
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
            lambda v: mmh3.hash(struct.pack("d", v)),
            lambda t: t == FloatType,
        ),
        DoubleType: (
            lambda v: mmh3.hash(struct.pack("d", v)),
            lambda t: t == DoubleType,
        ),
    }

    def __init__(self, source_type: Type, num_buckets: int):
        if (
            source_type not in Bucket._FUNCTIONS_MAP
            and not isinstance(source_type, FixedType)
            and not isinstance(source_type, DecimalType)
        ):
            raise ValueError(f"Cannot bucket by type: {source_type}")

        super().__init__(
            f"bucket[{num_buckets}]",
            f"transforms.bucket(source_type={repr(source_type)}, num_buckets={num_buckets})",
        )
        self._type = source_type
        self._num_buckets = num_buckets

        if isinstance(self._type, FixedType):
            self._hash_func = lambda v: mmh3.hash(v)
            self._can_transform = lambda t: isinstance(t, FixedType)
        elif isinstance(self._type, DecimalType):
            self._hash_func = lambda v: mmh3.hash(transform_util.decimal_to_bytes(v))
            self._can_transform = lambda t: isinstance(t, DecimalType)
        else:
            self._hash_func = Bucket._FUNCTIONS_MAP[self._type][0]
            self._can_transform = Bucket._FUNCTIONS_MAP[self._type][1]

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


class Time(Transform):
    """
    Time class is for both Date transforms and Timestamp transforms.
    """

    _TIME_SATISFIED_ORDER = dict(year=3, month=2, day=1, hour=0)
    _VALID_TIME_GRANULARITY = {
        DateType: {"year", "month", "day"},
        TimestampType: {"year", "month", "day", "hour"},
        TimestamptzType: {"year", "month", "day", "hour"},
    }
    _INSTANCES: dict = {DateType: {}, TimestampType: {}, TimestamptzType: {}}

    def __new__(cls, source_type: Type, name: str):
        if cls._INSTANCES.get(source_type, {}).get(name) is None:
            if name not in Time._VALID_TIME_GRANULARITY.get(source_type, {}):
                raise ValueError(f"Cannot partition type: {source_type} by {name}")
            cls._INSTANCES[source_type][name] = super(Time, cls).__new__(cls)
        return cls._INSTANCES[source_type][name]

    def __init__(self, source_type: Type, name: str):
        super().__init__(
            name,
            f"transforms.{name}(source_type={repr(source_type)})",
            getattr(transform_util, f"human_{name}"),
        )
        self._type = source_type
        self._name = name

        self._diff_func = getattr(transform_util, f"diff_{self._name}")
        if self._name == "day" and self._type == DateType:
            self._apply = lambda v: v
        elif self._type == DateType:
            self._apply = lambda v: self._diff_func(
                datetime.utcfromtimestamp(v * 86400)
            )
        else:
            self._apply = lambda v: self._diff_func(
                datetime.utcfromtimestamp(v / 1000000)
            )

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

        if isinstance(other, Time):
            return (
                Time._TIME_SATISFIED_ORDER[self._name]
                <= Time._TIME_SATISFIED_ORDER[other._name]
            )

        return False

    def dedup_name(self) -> str:
        return "time"


class Identity(Transform):
    _HUMAN_STRING_MAP = {
        DateType: lambda v: transform_util.human_day(v),
        TimeType: lambda v: transform_util.human_time(v),
        TimestampType: lambda v: transform_util.human_timestamp(v),
        TimestamptzType: lambda v: transform_util.human_timestamptz(v),
        BinaryType: lambda v: transform_util.base64encode(v),
    }

    def __init__(self, source_type: Type):
        if isinstance(source_type, FixedType):
            to_human_string = transform_util.base64encode
        else:
            to_human_string = Identity._HUMAN_STRING_MAP.get(
                source_type, transform_util.to_string
            )
        super().__init__(
            "identity",
            f"transforms.identity(source_type={repr(source_type)})",
            to_human_string,
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

    def __init__(self, source_type: Type, width: int):
        if source_type not in Truncate._VALID_TYPES and not isinstance(
            source_type, DecimalType
        ):
            raise ValueError(f"Cannot truncate type: {source_type}")

        super().__init__(
            f"truncate[{width}]",
            f"transforms.truncate(source_type={repr(source_type)}, width={width})",
            transform_util.base64encode
            if source_type == BinaryType
            else transform_util.to_string,
        )
        self._type = source_type
        self._width = width

        if self._type == IntegerType or self._type == LongType:
            self._apply = lambda v, w: v - (((v % w) + w) % w)
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
            f"UnknownTransform(source_type={repr(source_type)}, transform='{transform}')",
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
        return Identity(source_type)

    try:
        return Time(source_type, transform_lower)
    except ValueError:
        pass  # fall through to return unknown transform

    if transform_lower == "void":
        return VoidTransform()

    return UnknownTransform(source_type, transform)


def identity(source_type: Type) -> Identity:
    return Identity(source_type)


def year(source_type: Type) -> Time:
    return Time(source_type, "year")


def month(source_type: Type) -> Time:
    return Time(source_type, "month")


def day(source_type: Type) -> Time:
    return Time(source_type, "day")


def hour(source_type: Type) -> Time:
    return Time(source_type, "hour")


def bucket(source_type: Type, num_buckets: int) -> Bucket:
    return Bucket(source_type, num_buckets)


def truncate(source_type: Type, width: int) -> Truncate:
    return Truncate(source_type, width)


def always_null() -> Transform:
    return VoidTransform()

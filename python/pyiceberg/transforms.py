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
import struct
from abc import ABC, abstractmethod
from decimal import Decimal
from functools import singledispatch
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
)
from uuid import UUID

import mmh3  # type: ignore

from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    FixedType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils import datetime
from pyiceberg.utils.decimal import decimal_to_bytes, truncate_decimal
from pyiceberg.utils.singleton import Singleton

S = TypeVar("S")
T = TypeVar("T")


class Transform(ABC, Generic[S, T]):
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

    def __call__(self, value: Optional[S]) -> Optional[T]:
        return self.apply(value)

    @abstractmethod
    def apply(self, value: Optional[S]) -> Optional[T]:
        ...

    @abstractmethod
    def can_transform(self, source: IcebergType) -> bool:
        return False

    @abstractmethod
    def result_type(self, source: IcebergType) -> IcebergType:
        ...

    @property
    def preserves_order(self) -> bool:
        return False

    def satisfies_order_of(self, other) -> bool:
        return self == other

    def to_human_string(self, value: Optional[S]) -> str:
        return str(value) if value is not None else "null"

    @property
    def dedup_name(self) -> str:
        return self._transform_string


class BaseBucketTransform(Transform[S, int]):
    """Base Transform class to transform a value into a bucket partition value

    Transforms are parameterized by a number of buckets. Bucket partition transforms use a 32-bit
    hash of the source value to produce a positive value by mod the bucket number.

    Args:
      source_type (Type): An Iceberg Type of IntegerType, LongType, DecimalType, DateType, TimeType,
      TimestampType, TimestamptzType, StringType, BinaryType, FixedType, UUIDType.
      num_buckets (int): The number of buckets.
    """

    def __init__(self, source_type: IcebergType, num_buckets: int):
        super().__init__(
            f"bucket[{num_buckets}]",
            f"transforms.bucket(source_type={repr(source_type)}, num_buckets={num_buckets})",
        )
        self._num_buckets = num_buckets

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    def hash(self, value: S) -> int:
        raise NotImplementedError()

    def apply(self, value: Optional[S]) -> Optional[int]:
        return (self.hash(value) & IntegerType.max) % self._num_buckets if value else None

    def result_type(self, source: IcebergType) -> IcebergType:
        return IntegerType()

    @abstractmethod
    def can_transform(self, source: IcebergType) -> bool:
        pass


class BucketNumberTransform(BaseBucketTransform):
    """Transforms a value of IntegerType, LongType, DateType, TimeType, TimestampType, or TimestamptzType
    into a bucket partition value

    Example:
        >>> transform = BucketNumberTransform(LongType(), 100)
        >>> transform.apply(81068000000)
        59
    """

    def can_transform(self, source: IcebergType) -> bool:
        return type(source) in {IntegerType, DateType, LongType, TimeType, TimestampType, TimestamptzType}

    def hash(self, value) -> int:
        return mmh3.hash(struct.pack("<q", value))


class BucketDecimalTransform(BaseBucketTransform):
    """Transforms a value of DecimalType into a bucket partition value.

    Example:
        >>> transform = BucketDecimalTransform(DecimalType(9, 2), 100)
        >>> transform.apply(Decimal("14.20"))
        59
    """

    def can_transform(self, source: IcebergType) -> bool:
        return isinstance(source, DecimalType)

    def hash(self, value: Decimal) -> int:
        return mmh3.hash(decimal_to_bytes(value))


class BucketStringTransform(BaseBucketTransform):
    """Transforms a value of StringType into a bucket partition value.

    Example:
        >>> transform = BucketStringTransform(100)
        >>> transform.apply("iceberg")
        89
    """

    def __init__(self, num_buckets: int):
        super().__init__(StringType(), num_buckets)

    def can_transform(self, source: IcebergType) -> bool:
        return isinstance(source, StringType)

    def hash(self, value: str) -> int:
        return mmh3.hash(value)


class BucketBytesTransform(BaseBucketTransform):
    """Transforms a value of FixedType or BinaryType into a bucket partition value.

    Example:
        >>> transform = BucketBytesTransform(BinaryType(), 100)
        >>> transform.apply(b"\\x00\\x01\\x02\\x03")
        41
    """

    def can_transform(self, source: IcebergType) -> bool:
        return type(source) in {FixedType, BinaryType}

    def hash(self, value: bytes) -> int:
        return mmh3.hash(value)


class BucketUUIDTransform(BaseBucketTransform):
    """Transforms a value of UUIDType into a bucket partition value.

    Example:
        >>> transform = BucketUUIDTransform(100)
        >>> transform.apply(UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"))
        40
    """

    def __init__(self, num_buckets: int):
        super().__init__(UUIDType(), num_buckets)

    def can_transform(self, source: IcebergType) -> bool:
        return isinstance(source, UUIDType)

    def hash(self, value: UUID) -> int:
        return mmh3.hash(
            struct.pack(
                ">QQ",
                (value.int >> 64) & 0xFFFFFFFFFFFFFFFF,
                value.int & 0xFFFFFFFFFFFFFFFF,
            )
        )


def _base64encode(buffer: bytes) -> str:
    """Converts bytes to base64 string"""
    return base64.b64encode(buffer).decode("ISO-8859-1")


class IdentityTransform(Transform[S, S]):
    """Transforms a value into itself.

    Example:
        >>> transform = IdentityTransform(StringType())
        >>> transform.apply('hello-world')
        'hello-world'
    """

    def __init__(self, source_type: IcebergType):
        super().__init__(
            "identity",
            f"transforms.identity(source_type={repr(source_type)})",
        )
        self._type = source_type

    def apply(self, value: Optional[S]) -> Optional[S]:
        return value

    def can_transform(self, source: IcebergType) -> bool:
        return source.is_primitive

    def result_type(self, source: IcebergType) -> IcebergType:
        return source

    @property
    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        """ordering by value is the same as long as the other preserves order"""
        return other.preserves_order

    def to_human_string(self, value: Optional[S]) -> str:
        return _human_string(value, self._type) if value is not None else "null"


class TruncateTransform(Transform[S, S]):
    """A transform for truncating a value to a specified width.
    Args:
      source_type (Type): An Iceberg Type of IntegerType, LongType, StringType, BinaryType or DecimalType
      width (int): The truncate width
    Raises:
      ValueError: If a type is provided that is incompatible with a Truncate transform
    """

    def __init__(self, source_type: IcebergType, width: int):
        assert width > 0, f"width ({width}) should be greater than 0"
        super().__init__(
            f"truncate[{width}]",
            f"transforms.truncate(source_type={repr(source_type)}, width={width})",
        )
        self._type = source_type
        self._width = width

    @property
    def width(self) -> int:
        return self._width

    @property
    def type(self) -> IcebergType:
        return self._type

    def apply(self, value: Optional[S]) -> Optional[S]:
        return _truncate_value(value, self._width) if value is not None else None

    def can_transform(self, source: IcebergType) -> bool:
        return self._type == source

    def result_type(self, source: IcebergType) -> IcebergType:
        return source

    @property
    def preserves_order(self) -> bool:
        return True

    def satisfies_order_of(self, other: Transform) -> bool:
        if self == other:
            return True
        elif isinstance(self._type, StringType) and isinstance(other, TruncateTransform) and isinstance(other.type, StringType):
            return self._width >= other.width

        return False

    def to_human_string(self, value: Optional[S]) -> str:
        if value is None:
            return "null"
        elif isinstance(value, bytes):
            return _base64encode(value)
        else:
            return str(value)


@singledispatch
def _human_string(value: Any, _type: IcebergType) -> str:
    return str(value)


@_human_string.register(bytes)
def _(value: bytes, _type: IcebergType) -> str:
    return _base64encode(value)


@_human_string.register(int)
def _(value: int, _type: IcebergType) -> str:
    return _int_to_human_string(_type, value)


@singledispatch
def _int_to_human_string(_type: IcebergType, value: int) -> str:
    return str(value)


@_int_to_human_string.register(DateType)
def _(_type: IcebergType, value: int) -> str:
    return datetime.to_human_day(value)


@_int_to_human_string.register(TimeType)
def _(_type: IcebergType, value: int) -> str:
    return datetime.to_human_time(value)


@_int_to_human_string.register(TimestampType)
def _(_type: IcebergType, value: int) -> str:
    return datetime.to_human_timestamp(value)


@_int_to_human_string.register(TimestamptzType)
def _(_type: IcebergType, value: int) -> str:
    return datetime.to_human_timestamptz(value)


@singledispatch
def _truncate_value(value: Any, _width: int) -> S:
    raise ValueError(f"Cannot truncate value: {value}")


@_truncate_value.register(int)
def _(value: int, _width: int) -> int:
    """Truncate a given int value into a given width if feasible."""
    return value - value % _width


@_truncate_value.register(str)
def _(value: str, _width: int) -> str:
    """Truncate a given string to a given width."""
    return value[0 : min(_width, len(value))]


@_truncate_value.register(bytes)
def _(value: bytes, _width: int) -> bytes:
    """Truncate a given binary bytes into a given width."""
    return value[0 : min(_width, len(value))]


@_truncate_value.register(Decimal)
def _(value: Decimal, _width: int) -> Decimal:
    """Truncate a given decimal value into a given width."""
    return truncate_decimal(value, _width)


class UnknownTransform(Transform):
    """A transform that represents when an unknown transform is provided
    Args:
      source_type (Type): An Iceberg `Type`
      transform (str): A string name of a transform
    Raises:
      AttributeError: If the apply method is called.
    """

    def __init__(self, source_type: IcebergType, transform: str):
        super().__init__(
            transform,
            f"transforms.UnknownTransform(source_type={repr(source_type)}, transform={repr(transform)})",
        )
        self._type = source_type
        self._transform = transform

    def apply(self, value: Optional[S]):
        raise AttributeError(f"Cannot apply unsupported transform: {self}")

    def can_transform(self, source: IcebergType) -> bool:
        return self._type == source

    def result_type(self, source: IcebergType) -> IcebergType:
        return StringType()


class VoidTransform(Transform, Singleton):
    """A transform that always returns None"""

    def __init__(self):
        super().__init__("void", "transforms.always_null()")

    def apply(self, value: Optional[S]) -> None:
        return None

    def can_transform(self, _: IcebergType) -> bool:
        return True

    def result_type(self, source: IcebergType) -> IcebergType:
        return source

    def to_human_string(self, value: Optional[S]) -> str:
        return "null"


def bucket(source_type: IcebergType, num_buckets: int) -> BaseBucketTransform:
    if type(source_type) in {IntegerType, LongType, DateType, TimeType, TimestampType, TimestamptzType}:
        return BucketNumberTransform(source_type, num_buckets)
    elif isinstance(source_type, DecimalType):
        return BucketDecimalTransform(source_type, num_buckets)
    elif isinstance(source_type, StringType):
        return BucketStringTransform(num_buckets)
    elif isinstance(source_type, BinaryType):
        return BucketBytesTransform(source_type, num_buckets)
    elif isinstance(source_type, FixedType):
        return BucketBytesTransform(source_type, num_buckets)
    elif isinstance(source_type, UUIDType):
        return BucketUUIDTransform(num_buckets)
    else:
        raise ValueError(f"Cannot bucket by type: {source_type}")


def identity(source_type: IcebergType) -> IdentityTransform:
    return IdentityTransform(source_type)


def truncate(source_type: IcebergType, width: int) -> TruncateTransform:
    return TruncateTransform(source_type, width)


def always_null() -> Transform:
    return VoidTransform()

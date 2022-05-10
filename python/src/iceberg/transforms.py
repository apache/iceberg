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

import struct
from abc import ABC
from decimal import Decimal
from typing import Generic, Optional, TypeVar
from uuid import UUID

import mmh3  # type: ignore

from iceberg.types import (
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
from iceberg.utils.decimal import decimal_to_bytes

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

    def apply(self, value: Optional[S]) -> Optional[T]:
        ...

    def can_transform(self, source: IcebergType) -> bool:
        return False

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

    def apply(self, value):
        raise AttributeError(f"Cannot apply unsupported transform: {self}")

    def can_transform(self, target: IcebergType) -> bool:
        return self._type == target

    def result_type(self, source: IcebergType) -> IcebergType:
        return StringType()


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

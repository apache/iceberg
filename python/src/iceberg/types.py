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
"""Data types used in describing Iceberg schemas

This module implements the data types described in the Iceberg specification for Iceberg schemas. To
describe an Iceberg table schema, these classes can be used in the construction of a StructType instance.

Example:
    >>> str(StructType(
    ...     NestedField(1, "required_field", StringType(), True),
    ...     NestedField(2, "optional_field", IntegerType())
    ... ))
    'struct<1: required_field: optional string, 2: optional_field: optional int>'

Notes:
  - https://iceberg.apache.org/#spec/#primitive-types
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import ClassVar

from iceberg.utils.singleton import Singleton


@dataclass(frozen=True)
class IcebergType(metaclass=Singleton):
    """Base type for all Iceberg Types

    Example:
        >>> str(IcebergType())
        'IcebergType()'
        >>> repr(IcebergType())
        'IcebergType()'
    """

    @property
    def string_type(self) -> str:
        return self.__repr__()

    def __str__(self) -> str:
        return self.string_type

    @property
    def is_primitive(self) -> bool:
        return isinstance(self, PrimitiveType)


@dataclass(frozen=True, eq=True)
class PrimitiveType(IcebergType):
    """Base class for all Iceberg Primitive Types

    Example:
        >>> str(PrimitiveType())
        'PrimitiveType()'
    """


@dataclass(frozen=True)
class FixedType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> FixedType(8)
        FixedType(length=8)
        >>> FixedType(8) == FixedType(8)
        True
    """

    length: int = field()

    @property
    def string_type(self) -> str:
        return f"fixed[{self.length}]"


@dataclass(frozen=True, eq=True)
class DecimalType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> DecimalType(32, 3)
        DecimalType(precision=32, scale=3)
        >>> DecimalType(8, 3) == DecimalType(8, 3)
        True
    """

    precision: int = field()
    scale: int = field()

    @property
    def string_type(self) -> str:
        return f"decimal({self.precision}, {self.scale})"


@dataclass(frozen=True)
class NestedField(IcebergType):
    """Represents a field of a struct, a map key, a map value, or a list element.

    This is where field IDs, names, docs, and nullability are tracked.

    Example:
        >>> str(NestedField(
        ...     field_id=1,
        ...     name='foo',
        ...     field_type=FixedType(22),
        ...     required=False,
        ... ))
        '1: foo: required fixed[22]'
        >>> str(NestedField(
        ...     field_id=2,
        ...     name='bar',
        ...     field_type=LongType(),
        ...     required=False,
        ...     doc="Just a long"
        ... ))
        '2: bar: required long (Just a long)'
    """

    field_id: int = field()
    name: str = field()
    field_type: IcebergType = field()
    required: bool = field(default=True)
    doc: str | None = field(default=None, repr=False)

    @property
    def optional(self) -> bool:
        return not self.required

    @property
    def string_type(self) -> str:
        doc = "" if not self.doc else f" ({self.doc})"
        req = "optional" if self.required else "required"
        return f"{self.field_id}: {self.name}: {req} {self.field_type}{doc}"


@dataclass(frozen=True, init=False)
class StructType(IcebergType):
    """A struct type in Iceberg

    Example:
        >>> str(StructType(
        ...     NestedField(1, "required_field", StringType(), True),
        ...     NestedField(2, "optional_field", IntegerType())
        ... ))
        'struct<1: required_field: optional string, 2: optional_field: optional int>'
    """

    fields: tuple[NestedField] = field()

    def __init__(self, *fields: NestedField, **kwargs):  # pylint: disable=super-init-not-called
        if not fields and "fields" in kwargs:
            fields = kwargs["fields"]
        object.__setattr__(self, "fields", fields)

    @cached_property
    def string_type(self) -> str:
        return f"struct<{', '.join(map(str, self.fields))}>"


@dataclass(frozen=True)
class ListType(IcebergType):
    """A list type in Iceberg

    Example:
        >>> ListType(element_id=3, element_type=StringType(), element_required=True)
        ListType(element_id=3, element_type=StringType(), element_required=True)
    """

    element_id: int = field()
    element_type: IcebergType = field()
    element_required: bool = field(default=True)
    element: NestedField = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(
            self,
            "element",
            NestedField(
                name="element",
                required=self.element_required,
                field_id=self.element_id,
                field_type=self.element_type,
            ),
        )

    @property
    def string_type(self) -> str:
        return f"list<{self.element_type}>"


@dataclass(frozen=True)
class MapType(IcebergType):
    """A map type in Iceberg

    Example:
        >>> MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=True)
        MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=True)
    """

    key_id: int = field()
    key_type: IcebergType = field()
    value_id: int = field()
    value_type: IcebergType = field()
    value_required: bool = field(default=True)
    key: NestedField = field(init=False, repr=False)
    value: NestedField = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "key", NestedField(name="key", field_id=self.key_id, field_type=self.key_type, required=False))
        object.__setattr__(
            self,
            "value",
            NestedField(
                name="value",
                field_id=self.value_id,
                field_type=self.value_type,
                required=self.value_required,
            ),
        )


@dataclass(frozen=True)
class BooleanType(PrimitiveType):
    """A boolean data type in Iceberg can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
        >>> column_foo
        BooleanType()
    """

    @property
    def string_type(self) -> str:
        return "boolean"


@dataclass(frozen=True)
class IntegerType(PrimitiveType):
    """An Integer data type in Iceberg can be represented using an instance of this class. Integers in Iceberg are
    32-bit signed and can be promoted to Longs.

    Example:
        >>> column_foo = IntegerType()
        >>> isinstance(column_foo, IntegerType)
        True

    Attributes:
        max (int): The maximum allowed value for Integers, inherited from the canonical Iceberg implementation
          in Java (returns `2147483647`)
        min (int): The minimum allowed value for Integers, inherited from the canonical Iceberg implementation
          in Java (returns `-2147483648`)
    """

    max: ClassVar[int] = 2147483647
    min: ClassVar[int] = -2147483648

    @property
    def string_type(self) -> str:
        return "int"


@dataclass(frozen=True)
class LongType(PrimitiveType):
    """A Long data type in Iceberg can be represented using an instance of this class. Longs in Iceberg are
    64-bit signed integers.

    Example:
        >>> column_foo = LongType()
        >>> isinstance(column_foo, LongType)
        True
        >>> column_foo
        LongType()
        >>> str(column_foo)
        'long'

    Attributes:
        max (int): The maximum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java. (returns `9223372036854775807`)
        min (int): The minimum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java (returns `-9223372036854775808`)
    """

    max: ClassVar[int] = 9223372036854775807
    min: ClassVar[int] = -9223372036854775808

    @property
    def string_type(self) -> str:
        return "long"


@dataclass(frozen=True)
class FloatType(PrimitiveType):
    """A Float data type in Iceberg can be represented using an instance of this class. Floats in Iceberg are
    32-bit IEEE 754 floating points and can be promoted to Doubles.

    Example:
        >>> column_foo = FloatType()
        >>> isinstance(column_foo, FloatType)
        True
        >>> column_foo
        FloatType()

    Attributes:
        max (float): The maximum allowed value for Floats, inherited from the canonical Iceberg implementation
          in Java. (returns `3.4028235e38`)
        min (float): The minimum allowed value for Floats, inherited from the canonical Iceberg implementation
          in Java (returns `-3.4028235e38`)
    """

    max: ClassVar[float] = 3.4028235e38
    min: ClassVar[float] = -3.4028235e38

    @property
    def string_type(self) -> str:
        return "float"


@dataclass(frozen=True)
class DoubleType(PrimitiveType):
    """A Double data type in Iceberg can be represented using an instance of this class. Doubles in Iceberg are
    64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
        >>> column_foo
        DoubleType()
    """

    @property
    def string_type(self) -> str:
        return "double"


@dataclass(frozen=True)
class DateType(PrimitiveType):
    """A Date data type in Iceberg can be represented using an instance of this class. Dates in Iceberg are
    calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
        >>> column_foo
        DateType()
    """

    @property
    def string_type(self) -> str:
        return "date"


@dataclass(frozen=True)
class TimeType(PrimitiveType):
    """A Time data type in Iceberg can be represented using an instance of this class. Times in Iceberg
    have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True
        >>> column_foo
        TimeType()
    """

    @property
    def string_type(self) -> str:
        return "time"


@dataclass(frozen=True)
class TimestampType(PrimitiveType):
    """A Timestamp data type in Iceberg can be represented using an instance of this class. Timestamps in
    Iceberg have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True
        >>> column_foo
        TimestampType()
    """

    @property
    def string_type(self) -> str:
        return "timestamp"


@dataclass(frozen=True)
class TimestamptzType(PrimitiveType):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class. Timestamptzs in
    Iceberg are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
        >>> column_foo
        TimestamptzType()
    """

    @property
    def string_type(self) -> str:
        return "timestamptz"


@dataclass(frozen=True)
class StringType(PrimitiveType):
    """A String data type in Iceberg can be represented using an instance of this class. Strings in
    Iceberg are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
        >>> column_foo
        StringType()
    """

    @property
    def string_type(self) -> str:
        return "string"


@dataclass(frozen=True)
class UUIDType(PrimitiveType):
    """A UUID data type in Iceberg can be represented using an instance of this class. UUIDs in
    Iceberg are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
        >>> column_foo
        UUIDType()
    """

    @property
    def string_type(self) -> str:
        return "uuid"


@dataclass(frozen=True)
class BinaryType(PrimitiveType):
    """A Binary data type in Iceberg can be represented using an instance of this class. Binaries in
    Iceberg are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
        >>> column_foo
        BinaryType()
    """

    @property
    def string_type(self) -> str:
        return "binary"

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
    >>> StructType(
        [
            NestedField(True, 1, "required_field", StringType()),
            NestedField(False, 2, "optional_field", IntegerType()),
        ]
    )

Notes:
  - https://iceberg.apache.org/#spec/#primitive-types
"""

from typing import Optional


class Type:
    def __init__(self, type_string: str, repr_string: str, is_primitive=False):
        self._type_string = type_string
        self._repr_string = repr_string
        self._is_primitive = is_primitive

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._type_string

    @property
    def is_primitive(self) -> bool:
        return self._is_primitive


class FixedType(Type):
    def __init__(self, length: int):
        super().__init__(
            f"fixed[{length}]", f"FixedType(length={length})", is_primitive=True
        )
        self._length = length

    @property
    def length(self) -> int:
        return self._length


class DecimalType(Type):
    def __init__(self, precision: int, scale: int):
        super().__init__(
            f"decimal({precision}, {scale})",
            f"DecimalType(precision={precision}, scale={scale})",
            is_primitive=True,
        )
        self._precision = precision
        self._scale = scale

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def scale(self) -> int:
        return self._scale


class NestedField(object):
    def __init__(
        self,
        is_optional: bool,
        field_id: int,
        name: str,
        field_type: Type,
        doc: Optional[str] = None,
    ):
        self._is_optional = is_optional
        self._id = field_id
        self._name = name
        self._type = field_type
        self._doc = doc

    @property
    def is_optional(self) -> bool:
        return self._is_optional

    @property
    def is_required(self) -> bool:
        return not self._is_optional

    @property
    def field_id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def type(self) -> Type:
        return self._type

    def __repr__(self):
        return (
            f"NestedField(is_optional={self._is_optional}, field_id={self._id}, "
            f"name={repr(self._name)}, field_type={repr(self._type)}, doc={repr(self._doc)})"
        )

    def __str__(self):
        return (
            f"{self._id}: {self._name}: {'optional' if self._is_optional else 'required'} {self._type}"
            ""
            if self._doc is None
            else f" ({self._doc})"
        )


class StructType(Type):
    def __init__(self, fields: list):
        super().__init__(
            f"struct<{', '.join(map(str, fields))}>",
            f"StructType(fields={repr(fields)})",
        )
        self._fields = fields

    @property
    def fields(self) -> list:
        return self._fields


class ListType(Type):
    def __init__(self, element: NestedField):
        super().__init__(f"list<{element.type}>", f"ListType(element={repr(element)})")
        self._element_field = element

    @property
    def element(self) -> NestedField:
        return self._element_field


class MapType(Type):
    def __init__(self, key: NestedField, value: NestedField):
        super().__init__(
            f"map<{key.type}, {value.type}>",
            f"MapType(key={repr(key)}, value={repr(value)})",
        )
        self._key_field = key
        self._value_field = value

    @property
    def key(self) -> NestedField:
        return self._key_field

    @property
    def value(self) -> NestedField:
        return self._value_field


class BooleanType(Type):
    """A boolean data type in Iceberg can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
    """

    def __init__(self):
        super().__init__("boolean", "BooleanType()", is_primitive=True)


class IntegerType(Type):
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

    max: int = 2147483647

    min: int = -2147483648

    def __init__(self):
        super().__init__("int", "IntegerType()", is_primitive=True)


class LongType(Type):
    """A Long data type in Iceberg can be represented using an instance of this class. Longs in Iceberg are
    64-bit signed integers.

    Example:
        >>> column_foo = LongType()
        >>> isinstance(column_foo, LongType)
        True

    Attributes:
        max (int): The maximum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java. (returns `9223372036854775807`)
        min (int): The minimum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java (returns `-9223372036854775808`)
    """

    max: int = 9223372036854775807

    min: int = -9223372036854775808

    def __init__(self):
        super().__init__("long", "LongType()", is_primitive=True)


class FloatType(Type):
    """A Float data type in Iceberg can be represented using an instance of this class. Floats in Iceberg are
    32-bit IEEE 754 floating points and can be promoted to Doubles.

    Example:
        >>> column_foo = FloatType()
        >>> isinstance(column_foo, FloatType)
        True
    """

    def __init__(self):
        super().__init__("float", "FloatType()", is_primitive=True)


class DoubleType(Type):
    """A Double data type in Iceberg can be represented using an instance of this class. Doubles in Iceberg are
    64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
    """

    def __init__(self):
        super().__init__("double", "DoubleType()", is_primitive=True)


class DateType(Type):
    """A Date data type in Iceberg can be represented using an instance of this class. Dates in Iceberg are
    calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
    """

    def __init__(self):
        super().__init__("date", "DateType()", is_primitive=True)


class TimeType(Type):
    """A Time data type in Iceberg can be represented using an instance of this class. Times in Iceberg
    have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True

    """

    def __init__(self):
        super().__init__("time", "TimeType()", is_primitive=True)


class TimestampType(Type):
    """A Timestamp data type in Iceberg can be represented using an instance of this class. Timestamps in
    Iceberg have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True

    """

    def __init__(self):
        super().__init__("timestamp", "TimestampType()", is_primitive=True)


class TimestamptzType(Type):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class. Timestamptzs in
    Iceberg are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
    """

    def __init__(self):
        super().__init__("timestamptz", "TimestamptzType()", is_primitive=True)


class StringType(Type):
    """A String data type in Iceberg can be represented using an instance of this class. Strings in
    Iceberg are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
    """

    def __init__(self):
        super().__init__("string", "StringType()", is_primitive=True)


class UUIDType(Type):
    """A UUID data type in Iceberg can be represented using an instance of this class. UUIDs in
    Iceberg are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
    """

    def __init__(self):
        super().__init__("uuid", "UUIDType()", is_primitive=True)


class BinaryType(Type):
    """A Binary data type in Iceberg can be represented using an instance of this class. Binarys in
    Iceberg are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
    """

    def __init__(self):
        super().__init__("binary", "BinaryType()", is_primitive=True)

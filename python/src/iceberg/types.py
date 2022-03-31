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

from typing import Dict, Optional, Tuple


class Singleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class IcebergType:
    """Base type for all Iceberg Types"""

    _initialized = False

    def __init__(self, type_string: str, repr_string: str):
        self._type_string = type_string
        self._repr_string = repr_string
        self._initialized = True

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._type_string

    @property
    def is_primitive(self) -> bool:
        return isinstance(self, PrimitiveType)


class PrimitiveType(IcebergType):
    """Base class for all Iceberg Primitive Types"""


class FixedType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> FixedType(8)
        FixedType(length=8)
        >>> FixedType(8)==FixedType(8)
        True
    """

    _instances: Dict[int, "FixedType"] = {}

    def __new__(cls, length: int):
        cls._instances[length] = cls._instances.get(length) or object.__new__(cls)
        return cls._instances[length]

    def __init__(self, length: int):
        if not self._initialized:
            super().__init__(f"fixed[{length}]", f"FixedType(length={length})")
            self._length = length

    @property
    def length(self) -> int:
        return self._length


class DecimalType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> DecimalType(32, 3)
        DecimalType(precision=32, scale=3)
        >>> DecimalType(8, 3) == DecimalType(8, 3)
        True
    """

    _instances: Dict[Tuple[int, int], "DecimalType"] = {}

    def __new__(cls, precision: int, scale: int):
        key = (precision, scale)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(self, precision: int, scale: int):
        if not self._initialized:
            super().__init__(
                f"decimal({precision}, {scale})",
                f"DecimalType(precision={precision}, scale={scale})",
            )
            self._precision = precision
            self._scale = scale

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def scale(self) -> int:
        return self._scale


class NestedField(IcebergType):
    """Represents a field of a struct, a map key, a map value, or a list element.

    This is where field IDs, names, docs, and nullability are tracked.
    """

    _instances: Dict[Tuple[bool, int, str, IcebergType, Optional[str]], "NestedField"] = {}

    def __new__(
        cls,
        field_id: int,
        name: str,
        field_type: IcebergType,
        is_optional: bool = True,
        doc: Optional[str] = None,
    ):
        key = (is_optional, field_id, name, field_type, doc)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(
        self,
        field_id: int,
        name: str,
        field_type: IcebergType,
        is_optional: bool = True,
        doc: Optional[str] = None,
    ):
        if not self._initialized:
            docString = "" if doc is None else f", doc={repr(doc)}"
            super().__init__(
                (
                    f"{field_id}: {name}: {'optional' if is_optional else 'required'} {field_type}" ""
                    if doc is None
                    else f" ({doc})"
                ),
                f"NestedField(field_id={field_id}, name={repr(name)}, field_type={repr(field_type)}, is_optional={is_optional}"
                f"{docString})",
            )
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
    def doc(self) -> Optional[str]:
        return self._doc

    @property
    def type(self) -> IcebergType:
        return self._type


class StructType(IcebergType):
    """A struct type in Iceberg

    Example:
        >>> str(StructType(
        ...     NestedField(1, "required_field", StringType(), True),
        ...     NestedField(2, "optional_field", IntegerType())
        ... ))
        'struct<1: required_field: optional string, 2: optional_field: optional int>'
    """

    _instances: Dict[Tuple[NestedField, ...], "StructType"] = {}

    def __new__(cls, *fields: NestedField):
        cls._instances[fields] = cls._instances.get(fields) or object.__new__(cls)
        return cls._instances[fields]

    def __init__(self, *fields: NestedField):
        if not self._initialized:
            super().__init__(
                f"struct<{', '.join(map(str, fields))}>",
                f"StructType{repr(fields)}",
            )
            self._fields = fields

    @property
    def fields(self) -> Tuple[NestedField, ...]:
        return self._fields


class ListType(IcebergType):
    """A list type in Iceberg

    Example:
        >>> ListType(element_id=3, element_type=StringType(), element_is_optional=True)
        ListType(element_id=3, element_type=StringType(), element_is_optional=True)
    """

    _instances: Dict[Tuple[bool, int, IcebergType], "ListType"] = {}

    def __new__(
        cls,
        element_id: int,
        element_type: IcebergType,
        element_is_optional: bool = True,
    ):
        key = (element_is_optional, element_id, element_type)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(
        self,
        element_id: int,
        element_type: IcebergType,
        element_is_optional: bool = True,
    ):
        if not self._initialized:
            super().__init__(
                f"list<{element_type}>",
                f"ListType(element_id={element_id}, element_type={repr(element_type)}, "
                f"element_is_optional={element_is_optional})",
            )
            self._element_field = NestedField(
                name="element",
                is_optional=element_is_optional,
                field_id=element_id,
                field_type=element_type,
            )

    @property
    def element(self) -> NestedField:
        return self._element_field


class MapType(IcebergType):
    """A map type in Iceberg

    Example:
        >>> MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_is_optional=True)
        MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_is_optional=True)
    """

    _instances: Dict[Tuple[int, IcebergType, int, IcebergType, bool], "MapType"] = {}

    def __new__(
        cls,
        key_id: int,
        key_type: IcebergType,
        value_id: int,
        value_type: IcebergType,
        value_is_optional: bool = True,
    ):
        impl_key = (key_id, key_type, value_id, value_type, value_is_optional)
        cls._instances[impl_key] = cls._instances.get(impl_key) or object.__new__(cls)
        return cls._instances[impl_key]

    def __init__(
        self,
        key_id: int,
        key_type: IcebergType,
        value_id: int,
        value_type: IcebergType,
        value_is_optional: bool = True,
    ):
        if not self._initialized:
            super().__init__(
                f"map<{key_type}, {value_type}>",
                f"MapType(key_id={key_id}, key_type={repr(key_type)}, value_id={value_id}, value_type={repr(value_type)}, "
                f"value_is_optional={value_is_optional})",
            )
            self._key_field = NestedField(name="key", field_id=key_id, field_type=key_type, is_optional=False)
            self._value_field = NestedField(
                name="value",
                field_id=value_id,
                field_type=value_type,
                is_optional=value_is_optional,
            )

    @property
    def key(self) -> NestedField:
        return self._key_field

    @property
    def value(self) -> NestedField:
        return self._value_field


class BooleanType(PrimitiveType, Singleton):
    """A boolean data type in Iceberg can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("boolean", "BooleanType()")


class IntegerType(PrimitiveType, Singleton):
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
        if not self._initialized:
            super().__init__("int", "IntegerType()")


class LongType(PrimitiveType, Singleton):
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
        if not self._initialized:
            super().__init__("long", "LongType()")


class FloatType(PrimitiveType, Singleton):
    """A Float data type in Iceberg can be represented using an instance of this class. Floats in Iceberg are
    32-bit IEEE 754 floating points and can be promoted to Doubles.

    Example:
        >>> column_foo = FloatType()
        >>> isinstance(column_foo, FloatType)
        True

    Attributes:
        max (float): The maximum allowed value for Floats, inherited from the canonical Iceberg implementation
          in Java. (returns `3.4028235e38`)
        min (float): The minimum allowed value for Floats, inherited from the canonical Iceberg implementation
          in Java (returns `-3.4028235e38`)
    """

    max: float = 3.4028235e38

    min: float = -3.4028235e38

    def __init__(self):
        if not self._initialized:
            super().__init__("float", "FloatType()")


class DoubleType(PrimitiveType, Singleton):
    """A Double data type in Iceberg can be represented using an instance of this class. Doubles in Iceberg are
    64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("double", "DoubleType()")


class DateType(PrimitiveType, Singleton):
    """A Date data type in Iceberg can be represented using an instance of this class. Dates in Iceberg are
    calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("date", "DateType()")


class TimeType(PrimitiveType, Singleton):
    """A Time data type in Iceberg can be represented using an instance of this class. Times in Iceberg
    have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("time", "TimeType()")


class TimestampType(PrimitiveType, Singleton):
    """A Timestamp data type in Iceberg can be represented using an instance of this class. Timestamps in
    Iceberg have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("timestamp", "TimestampType()")


class TimestamptzType(PrimitiveType, Singleton):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class. Timestamptzs in
    Iceberg are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("timestamptz", "TimestamptzType()")


class StringType(PrimitiveType, Singleton):
    """A String data type in Iceberg can be represented using an instance of this class. Strings in
    Iceberg are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("string", "StringType()")


class UUIDType(PrimitiveType, Singleton):
    """A UUID data type in Iceberg can be represented using an instance of this class. UUIDs in
    Iceberg are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("uuid", "UUIDType()")


class BinaryType(PrimitiveType, Singleton):
    """A Binary data type in Iceberg can be represented using an instance of this class. Binarys in
    Iceberg are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
    """

    def __init__(self):
        if not self._initialized:
            super().__init__("binary", "BinaryType()")

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

from typing import Dict, List, Optional, Tuple


class Singleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class IcebergType:
    """Base type for all Iceberg Types"""

    def __init__(self, type_string: str, repr_string: str, is_primitive=False):
        self._type_string = type_string
        self._repr_string = repr_string

    def __repr__(self):
        return self._repr_string

    def __str__(self):
        return self._type_string

    @property
    def is_primitive(self) -> bool:
        return isinstance(self, PrimitiveType)


class PrimitiveType(IcebergType):
    """Base class for all Iceberg Primitive Types"""

    _instances = {}  # type: ignore

    def __new__(cls):
        cls._instances[cls] = cls._instances.get(cls) or object.__new__(cls)
        return cls._instances[cls]


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
        >>> DecimalType(8, 3)==DecimalType(8, 3)
        True
    """

    _instances: Dict[Tuple[int, int], "DecimalType"] = {}

    def __new__(cls, precision: int, scale: int):
        key = precision, scale
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(self, precision: int, scale: int):
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
    """equivalent of `NestedField` type from Java implementation"""

    _instances: Dict[Tuple[bool, int, str, IcebergType, Optional[str]], "NestedField"] = {}

    def __new__(
        cls,
        is_optional: bool,
        field_id: int,
        name: str,
        field_type: IcebergType,
        doc: Optional[str] = None,
    ):
        key = is_optional, field_id, name, field_type, doc
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(
        self,
        is_optional: bool,
        field_id: int,
        name: str,
        field_type: IcebergType,
        doc: Optional[str] = None,
    ):
        super().__init__(
            (f"{field_id}: {name}: {'optional' if is_optional else 'required'} {field_type}" "" if doc is None else f" ({doc})"),
            f"NestedField(is_optional={is_optional}, field_id={field_id}, "
            f"name={repr(name)}, field_type={repr(field_type)}, doc={repr(doc)})",
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
            >>> StructType(
                [
                    NestedField(True, 1, "required_field", StringType()),
                    NestedField(False, 2, "optional_field", IntegerType()),
                ]
    """

    _instances: Dict[Tuple[NestedField, ...], "StructType"] = {}

    def __new__(cls, fields: List[NestedField]):
        key = tuple(fields)
        cls._instances[key] = cls._instances.get(key) or object.__new__(cls)
        return cls._instances[key]

    def __init__(self, fields: List[NestedField] = []):
        super().__init__(
            f"struct<{', '.join(map(str, fields))}>",
            f"StructType(fields={repr(fields)})",
        )
        self._fields = fields

    @property
    def fields(self) -> List[NestedField]:
        return self._fields


class ListType(IcebergType):
    """A list type in Iceberg

    Example:
        >>> ListType(NestedField(True, 1, "required_field", StringType()))
        ListType(element=NestedField(is_optional=True, field_id=1, name='required_field', field_type=StringType(), doc=None))
    """

    _instances: Dict[NestedField, "ListType"] = {}

    def __new__(cls, element: NestedField):
        cls._instances[element] = cls._instances.get(element) or object.__new__(cls)
        return cls._instances[element]

    def __init__(self, element: NestedField):
        super().__init__(f"list<{element.type}>", f"ListType(element={repr(element)})")
        self._element_field = element

    @property
    def element(self) -> NestedField:
        return self._element_field


class MapType(IcebergType):
    """A map type in Iceberg

    Example:
        >>> MapType(
                NestedField(True, 1, "required_field", StringType()),
                NestedField(False, 2, "optional_field", IntegerType()),
            )
        MapType(key=NestedField(is_optional=True, field_id=1, name='required_field', field_type=StringType(), doc=None), value=NestedField(is_optional=False, field_id=2, name='optional_field', field_type=IntegerType(), doc=None))
    """

    _instances: Dict[Tuple[NestedField, NestedField], "MapType"] = {}

    def __new__(cls, key: NestedField, value: NestedField):
        impl_key = key, value
        cls._instances[impl_key] = cls._instances.get(impl_key) or object.__new__(cls)
        return cls._instances[impl_key]

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


class BooleanType(IcebergType, Singleton):
    """A boolean data type in Iceberg can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
    """

    def __init__(self):
        super().__init__("boolean", "BooleanType()")


class IntegerType(IcebergType, Singleton):
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
        super().__init__("int", "IntegerType()")


class LongType(IcebergType, Singleton):
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
        super().__init__("long", "LongType()")


class FloatType(IcebergType, Singleton):
    """A Float data type in Iceberg can be represented using an instance of this class. Floats in Iceberg are
    32-bit IEEE 754 floating points and can be promoted to Doubles.

    Example:
        >>> column_foo = FloatType()
        >>> isinstance(column_foo, FloatType)
        True
    """

    def __init__(self):
        super().__init__("float", "FloatType()")


class DoubleType(IcebergType, Singleton):
    """A Double data type in Iceberg can be represented using an instance of this class. Doubles in Iceberg are
    64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
    """

    def __init__(self):
        super().__init__("double", "DoubleType()")


class DateType(IcebergType, Singleton):
    """A Date data type in Iceberg can be represented using an instance of this class. Dates in Iceberg are
    calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
    """

    def __init__(self):
        super().__init__("date", "DateType()")


class TimeType(IcebergType, Singleton):
    """A Time data type in Iceberg can be represented using an instance of this class. Times in Iceberg
    have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True
    """

    def __init__(self):
        super().__init__("time", "TimeType()")


class TimestampType(IcebergType, Singleton):
    """A Timestamp data type in Iceberg can be represented using an instance of this class. Timestamps in
    Iceberg have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True
    """

    def __init__(self):
        super().__init__("timestamp", "TimestampType()")


class TimestamptzType(IcebergType, Singleton):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class. Timestamptzs in
    Iceberg are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
    """

    def __init__(self):
        super().__init__("timestamptz", "TimestamptzType()")


class StringType(IcebergType, Singleton):
    """A String data type in Iceberg can be represented using an instance of this class. Strings in
    Iceberg are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
    """

    def __init__(self):
        super().__init__("string", "StringType()")


class UUIDType(IcebergType, Singleton):
    """A UUID data type in Iceberg can be represented using an instance of this class. UUIDs in
    Iceberg are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
    """

    def __init__(self):
        super().__init__("uuid", "UUIDType()")


class BinaryType(IcebergType, Singleton):
    """A Binary data type in Iceberg can be represented using an instance of this class. Binarys in
    Iceberg are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
    """

    def __init__(self):
        super().__init__("binary", "BinaryType()")

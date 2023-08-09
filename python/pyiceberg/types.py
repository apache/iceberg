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
"""Data types used in describing Iceberg schemas.

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

import re
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    Literal,
    Optional,
    Tuple,
)

from pydantic import (
    BeforeValidator,
    Field,
    GetCoreSchemaHandler,
    PlainSerializer,
    SerializeAsAny,
    WithJsonSchema, PrivateAttr,
)
from pydantic_core import core_schema

from pyiceberg.exceptions import ValidationError
from pyiceberg.typedef import IcebergBaseModel, IcebergRootModel
from pyiceberg.utils.parsing import ParseNumberFromBrackets
from pyiceberg.utils.singleton import Singleton

DECIMAL_REGEX = re.compile(r"decimal\((\d+),\s*(\d+)\)")
FIXED = "fixed"
FIXED_PARSER = ParseNumberFromBrackets(FIXED)


class IcebergType(IcebergBaseModel):
    """Base type for all Iceberg Types.

    Example:
        >>> str(IcebergType())
        'IcebergType()'
        >>> repr(IcebergType())
        'IcebergType()'
    """

    @classmethod
    def validate(cls, v: Any) -> IcebergType:
        # When Pydantic is unable to determine the subtype
        # In this case we'll help pydantic a bit by parsing the
        # primitive type ourselves, or pointing it at the correct
        # complex type by looking at the type field

        if isinstance(v, dict):
            if v.get("type") == "struct":
                return StructType(**v)
            elif v.get("type") == "list":
                return ListType(**v)
            elif v.get("type") == "map":
                return MapType(**v)
            else:
                return NestedField(**v)
        else:
            return v

    @property
    def is_primitive(self) -> bool:
        return isinstance(self, PrimitiveType)

    @property
    def is_struct(self) -> bool:
        return isinstance(self, StructType)


class PrimitiveType(IcebergRootModel[str], IcebergType, Singleton):
    """Base class for all Iceberg Primitive Types."""

    root: str = Field()

    def __repr__(self) -> str:
        """Returns the string representation of the PrimitiveType class."""
        return f"{type(self).__name__}()"

    def __str__(self) -> str:
        """Returns the string representation of the PrimitiveType class."""
        return self.root


def _parse_fixed_type(fixed: Any) -> int:
    if isinstance(fixed, str):
        return FIXED_PARSER.match(fixed)
    elif isinstance(fixed, dict):
        return fixed["length"]
    else:
        return fixed


class FixedType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> FixedType(8)
        FixedType(length=8)
        >>> FixedType(8) == FixedType(8)
        True
        >>> FixedType(19) == FixedType(25)
        False
    """

    root: str = Field()
    _length = PrivateAttr()

    def __init__(self, a: Any, **data) -> None:
        self._length = _parse_fixed_type(a)
        super().__init__(f"fixed[{a}]", **data)

    def __len__(self) -> int:
        """Returns the length of an instance of the FixedType class."""
        return self._length

    def __str__(self) -> str:
        return f"fixed[{self._length}]"

    def __repr__(self) -> str:
        """Returns the string representation of the FixedType class."""
        return f"FixedType({self._length})"

    def __getnewargs__(self) -> Tuple[int]:
        """A magic function for pickling the FixedType class."""
        return (self._length,)

    def __hash__(self) -> int:
        return hash(self._length)

    def __eq__(self, other: Any) -> bool:
        return len(other) == len(self) if isinstance(other, FixedType) else False


def _parse_decimal_type(decimal: Any) -> Tuple[int, int]:
    if isinstance(decimal, str):
        matches = DECIMAL_REGEX.search(decimal)
        if matches:
            return int(matches.group(1)), int(matches.group(2))
        else:
            raise ValidationError(f"Could not parse {decimal} into a DecimalType")
    elif isinstance(decimal, dict):
        return decimal["precision"], decimal["scale"]
    else:
        return decimal


class DecimalType(PrimitiveType):
    """A fixed data type in Iceberg.

    Example:
        >>> DecimalType(32, 3)
        DecimalType(precision=32, scale=3)
        >>> DecimalType(8, 3) == DecimalType(8, 3)
        True
    """

    root: str = Field()
    _precision: int = PrivateAttr()
    _scale: int = PrivateAttr()

    def __init__(self, precision: int, scale: int, **data) -> None:
        self._precision = precision
        self._scale = scale
        super().__init__(f"Decimal({self._precision}, {self._scale})", **data)


    @property
    def precision(self) -> int:
        return self._precision

    @property
    def scale(self) -> int:
        return self._scale

    def __repr__(self) -> str:
        """Returns the string representation of the DecimalType class."""
        return f"DecimalType(precision={self.precision}, scale={self.scale})"

    def __str__(self) -> str:
        return f"decimal({self.precision}, {self.scale})"

    def __getnewargs__(self) -> Tuple[int, int]:
        """A magic function for pickling the DecimalType class."""
        return self._precision, self._scale

    def __eq__(self, other: Any) -> bool:
        return self.root == other.root if isinstance(other, DecimalType) else False


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
        '1: foo: optional fixed[22]'
        >>> str(NestedField(
        ...     field_id=2,
        ...     name='bar',
        ...     field_type=LongType(),
        ...     is_optional=False,
        ...     doc="Just a long"
        ... ))
        '2: bar: required long (Just a long)'
    """

    field_id: int = Field(alias="id")
    name: str = Field()
    field_type: SerializeAsAny[IcebergType] = Field(alias="type")
    required: bool = Field(default=True)
    doc: Optional[str] = Field(default=None, repr=False)
    initial_default: Optional[Any] = Field(alias="initial-default", default=None, repr=False)

    def __init__(
        self,
        field_id: Optional[int] = None,
        name: Optional[str] = None,
        field_type: Optional[IcebergType] = None,
        required: bool = True,
        doc: Optional[str] = None,
        initial_default: Optional[Any] = None,
        **data: Any,
    ):
        # We need an init when we want to use positional arguments, but
        # need also to support the aliases.
        data["id"] = data["id"] if "id" in data else field_id
        data["name"] = name
        data["type"] = data["type"] if "type" in data else field_type
        data["required"] = required
        data["doc"] = doc
        data["initial-default"] = initial_default
        super().__init__(**data)


    def __str__(self) -> str:
        """Returns the string representation of the NestedField class."""
        doc = "" if not self.doc else f" ({self.doc})"
        req = "required" if self.required else "optional"
        return f"{self.field_id}: {self.name}: {req} {self.field_type}{doc}"

    def __getnewargs__(self) -> Tuple[int, str, IcebergType, bool, Optional[str]]:
        """A magic function for pickling the NestedField class."""
        return self.field_id, self.name, self.field_type, self.required, self.doc

    @property
    def optional(self) -> bool:
        return not self.required


class StructType(IcebergType):
    """A struct type in Iceberg.

    Example:
        >>> str(StructType(
        ...     NestedField(1, "required_field", StringType(), True),
        ...     NestedField(2, "optional_field", IntegerType())
        ... ))
        'struct<1: required_field: optional string, 2: optional_field: optional int>'
    """

    type: Literal["struct"] = "struct"
    fields: Tuple[NestedField, ...] = Field(default_factory=tuple)
    _hash: int = PrivateAttr()

    def __init__(self, *fields: NestedField, **data: Any):
        # In case we use positional arguments, instead of keyword args
        if fields:
            data["fields"] = fields
        super().__init__(**data)
        self._hash = hash(self.fields)

    def field(self, field_id: int) -> Optional[NestedField]:
        for field in self.fields:
            if field.field_id == field_id:
                return field
        return None

    def __str__(self) -> str:
        """Returns the string representation of the StructType class."""
        return f"struct<{', '.join(map(str, self.fields))}>"

    def __repr__(self) -> str:
        """Returns the string representation of the StructType class."""
        return f"StructType(fields=({', '.join(map(repr, self.fields))},))"

    def __len__(self) -> int:
        """Returns the length of an instance of the StructType class."""
        return len(self.fields)

    def __getnewargs__(self) -> Tuple[NestedField, ...]:
        """A magic function for pickling the StructType class."""
        return self.fields

    def __hash__(self) -> int:
        """Used the cache hash value of the StructType class."""
        return self._hash


class ListType(IcebergType):
    """A list type in Iceberg.

    Example:
        >>> ListType(element_id=3, element_type=StringType(), element_required=True)
        ListType(element_id=3, element_type=StringType(), element_required=True)
    """

    class Config:
        fields = {"element_field": {"exclude": True}}

    type: Literal["list"] = "list"
    element_id: int = Field(alias="element-id")
    element_type: SerializeAsAny[IcebergType] = Field(alias="element")
    element_required: bool = Field(alias="element-required", default=True)
    element_field: NestedField = Field(init=False, repr=False)
    _hash: int = PrivateAttr()

    def __init__(
        self, element_id: Optional[int] = None, element: Optional[IcebergType] = None, element_required: bool = True, **data: Any
    ):
        data["element-id"] = data["element-id"] if "element-id" in data else element_id
        data["element"] = element or data["element_type"]
        data["element-required"] = data["element-required"] if "element-required" in data else element_required
        data["element_field"] = NestedField(
            name="element",
            field_id=data["element-id"],
            field_type=data["element"],
            required=data["element-required"],
        )
        super().__init__(**data)
        self._hash = hash(data.values())

    def __str__(self) -> str:
        """Returns the string representation of the ListType class."""
        return f"list<{self.element_type}>"

    def __getnewargs__(self) -> Tuple[int, IcebergType, bool]:
        """A magic function for pickling the ListType class."""
        return self.element_id, self.element_type, self.element_required

    def __hash__(self) -> int:
        """Used the cache hash value of the StructType class."""
        return self._hash

    def __eq__(self, other: Any) -> bool:
        return self.element_field == other.element_field if isinstance(other, ListType) else False


class MapType(IcebergType):
    """A map type in Iceberg.

    Example:
        >>> MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=True)
        MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=True)
    """

    type: Literal["map"] = "map"
    key_id: int = Field(alias="key-id")
    key_type: IcebergType = Field(alias="key")
    value_id: int = Field(alias="value-id")
    value_type: IcebergType = Field(alias="value")
    value_required: bool = Field(alias="value-required", default=True)
    key_field: NestedField = Field(init=False, repr=False)
    value_field: NestedField = Field(init=False, repr=False)

    class Config:
        fields = {"key_field": {"exclude": True}, "value_field": {"exclude": True}}

    def __init__(
        self,
        key_id: Optional[int] = None,
        key_type: Optional[IcebergType] = None,
        value_id: Optional[int] = None,
        value_type: Optional[IcebergType] = None,
        value_required: bool = True,
        **data: Any,
    ):
        data["key-id"] = data["key-id"] if "key-id" in data else key_id
        data["key"] = data["key"] if "key" in data else key_type
        data["value-id"] = data["value-id"] if "value-id" in data else value_id
        data["value"] = data["value"] if "value" in data else value_type
        data["value_required"] = data["value_required"] if "value_required" in data else value_required

        data["key_field"] = NestedField(name="key", field_id=data["key-id"], field_type=data["key"], required=True)
        data["value_field"] = NestedField(
            name="value", field_id=data["value-id"], field_type=data["value"], required=data["value_required"]
        )
        super().__init__(**data)

    def __str__(self) -> str:
        """Returns the string representation of the MapType class."""
        return f"map<{self.key_type}, {self.value_type}>"

    def __getnewargs__(self) -> Tuple[int, IcebergType, int, IcebergType, bool]:
        """A magic function for pickling the MapType class."""
        return (self.key_id, self.key_type, self.value_id, self.value_type, self.value_required)


class BooleanType(PrimitiveType):
    """A boolean data type in Iceberg can be represented using an instance of this class.

    Example:
        >>> column_foo = BooleanType()
        >>> isinstance(column_foo, BooleanType)
        True
        >>> column_foo
        BooleanType()
    """

    def __init__(self) -> None:
        super().__init__("boolean")

    root: Literal["boolean"] = "boolean"


class IntegerType(PrimitiveType):
    """An Integer data type in Iceberg can be represented using an instance of this class.

    Integers in Iceberg are 32-bit signed and can be promoted to Longs.

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

    def __init__(self) -> None:
        super().__init__("int")

    max: ClassVar[int] = 2147483647
    min: ClassVar[int] = -2147483648

    root: Literal["int"] = "int"


class LongType(PrimitiveType):
    """A Long data type in Iceberg can be represented using an instance of this class.

    Longs in Iceberg are 64-bit signed integers.

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

    def __init__(self) -> None:
        super().__init__("long")

    max: ClassVar[int] = 9223372036854775807
    min: ClassVar[int] = -9223372036854775808

    root: Literal["long"] = "long"


class FloatType(PrimitiveType):
    """A Float data type in Iceberg can be represented using an instance of this class.

    Floats in Iceberg are 32-bit IEEE 754 floating points and can be promoted to Doubles.

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

    def __init__(self) -> None:
        super().__init__("float")

    max: ClassVar[float] = 3.4028235e38
    min: ClassVar[float] = -3.4028235e38

    root: Literal["float"] = "float"


class DoubleType(PrimitiveType):
    """A Double data type in Iceberg can be represented using an instance of this class.

    Doubles in Iceberg are 64-bit IEEE 754 floating points.

    Example:
        >>> column_foo = DoubleType()
        >>> isinstance(column_foo, DoubleType)
        True
        >>> column_foo
        DoubleType()
    """

    def __init__(self) -> None:
        super().__init__("double")

    root: Literal["double"] = "double"


class DateType(PrimitiveType):
    """A Date data type in Iceberg can be represented using an instance of this class.

    Dates in Iceberg are calendar dates without a timezone or time.

    Example:
        >>> column_foo = DateType()
        >>> isinstance(column_foo, DateType)
        True
        >>> column_foo
        DateType()
    """

    def __init__(self) -> None:
        super().__init__("date")

    root: Literal["date"] = "date"


class TimeType(PrimitiveType):
    """A Time data type in Iceberg can be represented using an instance of this class.

    Times in Iceberg have microsecond precision and are a time of day without a date or timezone.

    Example:
        >>> column_foo = TimeType()
        >>> isinstance(column_foo, TimeType)
        True
        >>> column_foo
        TimeType()
    """

    def __init__(self) -> None:
        super().__init__("time")

    root: Literal["time"] = "time"


class TimestampType(PrimitiveType):
    """A Timestamp data type in Iceberg can be represented using an instance of this class.

    Timestamps in Iceberg have microsecond precision and include a date and a time of day without a timezone.

    Example:
        >>> column_foo = TimestampType()
        >>> isinstance(column_foo, TimestampType)
        True
        >>> column_foo
        TimestampType()
    """

    def __init__(self) -> None:
        super().__init__("timestamp")

    root: Literal["timestamp"] = "timestamp"


class TimestamptzType(PrimitiveType):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class.

    Timestamptzs in Iceberg are stored as UTC and include a date and a time of day with a timezone.

    Example:
        >>> column_foo = TimestamptzType()
        >>> isinstance(column_foo, TimestamptzType)
        True
        >>> column_foo
        TimestamptzType()
    """

    def __init__(self) -> None:
        super().__init__("timestamptz")

    root: Literal["timestamptz"] = "timestamptz"


class StringType(PrimitiveType):
    """A String data type in Iceberg can be represented using an instance of this class.

    Strings in Iceberg are arbitrary-length character sequences and are encoded with UTF-8.

    Example:
        >>> column_foo = StringType()
        >>> isinstance(column_foo, StringType)
        True
        >>> column_foo
        StringType()
    """

    def __init__(self) -> None:
        super().__init__("string")

    root: Literal["string"] = "string"


class UUIDType(PrimitiveType):
    """A UUID data type in Iceberg can be represented using an instance of this class.

    UUIDs in Iceberg are universally unique identifiers.

    Example:
        >>> column_foo = UUIDType()
        >>> isinstance(column_foo, UUIDType)
        True
        >>> column_foo
        UUIDType()
    """

    def __init__(self) -> None:
        super().__init__("uuid")

    root: Literal["uuid"] = "uuid"


class BinaryType(PrimitiveType):
    """A Binary data type in Iceberg can be represented using an instance of this class.

    Binaries in Iceberg are arbitrary-length byte arrays.

    Example:
        >>> column_foo = BinaryType()
        >>> isinstance(column_foo, BinaryType)
        True
        >>> column_foo
        BinaryType()
    """

    def __init__(self) -> None:
        super().__init__("binary")

    root: Literal["binary"] = "binary"


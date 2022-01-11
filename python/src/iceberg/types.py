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

from typing import Optional


class Type(object):
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


BooleanType = Type("boolean", "BooleanType", is_primitive=True)
IntegerType = Type("int", "IntegerType", is_primitive=True)
LongType = Type("long", "LongType", is_primitive=True)
FloatType = Type("float", "FloatType", is_primitive=True)
DoubleType = Type("double", "DoubleType", is_primitive=True)
DateType = Type("date", "DateType", is_primitive=True)
TimeType = Type("time", "TimeType", is_primitive=True)
TimestampType = Type("timestamp", "TimestampType", is_primitive=True)
TimestamptzType = Type("timestamptz", "TimestamptzType", is_primitive=True)
StringType = Type("string", "StringType", is_primitive=True)
UUIDType = Type("uuid", "UUIDType", is_primitive=True)
BinaryType = Type("binary", "BinaryType", is_primitive=True)

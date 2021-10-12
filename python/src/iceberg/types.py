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
    def is_primitive(self):
        return self._is_primitive


class FixedType(Type):
    def __init__(self, length: int):
        super().__init__(f"fixed[{length}]", f"FixedType[{length}]", is_primitive=True)
        self._length = length


class DecimalType(Type):
    def __init__(self, precision: int, scale: int):
        super().__init__(f"decimal({precision}, {scale})", f"DecimalType({precision}, {scale})", is_primitive=True)
        self._precision = precision
        self._scale = scale

    def precision(self):
        return self._precision

    def scale(self):
        return self._scale


class NestedField(object):
    def __init__(self, is_optional: bool, field_id: int, name: str, field_type: Type, doc=None):
        self._is_optional = is_optional
        self._id = field_id
        self._name = name
        self._type = field_type
        self._doc = doc

    @property
    def is_optional(self):
        return self._is_optional

    @property
    def is_required(self):
        return not self._is_optional

    @property
    def field_id(self):
        return self._id

    @property
    def type(self):
        return self._type

    def __repr__(self):
        return f"{self._id}: {self._name}: {'optional' if self._is_optional else 'required'} {self._type}" \
               "" if self._doc is None else f" ({self._doc})"

    def __str__(self):
        return self.__repr__()


class StructType(Type):
    def __init__(self, fields: list):
        super().__init__(f"struct<{', '.join(map(str, fields))}>", f"StructType<{', '.join(map(str, fields))}>")
        self._fields = fields


class ListType(Type):
    def __init__(self, element_field: NestedField):
        super().__init__(f"list<{element_field.type}>", f"ListType<{element_field.type}>")
        self._element_field = element_field


class MapType(Type):
    def __init__(self, key_field: NestedField, value_field: NestedField):
        super().__init__(f"map<{key_field.type}, {value_field.type}>",
                         f"MapType<{key_field.type}, {value_field.type}>")
        self._key_field = key_field
        self._value_field = value_field


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

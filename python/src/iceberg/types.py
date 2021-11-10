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


from typing import TypeVar, Generic, List, Dict, Optional


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
    def __init__(self, fields: List[NestedField]):
        super().__init__(
            f"struct<{', '.join(map(str, fields))}>",
            f"StructType(fields={repr(fields)})",
        )
        self._fields = fields

    @property
    def fields(self) -> List[NestedField]:
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


class Schema(object):
    """
    Schema of a data table
    """

    def __init__(
        self,
        columns: List[NestedField],
        schema_id: int,
        identifier_field_ids: [int],
        id_to_field: Dict[int, NestedField],
        alias_to_id: Dict[str, int],
        name_to_id: Dict[str, int],
        lowercase_name_to_id: Dict[str, int],
    ):
        self._struct = StructType(columns)
        self._schema_id = schema_id
        self._identifier_field_ids = identifier_field_ids
        self._id_to_field = id_to_field
        self._alias_to_id = alias_to_id
        self._name_to_id = name_to_id
        self._lowercase_name_to_id = lowercase_name_to_id

    @property
    def columns(self):
        return self._struct.fields

    @property
    def schema_id(self):
        return self._schema_id

    @property
    def identifier_field_ids(self):
        return self._identifier_field_ids

    def as_struct(self):
        return self._struct

    def _id_to_field(self) -> Dict[int, NestedField]:
        if not self._id_to_field:
            return index_by_id(self._struct)

    def find_type(self, field_id):
        field = self._id_to_field.get(field_id)
        if field:
            return field.type
        return None

    def _name_to_id(self) -> Dict[str, int]:
        if not self._name_to_id:
            return index_by_name(self._struct)

    def _id_to_name(self) -> Dict[int, str]:
        if not self._id_to_name():
            return index_name_by_id(self._struct)

    def find_field(self, name: str) -> NestedField:
        # TODO: needs TypeUtil Methods
        pass


T = TypeVar("T")


class SchemaVisitor(Generic[T]):
    def before_field(self, field: NestedField) -> None:
        pass

    def after_field(self, field: NestedField) -> None:
        pass

    def before_list_element(self, element: NestedField) -> None:
        self.before_field(element)

    def after_list_element(self, element: NestedField) -> None:
        self.after_field(element)

    def before_map_key(self, key: NestedField) -> None:
        self.before_field(key)

    def after_map_key(self, key: NestedField) -> None:
        self.after_field(key)

    def before_map_value(self, value: NestedField) -> None:
        self.before_field(value)

    def after_map_value(self, value: NestedField) -> None:
        self.after_field(value)

    def schema(self, schema: Schema, struct_result: T) -> T:
        return None

    def struct(self, struct: StructType, field_results: List[T]) -> T:
        return None

    def field(self, field: NestedField, field_result: T) -> T:
        return None

    def list(self, list_type: ListType, element_result: T) -> T:
        return None

    def map(self, map_type: MapType, key_result: T, value_result: T) -> T:
        return None

    def primitive(self, primitive: Type) -> T:
        return None


def visit(obj, visitor: SchemaVisitor[T]) -> T:
    if isinstance(obj, Schema):
        return visitor.schema(obj, visit(obj.as_struct(), visitor))

    elif isinstance(obj, StructType):
        results = []
        for field in obj.fields:
            visitor.before_field(field)
            try:
                result = visit(field.type, visitor)
            finally:
                visitor.after_field(field)

            results.append(visitor.field(field, result))

        return visitor.struct(obj, results)

    elif isinstance(obj, ListType):
        visitor.before_list_element(obj.element)
        try:
            result = visit(obj.element.type, visitor)
        finally:
            visitor.after_list_element(obj.element)

        return visitor.list(obj, result)

    elif isinstance(obj, MapType):
        visitor.before_map_key(obj.key)
        try:
            key_result = visit(obj.key.type, visitor)
        finally:
            visitor.after_map_key(obj.key)

        visitor.before_map_value(obj.value)
        try:
            value_result = visit(obj.value.type, visitor)
        finally:
            visitor.after_list_element(obj.value)

        return visitor.map(obj, key_result, value_result)

    elif isinstance(obj, Type):
        return visitor.primitive(obj)

    else:
        raise NotImplementedError("Cannot visit non-type: %s" % obj)


def index_by_id(schema_or_type) -> Dict[int, NestedField]:
    class IndexById(SchemaVisitor[Dict[int, NestedField]]):
        def __init__(self):
            self._index: Dict[int, NestedField] = {}

        def schema(self, schema, result):
            return self._index

        def struct(self, struct, results):
            return self._index

        def field(self, field, result):
            self._index[field.field_id] = field
            return self._index

        def list(self, list_type, result):
            self._index[list_type.element.field_id] = list_type.element
            return self._index

        def map(self, map_type, key_result, value_result):
            self._index[map_type.key.field_id] = map_type.key
            self._index[map_type.value.field_id] = map_type.value
            return self._index

        def primitive(self, primitive):
            return self._index

    return visit(schema_or_type, IndexById())


def index_by_name(schema_or_type) -> Dict[str, int]:
    class IndexByName(SchemaVisitor[Dict[str, int]]):
        def __init__(self):
            self._index: Dict[str, int] = {}

        def schema(self, schema, result):
            return self._index

        def struct(self, struct, results):
            return self._index

        def field(self, field, result):
            self._index[field.name] = field.field_id
            return self._index

        def list(self, list_type, result):
            self._index[list_type.element.name] = list_type.element.field_id
            return self._index

        def map(self, map_type, key_result, value_result):
            self._index[map_type.key.name] = map_type.key.field_id
            self._index[map_type.value.name] = map_type.value.field_id
            return self._index

        def primitive(self, primitive):
            return self._index

    return visit(schema_or_type, IndexByName())


def index_name_by_id(schema_or_type) -> Dict[int, str]:
    class IndexByNameById(SchemaVisitor[Dict[int, str]]):
        def __init__(self):
            self._index: Dict[int, str] = {}

        def schema(self, schema, result):
            return self._index

        def struct(self, struct, results):
            return self._index

        def field(self, field, result):
            self._index[field.field_id] = field.name
            return self._index

        def list(self, list_type, result):
            self._index[list_type.element.field_id] = list_type.element.name
            return self._index

        def map(self, map_type, key_result, value_result):
            self._index[map_type.key.field_id] = map_type.key.name
            self._index[map_type.value.field_id] = map_type.value.name
            return self._index

        def primitive(self, primitive):
            return self._index

    return visit(schema_or_type, IndexByNameById())

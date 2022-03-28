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

import sys
from typing import Dict, Generic, Iterable, List, Optional, TypeVar

if sys.version_info >= (3, 8):  # pragma: no cover
    from functools import singledispatchmethod
    from typing import Protocol
else:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore
    from singledispatch import singledispatchmethod  # type: ignore

from iceberg.types import ListType, MapType, NestedField, PrimitiveType, StructType


class Schema(object):
    """A table Schema"""

    def __init__(self, *columns: Iterable[NestedField]):
        self._struct = StructType(*columns)  # type: ignore

    def __str__(self):
        return "table { \n" + "\n".join([" " + str(field) for field in self.columns]) + "\n }"

    def __repr__(self):
        return f"Schema(fields={repr(self.columns)})"

    @property
    def columns(self):
        return self._struct.fields

    def as_struct(self):
        return self._struct


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

    def schema(self, schema: Schema, struct_result: T) -> Optional[T]:
        return None

    def struct(self, struct: StructType, field_results: List[T]) -> Optional[T]:
        return None

    def field(self, field: NestedField, field_result: T) -> Optional[T]:
        return None

    def list(self, list_type: ListType, element_result: T) -> Optional[T]:
        return None

    def map(self, map_type: MapType, key_result: T, value_result: T) -> Optional[T]:
        return None

    def primitive(self, primitive: PrimitiveType) -> Optional[T]:
        return None


def visit(obj, visitor: SchemaVisitor[Optional[T]]) -> Optional[T]:
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

    elif isinstance(obj, PrimitiveType):
        return visitor.primitive(obj)

    else:
        raise NotImplementedError("Cannot visit non-type: %s" % obj)


def index_by_id(schema_or_type) -> Optional[Dict[int, NestedField]]:
    class IndexById(SchemaVisitor[Optional[Dict[int, NestedField]]]):
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

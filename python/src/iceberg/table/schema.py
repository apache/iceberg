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

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Generic, Iterable, List, TypeVar

from iceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)

T = TypeVar("T")


class Schema:
    """A table Schema"""

    def __init__(self, *columns: Iterable[NestedField], schema_id: int, identifier_field_ids: List[int] = []):
        self._struct = StructType(*columns)  # type: ignore
        self._schema_id = schema_id
        self._identifier_field_ids = identifier_field_ids
        self._name_index: Dict[str, int] = index_by_name(self)
        self._id_index: Dict[int, NestedField] = {}  # Will be lazily set when self.id_index property method is called

    def __str__(self):
        return "table { \n" + "\n".join([" " + str(field) for field in self.columns]) + "\n }"

    def __repr__(self):
        return f"Schema(fields={repr(self.columns)})"

    @property
    def columns(self) -> Iterable[NestedField]:
        return self._struct.fields

    @property
    def id(self) -> int:
        return self._schema_id

    @property
    def identifier_field_ids(self) -> List[int]:
        return self._identifier_field_ids

    @property
    def id_index(self) -> Dict[int, NestedField]:
        if not self._id_index:
            self._id_index = index_by_id(self)
        return self._id_index

    @property
    def name_index(self) -> Dict[str, int]:
        return self._name_index

    def as_struct(self) -> StructType:
        return self._struct

    def find_field(self, name_or_id: str | int, case_sensitive: bool = True) -> NestedField:
        if isinstance(name_or_id, int):
            return self.id_index[name_or_id]
        if case_sensitive:
            field_id = self.name_index[name_or_id]
        else:
            name_index_lower = {name.lower(): field_id for name, field_id in self.name_index.items()}
            field_id = name_index_lower[name_or_id.lower()]
        return self.id_index[field_id]

    def find_type(self, name_or_id: str | int, case_sensitive: bool = True) -> IcebergType:
        if isinstance(name_or_id, int):
            return self.id_index[name_or_id].type
        if case_sensitive:
            field_id = self.name_index[name_or_id]
        else:
            name_index_lower = {name.lower(): field_id for name, field_id in self.name_index.items()}
            field_id = name_index_lower[name_or_id.lower()]
        return self.id_index[field_id].type

    def find_column_name(self, column_id: int) -> str:
        matched_column = [name for name, field_id in self.name_index.items() if field_id == column_id]
        if not matched_column:
            raise ValueError(f"Cannot find column name: {column_id}")
        return matched_column[0]

    def select(self, names: List[str], case_sensitive: bool = True) -> "Schema":
        if case_sensitive:
            return self._case_sensitive_select(schema=self, names=names)
        return self._case_insensitive_select(schema=self, names=names)

    @classmethod
    def _case_sensitive_select(cls, schema: "Schema", names: List[str]):
        # TODO: Add a PruneColumns schema visitor and use it here
        raise NotImplementedError()

    @classmethod
    def _case_insensitive_select(cls, schema: "Schema", names: List[str]):
        # TODO: Add a PruneColumns schema visitor and use it here
        raise NotImplementedError()


class SchemaVisitor(Generic[T], ABC):
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

    @abstractmethod
    def schema(self, schema: Schema, struct_result: T) -> T:
        ...

    @abstractmethod
    def struct(self, struct: StructType, field_results: List[T]) -> T:
        ...

    @abstractmethod
    def field(self, field: NestedField, field_result: T) -> T:
        ...

    @abstractmethod
    def list(self, list_type: ListType, element_result: T) -> T:
        ...

    @abstractmethod
    def map(self, map_type: MapType, key_result: T, value_result: T) -> T:
        ...

    @abstractmethod
    def primitive(self, primitive: PrimitiveType) -> T:
        ...


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

    elif isinstance(obj, PrimitiveType):
        return visitor.primitive(obj)

    else:
        raise NotImplementedError("Cannot visit non-type: %s" % obj)


def index_by_id(schema_or_type) -> Dict[int, NestedField]:
    class IndexById(SchemaVisitor[Dict[int, NestedField]]):
        def __init__(self) -> None:
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
        def __init__(self) -> None:
            self._index: Dict[str, int] = {}
            self._field_names: List[str] = []

        def before_field(self, field: NestedField) -> None:
            self._field_names.append(field.name)

        def after_field(self, field: NestedField) -> None:
            self._field_names.pop()

        def schema(self, schema, struct_result):
            return self._index

        def struct(self, struct, field_results):
            return self._index

        def field(self, field, field_result):
            self._add_field(field.name, field.field_id)

        def list(self, list_type, result):
            self._add_field(list_type.element.name, list_type.element.field_id)

        def map(self, map_type, key_result, value_result):
            self._add_field(map_type.key.name, map_type.key.field_id)
            self._add_field(map_type.value.name, map_type.value.field_id)

        def _add_field(self, name, field_id):
            full_name = name
            if self._field_names:
                full_name = ".".join([".".join(self._field_names), name])

            if full_name in self._index:
                raise ValueError(f"Invalid schema, multiple fields for name {full_name}: {index[full_name]} and {field_id}")
            self._index[full_name] = field_id

        def primitive(self, primitive):
            return self._index

    return visit(schema_or_type, IndexByName())

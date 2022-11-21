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
# pylint: disable=W0511
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property, partial, singledispatch
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import Field, PrivateAttr

from pyiceberg.typedef import StructProtocol
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel

T = TypeVar("T")

INITIAL_SCHEMA_ID = 0


class Schema(IcebergBaseModel):
    """A table Schema

    Example:
        >>> from pyiceberg import schema
        >>> from pyiceberg import types
    """

    type: Literal["struct"] = "struct"
    fields: Tuple[NestedField, ...] = Field(default_factory=tuple)
    schema_id: int = Field(alias="schema-id", default=INITIAL_SCHEMA_ID)
    identifier_field_ids: List[int] = Field(alias="identifier-field-ids", default_factory=list)

    _name_to_id: Dict[str, int] = PrivateAttr()

    def __init__(self, *fields: NestedField, **data):
        if fields:
            data["fields"] = fields
        super().__init__(**data)
        self._name_to_id = index_by_name(self)

    def __str__(self):
        return "table {\n" + "\n".join(["  " + str(field) for field in self.columns]) + "\n}"

    def __repr__(self):
        return f"Schema({', '.join(repr(column) for column in self.columns)}, schema_id={self.schema_id}, identifier_field_ids={self.identifier_field_ids})"

    def __eq__(self, other) -> bool:
        if not other:
            return False

        if not isinstance(other, Schema):
            return False

        if len(self.columns) != len(other.columns):
            return False

        identifier_field_ids_is_equal = self.identifier_field_ids == other.identifier_field_ids
        schema_is_equal = all([lhs == rhs for lhs, rhs in zip(self.columns, other.columns)])

        return identifier_field_ids_is_equal and schema_is_equal

    @property
    def columns(self) -> Tuple[NestedField, ...]:
        """A tuple of the top-level fields"""
        return self.fields

    @cached_property
    def _lazy_id_to_field(self) -> Dict[int, NestedField]:
        """Returns an index of field ID to NestedField instance

        This is calculated once when called for the first time. Subsequent calls to this method will use a cached index.
        """
        return index_by_id(self)

    @cached_property
    def _lazy_name_to_id_lower(self) -> Dict[str, int]:
        """Returns an index of lower-case field names to field IDs

        This is calculated once when called for the first time. Subsequent calls to this method will use a cached index.
        """
        return {name.lower(): field_id for name, field_id in self._name_to_id.items()}

    @cached_property
    def _lazy_id_to_name(self) -> Dict[int, str]:
        """Returns an index of field ID to full name

        This is calculated once when called for the first time. Subsequent calls to this method will use a cached index.
        """
        return index_name_by_id(self)

    @cached_property
    def _lazy_id_to_accessor(self) -> Dict[int, "Accessor"]:
        """Returns an index of field ID to accessor

        This is calculated once when called for the first time. Subsequent calls to this method will use a cached index.
        """
        return build_position_accessors(self)

    def as_struct(self) -> StructType:
        """Returns the schema as a struct"""
        return StructType(*self.fields)

    def find_field(self, name_or_id: Union[str, int], case_sensitive: bool = True) -> NestedField:
        """Find a field using a field name or field ID

        Args:
            name_or_id (str | int): Either a field name or a field ID
            case_sensitive (bool, optional): Whether to perform a case-sensitive lookup using a field name. Defaults to True.

        Raises:
            ValueError: When the value cannot be found

        Returns:
            NestedField: The matched NestedField
        """
        if isinstance(name_or_id, int):
            if name_or_id not in self._lazy_id_to_field:
                raise ValueError(f"Could not find field with id: {name_or_id}")
            return self._lazy_id_to_field[name_or_id]

        if case_sensitive:
            field_id = self._name_to_id.get(name_or_id)
        else:
            field_id = self._lazy_name_to_id_lower.get(name_or_id.lower())

        if field_id is None:
            raise ValueError(f"Could not find field with name {name_or_id}, case_sensitive={case_sensitive}")

        return self._lazy_id_to_field[field_id]

    def find_type(self, name_or_id: Union[str, int], case_sensitive: bool = True) -> IcebergType:
        """Find a field type using a field name or field ID

        Args:
            name_or_id (str | int): Either a field name or a field ID
            case_sensitive (bool, optional): Whether to perform a case-sensitive lookup using a field name. Defaults to True.

        Returns:
            NestedField: The type of the matched NestedField
        """
        field = self.find_field(name_or_id=name_or_id, case_sensitive=case_sensitive)
        if not field:
            raise ValueError(f"Could not find field with name or id {name_or_id}, case_sensitive={case_sensitive}")
        return field.field_type

    @property
    def highest_field_id(self):
        return visit(self.as_struct(), _FindLastFieldId())

    def find_column_name(self, column_id: int) -> Optional[str]:
        """Find a column name given a column ID

        Args:
            column_id (int): The ID of the column

        Returns:
            str: The column name (or None if the column ID cannot be found)
        """
        return self._lazy_id_to_name.get(column_id)

    def accessor_for_field(self, field_id: int) -> "Accessor":
        """Find a schema position accessor given a field ID

        Args:
            field_id (int): The ID of the field

        Raises:
            ValueError: When the value cannot be found

        Returns:
            Accessor: An accessor for the given field ID
        """

        if field_id not in self._lazy_id_to_accessor:
            raise ValueError(f"Could not find accessor for field with id: {field_id}")

        return self._lazy_id_to_accessor[field_id]

    def select(self, *names: str, case_sensitive: bool = True) -> "Schema":
        """Return a new schema instance pruned to a subset of columns

        Args:
            names (List[str]): A list of column names
            case_sensitive (bool, optional): Whether to perform a case-sensitive lookup for each column name. Defaults to True.

        Returns:
            Schema: A new schema with pruned columns

        Raises:
            ValueError: If a column is selected that doesn't exist
        """

        try:
            if case_sensitive:
                ids = {self._name_to_id[name] for name in names}
            else:
                ids = {self._lazy_name_to_id_lower[name.lower()] for name in names}
        except KeyError as e:
            raise ValueError(f"Could not find column: {e}") from e

        return prune_columns(self, ids)


class SchemaVisitor(Generic[T], ABC):
    def before_field(self, field: NestedField) -> None:
        """Override this method to perform an action immediately before visiting a field"""

    def after_field(self, field: NestedField) -> None:
        """Override this method to perform an action immediately after visiting a field"""

    def before_list_element(self, element: NestedField) -> None:
        """Override this method to perform an action immediately before visiting an element within a ListType"""
        self.before_field(element)

    def after_list_element(self, element: NestedField) -> None:
        """Override this method to perform an action immediately after visiting an element within a ListType"""
        self.after_field(element)

    def before_map_key(self, key: NestedField) -> None:
        """Override this method to perform an action immediately before visiting a key within a MapType"""
        self.before_field(key)

    def after_map_key(self, key: NestedField) -> None:
        """Override this method to perform an action immediately after visiting a key within a MapType"""
        self.after_field(key)

    def before_map_value(self, value: NestedField) -> None:
        """Override this method to perform an action immediately before visiting a value within a MapType"""
        self.before_field(value)

    def after_map_value(self, value: NestedField) -> None:
        """Override this method to perform an action immediately after visiting a value within a MapType"""
        self.after_field(value)

    @abstractmethod
    def schema(self, schema: Schema, struct_result: T) -> T:
        """Visit a Schema"""

    @abstractmethod
    def struct(self, struct: StructType, field_results: List[T]) -> T:
        """Visit a StructType"""

    @abstractmethod
    def field(self, field: NestedField, field_result: T) -> T:
        """Visit a NestedField"""

    @abstractmethod
    def list(self, list_type: ListType, element_result: T) -> T:
        """Visit a ListType"""

    @abstractmethod
    def map(self, map_type: MapType, key_result: T, value_result: T) -> T:
        """Visit a MapType"""

    @abstractmethod
    def primitive(self, primitive: PrimitiveType) -> T:
        """Visit a PrimitiveType"""


class PreOrderSchemaVisitor(Generic[T], ABC):
    @abstractmethod
    def schema(self, schema: Schema, struct_result: Callable[[], T]) -> T:
        """Visit a Schema"""

    @abstractmethod
    def struct(self, struct: StructType, field_results: List[Callable[[], T]]) -> T:
        """Visit a StructType"""

    @abstractmethod
    def field(self, field: NestedField, field_result: Callable[[], T]) -> T:
        """Visit a NestedField"""

    @abstractmethod
    def list(self, list_type: ListType, element_result: Callable[[], T]) -> T:
        """Visit a ListType"""

    @abstractmethod
    def map(self, map_type: MapType, key_result: Callable[[], T], value_result: Callable[[], T]) -> T:
        """Visit a MapType"""

    @abstractmethod
    def primitive(self, primitive: PrimitiveType) -> T:
        """Visit a PrimitiveType"""


@dataclass(init=True, eq=True, frozen=True)
class Accessor:
    """An accessor for a specific position in a container that implements the StructProtocol"""

    position: int
    inner: Optional["Accessor"] = None

    def __str__(self):
        return f"Accessor(position={self.position},inner={self.inner})"

    def __repr__(self):
        return self.__str__()

    def get(self, container: StructProtocol) -> Any:
        """Returns the value at self.position in `container`

        Args:
            container(StructProtocol): A container to access at position `self.position`

        Returns:
            Any: The value at position `self.position` in the container
        """
        pos = self.position
        val = container.get(pos)
        inner = self
        while inner.inner:
            inner = inner.inner
            val = val.get(inner.position)

        return val


@singledispatch
def visit(obj, visitor: SchemaVisitor[T]) -> T:
    """A generic function for applying a schema visitor to any point within a schema

    The function traverses the schema in post-order fashion

    Args:
        obj(Schema | IcebergType): An instance of a Schema or an IcebergType
        visitor (SchemaVisitor[T]): An instance of an implementation of the generic SchemaVisitor base class

    Raises:
        NotImplementedError: If attempting to visit an unrecognized object type
    """
    raise NotImplementedError("Cannot visit non-type: %s" % obj)


@visit.register(Schema)
def _(obj: Schema, visitor: SchemaVisitor[T]) -> T:
    """Visit a Schema with a concrete SchemaVisitor"""
    return visitor.schema(obj, visit(obj.as_struct(), visitor))


@visit.register(StructType)
def _(obj: StructType, visitor: SchemaVisitor[T]) -> T:
    """Visit a StructType with a concrete SchemaVisitor"""
    results = []

    for field in obj.fields:
        visitor.before_field(field)
        result = visit(field.field_type, visitor)
        visitor.after_field(field)
        results.append(visitor.field(field, result))

    return visitor.struct(obj, results)


@visit.register(ListType)
def _(obj: ListType, visitor: SchemaVisitor[T]) -> T:
    """Visit a ListType with a concrete SchemaVisitor"""

    visitor.before_list_element(obj.element_field)
    result = visit(obj.element_type, visitor)
    visitor.after_list_element(obj.element_field)

    return visitor.list(obj, result)


@visit.register(MapType)
def _(obj: MapType, visitor: SchemaVisitor[T]) -> T:
    """Visit a MapType with a concrete SchemaVisitor"""
    visitor.before_map_key(obj.key_field)
    key_result = visit(obj.key_type, visitor)
    visitor.after_map_key(obj.key_field)

    visitor.before_map_value(obj.value_field)
    value_result = visit(obj.value_type, visitor)
    visitor.after_list_element(obj.value_field)

    return visitor.map(obj, key_result, value_result)


@visit.register(PrimitiveType)
def _(obj: PrimitiveType, visitor: SchemaVisitor[T]) -> T:
    """Visit a PrimitiveType with a concrete SchemaVisitor"""
    return visitor.primitive(obj)


@singledispatch
def pre_order_visit(obj, visitor: PreOrderSchemaVisitor[T]) -> T:
    """A generic function for applying a schema visitor to any point within a schema

    The function traverses the schema in pre-order fashion. This is a slimmed down version
    compared to the post-order traversal (missing before and after methods), mostly
    because we don't use the pre-order traversal much.

    Args:
        obj(Schema | IcebergType): An instance of a Schema or an IcebergType
        visitor (PreOrderSchemaVisitor[T]): An instance of an implementation of the generic PreOrderSchemaVisitor base class

    Raises:
        NotImplementedError: If attempting to visit an unrecognized object type
    """
    raise NotImplementedError("Cannot visit non-type: %s" % obj)


@pre_order_visit.register(Schema)
def _(obj: Schema, visitor: PreOrderSchemaVisitor[T]) -> T:
    """Visit a Schema with a concrete PreOrderSchemaVisitor"""
    return visitor.schema(obj, lambda: pre_order_visit(obj.as_struct(), visitor))


@pre_order_visit.register(StructType)
def _(obj: StructType, visitor: PreOrderSchemaVisitor[T]) -> T:
    """Visit a StructType with a concrete PreOrderSchemaVisitor"""
    return visitor.struct(
        obj,
        [
            partial(
                lambda field: visitor.field(field, partial(lambda field: pre_order_visit(field.field_type, visitor), field)),
                field,
            )
            for field in obj.fields
        ],
    )


@pre_order_visit.register(ListType)
def _(obj: ListType, visitor: PreOrderSchemaVisitor[T]) -> T:
    """Visit a ListType with a concrete PreOrderSchemaVisitor"""
    return visitor.list(obj, lambda: pre_order_visit(obj.element_type, visitor))


@pre_order_visit.register(MapType)
def _(obj: MapType, visitor: PreOrderSchemaVisitor[T]) -> T:
    """Visit a MapType with a concrete PreOrderSchemaVisitor"""
    return visitor.map(obj, lambda: pre_order_visit(obj.key_type, visitor), lambda: pre_order_visit(obj.value_type, visitor))


@pre_order_visit.register(PrimitiveType)
def _(obj: PrimitiveType, visitor: PreOrderSchemaVisitor[T]) -> T:
    """Visit a PrimitiveType with a concrete PreOrderSchemaVisitor"""
    return visitor.primitive(obj)


class _IndexById(SchemaVisitor[Dict[int, NestedField]]):
    """A schema visitor for generating a field ID to NestedField index"""

    def __init__(self) -> None:
        self._index: Dict[int, NestedField] = {}

    def schema(self, schema: Schema, struct_result) -> Dict[int, NestedField]:
        return self._index

    def struct(self, struct: StructType, field_results) -> Dict[int, NestedField]:
        return self._index

    def field(self, field: NestedField, field_result) -> Dict[int, NestedField]:
        """Add the field ID to the index"""
        self._index[field.field_id] = field
        return self._index

    def list(self, list_type: ListType, element_result) -> Dict[int, NestedField]:
        """Add the list element ID to the index"""
        self._index[list_type.element_field.field_id] = list_type.element_field
        return self._index

    def map(self, map_type: MapType, key_result, value_result) -> Dict[int, NestedField]:
        """Add the key ID and value ID as individual items in the index"""
        self._index[map_type.key_field.field_id] = map_type.key_field
        self._index[map_type.value_field.field_id] = map_type.value_field
        return self._index

    def primitive(self, primitive) -> Dict[int, NestedField]:
        return self._index


def index_by_id(schema_or_type) -> Dict[int, NestedField]:
    """Generate an index of field IDs to NestedField instances

    Args:
        schema_or_type (Schema | IcebergType): A schema or type to index

    Returns:
        Dict[int, NestedField]: An index of field IDs to NestedField instances
    """
    return visit(schema_or_type, _IndexById())


class _IndexByName(SchemaVisitor[Dict[str, int]]):
    """A schema visitor for generating a field name to field ID index"""

    def __init__(self) -> None:
        self._index: Dict[str, int] = {}
        self._short_name_to_id: Dict[str, int] = {}
        self._combined_index: Dict[str, int] = {}
        self._field_names: List[str] = []
        self._short_field_names: List[str] = []

    def before_list_element(self, element: NestedField) -> None:
        """Short field names omit element when the element is a StructType"""
        if not isinstance(element.field_type, StructType):
            self._short_field_names.append(element.name)
        self._field_names.append(element.name)

    def after_list_element(self, element: NestedField) -> None:
        if not isinstance(element.field_type, StructType):
            self._short_field_names.pop()
        self._field_names.pop()

    def before_field(self, field: NestedField) -> None:
        """Store the field name"""
        self._field_names.append(field.name)
        self._short_field_names.append(field.name)

    def after_field(self, field: NestedField) -> None:
        """Remove the last field name stored"""
        self._field_names.pop()
        self._short_field_names.pop()

    def schema(self, schema: Schema, struct_result: Dict[str, int]) -> Dict[str, int]:
        return self._index

    def struct(self, struct: StructType, field_results: List[Dict[str, int]]) -> Dict[str, int]:
        return self._index

    def field(self, field: NestedField, field_result: Dict[str, int]) -> Dict[str, int]:
        """Add the field name to the index"""
        self._add_field(field.name, field.field_id)
        return self._index

    def list(self, list_type: ListType, element_result: Dict[str, int]) -> Dict[str, int]:
        """Add the list element name to the index"""
        self._add_field(list_type.element_field.name, list_type.element_field.field_id)
        return self._index

    def map(self, map_type: MapType, key_result: Dict[str, int], value_result: Dict[str, int]) -> Dict[str, int]:
        """Add the key name and value name as individual items in the index"""
        self._add_field(map_type.key_field.name, map_type.key_field.field_id)
        self._add_field(map_type.value_field.name, map_type.value_field.field_id)
        return self._index

    def _add_field(self, name: str, field_id: int):
        """Add a field name to the index, mapping its full name to its field ID

        Args:
            name (str): The field name
            field_id (int): The field ID

        Raises:
            ValueError: If the field name is already contained in the index
        """
        full_name = name

        if self._field_names:
            full_name = ".".join([".".join(self._field_names), name])

        if full_name in self._index:
            raise ValueError(f"Invalid schema, multiple fields for name {full_name}: {self._index[full_name]} and {field_id}")
        self._index[full_name] = field_id

        if self._short_field_names:
            short_name = ".".join([".".join(self._short_field_names), name])
            self._short_name_to_id[short_name] = field_id

    def primitive(self, primitive) -> Dict[str, int]:
        return self._index

    def by_name(self) -> Dict[str, int]:
        """Returns an index of combined full and short names

        Note: Only short names that do not conflict with full names are included.
        """
        combined_index = self._short_name_to_id.copy()
        combined_index.update(self._index)
        return combined_index

    def by_id(self) -> Dict[int, str]:
        """Returns an index of ID to full names"""
        id_to_full_name = {value: key for key, value in self._index.items()}
        return id_to_full_name


def index_by_name(schema_or_type: Union[Schema, IcebergType]) -> Dict[str, int]:
    """Generate an index of field names to field IDs

    Args:
        schema_or_type (Schema | IcebergType): A schema or type to index

    Returns:
        Dict[str, int]: An index of field names to field IDs
    """
    indexer = _IndexByName()
    visit(schema_or_type, indexer)
    return indexer.by_name()


def index_name_by_id(schema_or_type: Union[Schema, IcebergType]) -> Dict[int, str]:
    """Generate an index of field IDs full field names

    Args:
        schema_or_type (Schema | IcebergType): A schema or type to index

    Returns:
        Dict[str, int]: An index of field IDs to full names
    """
    indexer = _IndexByName()
    visit(schema_or_type, indexer)
    return indexer.by_id()


Position = int


class _BuildPositionAccessors(SchemaVisitor[Dict[Position, Accessor]]):
    """A schema visitor for generating a field ID to accessor index

    Example:
        >>> from pyiceberg.schema import Schema
        >>> from pyiceberg.types import *
        >>> schema = Schema(
        ...     NestedField(field_id=2, name="id", field_type=IntegerType(), required=False),
        ...     NestedField(field_id=1, name="data", field_type=StringType(), required=True),
        ...     NestedField(
        ...         field_id=3,
        ...         name="location",
        ...         field_type=StructType(
        ...             NestedField(field_id=5, name="latitude", field_type=FloatType(), required=False),
        ...             NestedField(field_id=6, name="longitude", field_type=FloatType(), required=False),
        ...         ),
        ...         required=True,
        ...     ),
        ...     schema_id=1,
        ...     identifier_field_ids=[1],
        ... )
        >>> result = build_position_accessors(schema)
        >>> expected = {
        ...     2: Accessor(position=0, inner=None),
        ...     1: Accessor(position=1, inner=None),
        ...     5: Accessor(position=2, inner=Accessor(position=0, inner=None)),
        ...     6: Accessor(position=2, inner=Accessor(position=1, inner=None))
        ... }
        >>> result == expected
        True
    """

    def schema(self, schema: Schema, struct_result: Dict[Position, Accessor]) -> Dict[Position, Accessor]:
        return struct_result

    def struct(self, struct: StructType, field_results: List[Dict[Position, Accessor]]) -> Dict[Position, Accessor]:
        result = {}

        for position, field in enumerate(struct.fields):
            if field_results[position]:
                for inner_field_id, acc in field_results[position].items():
                    result[inner_field_id] = Accessor(position, inner=acc)
            else:
                result[field.field_id] = Accessor(position)

        return result

    def field(self, field: NestedField, field_result: Dict[Position, Accessor]) -> Dict[Position, Accessor]:
        return field_result

    def list(self, list_type: ListType, element_result: Dict[Position, Accessor]) -> Dict[Position, Accessor]:
        return {}

    def map(
        self, map_type: MapType, key_result: Dict[Position, Accessor], value_result: Dict[Position, Accessor]
    ) -> Dict[Position, Accessor]:
        return {}

    def primitive(self, primitive: PrimitiveType) -> Dict[Position, Accessor]:
        return {}


def build_position_accessors(schema_or_type: Union[Schema, IcebergType]) -> Dict[int, Accessor]:
    """Generate an index of field IDs to schema position accessors

    Args:
        schema_or_type (Schema | IcebergType): A schema or type to index

    Returns:
        Dict[int, Accessor]: An index of field IDs to accessors
    """
    return visit(schema_or_type, _BuildPositionAccessors())


class _FindLastFieldId(SchemaVisitor[int]):
    """Traverses the schema to get the highest field-id"""

    def schema(self, schema: Schema, struct_result: int) -> int:
        return struct_result

    def struct(self, struct: StructType, field_results: List[int]) -> int:
        return max(field_results)

    def field(self, field: NestedField, field_result: int) -> int:
        return max(field.field_id, field_result)

    def list(self, list_type: ListType, element_result: int) -> int:
        return element_result

    def map(self, map_type: MapType, key_result: int, value_result: int) -> int:
        return max(key_result, value_result)

    def primitive(self, primitive: PrimitiveType) -> int:
        return 0


def assign_fresh_schema_ids(schema: Schema) -> Schema:
    """Traverses the schema, and sets new IDs"""
    return pre_order_visit(schema, _SetFreshIDs())


class _SetFreshIDs(PreOrderSchemaVisitor[IcebergType]):
    """Traverses the schema and assigns monotonically increasing ids"""

    counter: itertools.count  # type: ignore
    reserved_ids: Dict[int, int]

    def __init__(self, start: int = 1) -> None:
        self.counter = itertools.count(start)
        self.reserved_ids = {}

    def _get_and_increment(self) -> int:
        return next(self.counter)

    def schema(self, schema: Schema, struct_result: Callable[[], StructType]) -> Schema:
        # First we keep the original identifier_field_ids here, we remap afterwards
        fields = struct_result().fields
        return Schema(*fields, identifier_field_ids=[self.reserved_ids[field_id] for field_id in schema.identifier_field_ids])

    def struct(self, struct: StructType, field_results: List[Callable[[], IcebergType]]) -> StructType:
        # assign IDs for this struct's fields first
        self.reserved_ids.update({field.field_id: self._get_and_increment() for field in struct.fields})
        return StructType(*[field() for field in field_results])

    def field(self, field: NestedField, field_result: Callable[[], IcebergType]) -> IcebergType:
        return NestedField(
            field_id=self.reserved_ids[field.field_id],
            name=field.name,
            field_type=field_result(),
            required=field.required,
            doc=field.doc,
        )

    def list(self, list_type: ListType, element_result: Callable[[], IcebergType]) -> ListType:
        self.reserved_ids[list_type.element_id] = self._get_and_increment()
        return ListType(
            element_id=self.reserved_ids[list_type.element_id],
            element=element_result(),
            element_required=list_type.element_required,
        )

    def map(self, map_type: MapType, key_result: Callable[[], IcebergType], value_result: Callable[[], IcebergType]) -> MapType:
        self.reserved_ids[map_type.key_id] = self._get_and_increment()
        self.reserved_ids[map_type.value_id] = self._get_and_increment()
        return MapType(
            key_id=self.reserved_ids[map_type.key_id],
            key_type=key_result(),
            value_id=self.reserved_ids[map_type.value_id],
            value_type=value_result(),
            value_required=map_type.value_required,
        )

    def primitive(self, primitive: PrimitiveType) -> PrimitiveType:
        return primitive


def prune_columns(schema: Schema, selected: Set[int], select_full_types: bool = True) -> Schema:
    result = visit(schema.as_struct(), _PruneColumnsVisitor(selected, select_full_types))
    return Schema(
        *(result or StructType()).fields,
        schema_id=schema.schema_id,
        identifier_field_ids=list(selected.intersection(schema.identifier_field_ids)),
    )


class _PruneColumnsVisitor(SchemaVisitor[Optional[IcebergType]]):
    selected: Set[int]
    select_full_types: bool

    def __init__(self, selected: Set[int], select_full_types: bool):
        self.selected = selected
        self.select_full_types = select_full_types

    def schema(self, schema: Schema, struct_result: Optional[IcebergType]) -> Optional[IcebergType]:
        return struct_result

    def struct(self, struct: StructType, field_results: List[Optional[IcebergType]]) -> Optional[IcebergType]:
        fields = struct.fields
        selected_fields = []
        same_type = True

        for idx, projected_type in enumerate(field_results):
            field = fields[idx]
            if field.field_type == projected_type:
                selected_fields.append(field)
            elif projected_type is not None:
                same_type = False
                # Type has changed, create a new field with the projected type
                selected_fields.append(
                    NestedField(
                        field_id=field.field_id,
                        name=field.name,
                        field_type=projected_type,
                        doc=field.doc,
                        required=field.required,
                    )
                )

        if selected_fields:
            if len(selected_fields) == len(fields) and same_type is True:
                # Nothing has changed, and we can return the original struct
                return struct
            else:
                return StructType(*selected_fields)
        return None

    def field(self, field: NestedField, field_result: Optional[IcebergType]) -> Optional[IcebergType]:
        if field.field_id in self.selected:
            if self.select_full_types:
                return field.field_type
            elif field.field_type.is_struct:
                return self._project_selected_struct(field_result)
            else:
                if not field.field_type.is_primitive:
                    raise ValueError(
                        f"Cannot explicitly project List or Map types, {field.field_id}:{field.name} of type {field.field_type} was selected"
                    )
                # Selected non-struct field
                return field.field_type
        elif field_result is not None:
            # This field wasn't selected but a subfield was so include that
            return field_result
        else:
            return None

    def list(self, list_type: ListType, element_result: Optional[IcebergType]) -> Optional[IcebergType]:
        if list_type.element_id in self.selected:
            if self.select_full_types:
                return list_type
            elif list_type.element_type and list_type.element_type.is_struct:
                projected_struct = self._project_selected_struct(element_result)
                return self._project_list(list_type, projected_struct)
            else:
                if not list_type.element_type.is_primitive:
                    raise ValueError(
                        f"Cannot explicitly project List or Map types, {list_type.element_id} of type {list_type.element_type} was selected"
                    )
                return list_type
        elif element_result is not None:
            return self._project_list(list_type, element_result)
        else:
            return None

    def map(
        self, map_type: MapType, key_result: Optional[IcebergType], value_result: Optional[IcebergType]
    ) -> Optional[IcebergType]:
        if map_type.value_id in self.selected:
            if self.select_full_types:
                return map_type
            elif map_type.value_type and map_type.value_type.is_struct:
                projected_struct = self._project_selected_struct(value_result)
                return self._project_map(map_type, projected_struct)
            if not map_type.value_type.is_primitive:
                raise ValueError(
                    f"Cannot explicitly project List or Map types, Map value {map_type.value_id} of type {map_type.value_type} was selected"
                )
            return map_type
        elif value_result is not None:
            return self._project_map(map_type, value_result)
        elif map_type.key_id in self.selected:
            return map_type
        return None

    def primitive(self, primitive: PrimitiveType) -> Optional[IcebergType]:
        return None

    @staticmethod
    def _project_selected_struct(projected_field: Optional[IcebergType]) -> StructType:
        if projected_field and not isinstance(projected_field, StructType):
            raise ValueError("Expected a struct")

        if projected_field is None:
            return StructType()
        else:
            return projected_field

    @staticmethod
    def _project_list(list_type: ListType, element_result: IcebergType):
        if list_type.element_type == element_result:
            return list_type
        else:
            return ListType(
                element_id=list_type.element_id, element_type=element_result, element_required=list_type.element_required
            )

    @staticmethod
    def _project_map(map_type: MapType, value_result: IcebergType):
        if map_type.value_type == value_result:
            return map_type
        else:
            return MapType(
                key_id=map_type.key_id,
                value_id=map_type.value_id,
                key_type=map_type.key_type,
                value_type=value_result,
                value_required=map_type.value_required,
            )

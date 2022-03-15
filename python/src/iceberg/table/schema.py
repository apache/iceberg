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
from typing import Iterable, Tuple

if sys.version_info >= (3, 8):  # pragma: no cover
    from functools import singledispatchmethod
    from typing import Protocol
else:  # pragma: no cover
    from typing_extensions import Protocol  # type: ignore
    from singledispatch import singledispatchmethod  # type: ignore

from iceberg.types import IcebergType, ListType, MapType, NestedField, StructType


class SchemaVisitor(Protocol):
    def visit(self, node) -> None:  # pragma: no cover
        ...


class Schema:
    """Schema of a table

    Example:
        >>> from iceberg.table.schema import Schema
        >>> from iceberg.types import BooleanType, IntegerType, NestedField, StringType
        >>> fields = [
            NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
            NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
            NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        ]
        >>> table_schema = Schema(fields=fields, schema_id=1, aliases={"qux": 3})
        >>> print(table_schema)
        1: name=foo, type=string, required=True
        2: name=bar, type=int, required=False
        3: name=baz, type=boolean, required=True
    """

    def __init__(self, fields: Iterable[NestedField], schema_id: int, aliases: dict = {}):
        self._struct = StructType(*fields)
        self._schema_id = schema_id
        self._aliases = aliases

        index_by_id_visitor = IndexById()
        index_by_id_visitor.visit(self._struct)
        self._index_by_id = index_by_id_visitor.result

        index_by_name_visitor = IndexByName()
        index_by_name_visitor.visit(self._struct)
        self._index_by_name = index_by_name_visitor.result

    def __str__(self):
        schema_str = ""
        for field in self.fields:
            schema_str += f"{field.field_id}: name={field.name}, type={field.type}, required={field.is_required}\n"
        return schema_str.rstrip()

    def __repr__(self):
        return f"Schema(fields={repr(self.fields)}, schema_id={self.schema_id})"

    @property
    def fields(self) -> Tuple:
        return self._struct.fields

    @property
    def schema_id(self) -> int:
        return self._schema_id

    @property
    def struct(self) -> StructType:
        return self._struct

    def find_field_id_by_name(self, field_name: str, case_sensitive: bool = True) -> int:
        """Get a field ID for a given field name

        Args:
            field_name (str): A field name
            case_sensitive (bool): If False, case will not be considered when retrieving the field from the schema--default is True

        Returns:
            int: The field ID for the field name
        """
        if case_sensitive:
            for indexed_field_name, indexed_field_id in self._index_by_name.items():
                if indexed_field_name == field_name:
                    return indexed_field_id
        if not case_sensitive:
            for indexed_field_name, indexed_field_id in self._index_by_name.items():
                if indexed_field_name.lower() == field_name.lower():
                    return indexed_field_id
        raise ValueError(f"Cannot get field ID, name not found: {field_name}")

    def find_field_id_by_alias(self, field_alias: str, case_sensitive: bool = True) -> int:
        """Get a field ID for a given field alias

        Args:
            field_alias (str): A field alias
            case_sensitive (bool): If False, case will not be considered when retrieving the field from the schema--default is True

        Returns:
            int: The field ID for the field alias

        raises:
            ValueError: If the field ID cannot be retrieved either because the alias was not found or a case-insensitive
                match returned multiple results
        """
        if case_sensitive:
            try:
                # For case-sensitive, just try looking up the alias and raise if it's not found
                return self._aliases[field_alias]
            except KeyError:
                raise ValueError(f"Cannot get field ID, alias not found: {field_alias}")
        if not case_sensitive:
            matching_fields = [value for key, value in self._aliases.items() if key.lower() == field_alias.lower()]
            if len(matching_fields) == 1:  # If one matching alias found, return the corresponding ID
                return matching_fields[0]
            elif not matching_fields:  # If no matching fields, raise
                raise ValueError(f"Cannot get field ID, alias not found: {field_alias}")

            # If multiple IDs are returned for a case-insensitive alias lookup, raise
            raise ValueError(f"Cannot get field ID, case-insensitive alias returns multiple results: {field_alias}")

    def find_field_name_by_field_id(self, field_id: int) -> str:
        """Find a field name for a given field ID

        Args:
            field_id (int): A field ID

        Returns:
            str: The field name for the field ID

        Raises:
            ValueError: If the field ID does not exist
        """
        for indexed_field_name, indexed_field_id in self._index_by_name.items():
            if indexed_field_id == field_id:
                return indexed_field_name
        raise ValueError(f"Cannot get field name, field ID not found: {field_id}")

    def find_field_by_id(self, field_id: int) -> NestedField:
        """Find a field by it's field ID

        Args:
            field_id (int): A field ID

        Returns:
            NestedField: The field object for the given field ID

        Raise:
            ValueError: If the field ID does not exist
        """
        for field in self.fields:
            if field.field_id == field_id:
                return field
        raise ValueError(f"Cannot get field, ID does not exist: {field_id}")

    def find_field_type_by_id(self, field_id: int) -> IcebergType:
        """Find the type of a field by field ID

        Args:
            field_id (int): The ID of the field

        Raises:
            ValueError: If the field ID does not exist in this schema

        Returns
            IcebergType: The type of the field with ID of `field_id`
        """
        field = self.find_field_by_id(field_id)
        return field.type


class IndexById(SchemaVisitor):
    """Index a Schema by IDs

    This visitor provides a field ID to field name map for a given Schema instance. The result is stored in the `result` instance attribute.

    Example:
        >>> from iceberg.table.schema import IndexById, Schema
        >>> from iceberg.types import BooleanType, IntegerType, NestedField, StringType
        >>> fields = [
            NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
            NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
            NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        ]
        >>> table_schema = Schema(fields=fields, schema_id=1)
        >>> visitor = IndexById()
        >>> visitor.visit(table_schema)
        >>> print(visitor.result)
        {1: 'foo', 2: 'bar', 3: 'baz'}
    """

    def __init__(self):
        self.result = {}

    @singledispatchmethod
    def visit(self, node) -> None:
        """A generic single dispatch visit method

        Raises:
            NotImplementedError: If no concrete method has been registered for type of the `node` argument
        """
        raise NotImplementedError(f"Cannot visit node, no IndexById operation implemented for node type: {type(node)}")

    @visit.register
    def _(self, node: Schema) -> None:
        self.visit(node.struct)

    @visit.register
    def _(self, node: StructType) -> None:
        for field in node.fields:
            self.visit(field)

    @visit.register
    def _(self, node: NestedField) -> None:
        self.result[node.field_id] = node.name
        if isinstance(node.type, (ListType, MapType)):
            self.visit(node.type, nested_field_name=node.name)

    @visit.register
    def _(self, node: ListType, nested_field_name: str) -> None:
        """Index a ListType node

        ListType nodes add one item to the index:
            1. The ID for the ListType element and the name <nested_field_name>.<element_field_name>

        <element_field_name> is always "element".
        Args:
            node (ListType): The ListType instance for the NestedField instance
            nested_field_name (str): The name of the NestedField instance containing the ListType
        """
        self.result[node.element.field_id] = f"{nested_field_name}.{node.element.name}"

    @visit.register
    def _(self, node: MapType, nested_field_name: str) -> None:
        """Index a MapType node

        MapType nodes add two items to the index:
            1. The MapType key ID and the name <nested_field_name>.<key_field_name>
            2. The MapType value ID and the name <nested_field_name>.<value_field_name>

        <key_field_name> and <value_field_name> are always "key" and "value", respectively.

        Args:
            node (MapType): The MapType instance for the NestedField instance
            nested_field_name (str): The name of the NestedField instance containing the MapType
        """
        self.result[node.key.field_id] = f"{nested_field_name}.key"
        self.result[node.value.field_id] = f"{nested_field_name}.value"


class IndexByName(SchemaVisitor):
    """Index a Schema by names

    This visitor provides a field name to field ID map for a given Schema instance. The result is stored in the `result` instance attribute.

    Example:
        >>> from iceberg.table.schema import IndexByName, Schema
        >>> from iceberg.types import BooleanType, IntegerType, NestedField, StringType
        >>> fields = [
            NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
            NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
            NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        ]
        >>> table_schema = Schema(fields=fields, schema_id=1)
        >>> visitor = IndexByName()
        >>> visitor.visit(table_schema)
        >>> print(visitor.result)
        {'foo': 1, 'bar': 2, 'baz': 3}
    """

    def __init__(self):
        self.result = {}

    @singledispatchmethod
    def visit(self, node) -> None:
        """A generic single dispatch visit method

        Raises:
            NotImplementedError: If no concrete method has been registered for type of the `node` argument
        """
        raise NotImplementedError(f"Cannot visit node, no IndexByName operation implemented for node type: {type(node)}")

    @visit.register
    def _(self, node: Schema) -> None:
        self.visit(node.struct)

    @visit.register
    def _(self, node: StructType) -> None:
        for field in node.fields:
            self.visit(field)

    @visit.register
    def _(self, node: NestedField) -> None:
        self.result[node.name] = node.field_id
        if isinstance(node.type, (ListType, MapType)):
            self.visit(node.type, nested_field_name=node.name)

    @visit.register
    def _(self, node: ListType, nested_field_name: str) -> None:
        """Index a ListType node

        ListType nodes add one item to the index:
            1. The name <nested_field_name>.<element_field_name> and the ID for the ListType element

        <element_field_name> is always "element".
        Args:
            node (ListType): The ListType instance for the NestedField instance
            nested_field_name (str): The name of the NestedField instance containing the ListType
        """
        self.result[f"{nested_field_name}.{node.element.name}"] = node.element.field_id

    @visit.register
    def _(self, node: MapType, nested_field_name: str) -> None:
        """Index a MapType node

        MapType nodes add two items to the index:
            1. The name <nested_field_name>.<key_field_name> and the MapType key ID
            2. The name <nested_field_name>.<value_field_name> and the MapType value ID

        <key_field_name> and <value_field_name> are always "key" and "value", respectively.

        Args:
            node (MapType): The MapType instance for the NestedField instance
            nested_field_name (str): The name of the NestedField instance containing the MapType
        """
        self.result[f"{nested_field_name}.{node.key.name}"] = node.key.field_id
        self.result[f"{nested_field_name}.{node.value.name}"] = node.value.field_id

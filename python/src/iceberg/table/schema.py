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

from enum import Enum, auto
from typing import Iterable

try:
    from typing import Literal
except ImportError:  # pragma: no cover
    from typing_extensions import Literal  # type: ignore

from iceberg.types import NestedField, StructType


class FIELD_IDENTIFIER_TYPES(Enum):
    NAME = auto()
    ALIAS = auto()


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

    def __str__(self):
        schema_str = ""
        for field in self.fields:
            schema_str += f"{field.field_id}: name={field.name}, type={field.type}, required={field.is_required}\n"
        return schema_str.rstrip()

    def __repr__(self):
        return f"Schema(fields={repr(self.fields)}, schema_id={self.schema_id})"

    @property
    def fields(self):
        return self._struct.fields

    @property
    def schema_id(self):
        return self._schema_id

    @property
    def struct(self):
        return self._struct

    def _get_field_identifier_by_name(self, field_identifier: str, case_sensitive: bool):
        """Get a field ID for a given field name

        Args:
            field_identifier (str): A field name
            case_sensitive (bool): If False, case will not be considered when retrieving the field from the schema

        Returns:
            int: The field ID for the field name
        """
        if case_sensitive:
            for field in self.fields:
                if field.name == field_identifier:
                    return field.field_id
        else:
            for field in self.fields:
                if field.name.lower() == field_identifier.lower():
                    return field.field_id
        raise ValueError(f"Cannot get field ID, name not found: {field_identifier}")

    def _get_field_identifier_by_alias(self, field_identifier: str, case_sensitive: bool):
        """Get a field ID for a given field alias

        Args:
            field_identifier (str): A field alias
            case_sensitive (bool): If False, case will not be considered when retrieving the field from the schema

        Returns:
            int: The field ID for the field alias
        """
        if case_sensitive:
            try:
                # For case-sensitive, just try looking up the alias and raise if it's not found
                return self._aliases[field_identifier]
            except KeyError:
                raise ValueError(f"Cannot get field ID, alias not found: {field_identifier}")
        if not case_sensitive:
            matching_fields = [value for key, value in self._aliases.items() if key.lower() == field_identifier.lower()]
            if len(matching_fields) == 1:  # If one matching alias found, return the corresponding ID
                return matching_fields[0]
            elif not matching_fields:  # If no matching fields, raise
                raise ValueError(f"Cannot get field ID, alias not found: {field_identifier}")

            # If multiple IDs are returned for a case-insensitive alias lookup, raise
            raise ValueError(f"Cannot get field ID, case-insensitive alias returns multiple results: {field_identifier}")

    def get_field_id(
        self,
        field_identifier: str,
        field_identifier_type: Literal[FIELD_IDENTIFIER_TYPES.NAME, FIELD_IDENTIFIER_TYPES.ALIAS],
        case_sensitive: bool = True,
    ) -> int:
        """Get a field ID for a given NAME or ALIAS

        This calls either the `_get_field_identifier_by_name` method or the `_get_field_identifier_by_alias` method depending on the value of the `field_identifier_type` argument.

        Args:
            field_identifier (str): The unique identifier for the field
            field_identifier_type (FIELD_IDENTIFIER_TYPES): An FIELD_IDENTIFIER_TYPES value of either NAME or ALIAS
            case_sensitive (bool): If False, case will not be considered when retrieving the field from the schema. Default is True

        Raises:
            ValueError: If the field identifier does not exist in the schema

        Returns
            int: The field ID for the given field identifier
        """
        if field_identifier_type == FIELD_IDENTIFIER_TYPES.NAME:
            return self._get_field_identifier_by_name(field_identifier=field_identifier, case_sensitive=case_sensitive)
        elif field_identifier_type == FIELD_IDENTIFIER_TYPES.ALIAS:
            return self._get_field_identifier_by_alias(field_identifier=field_identifier, case_sensitive=case_sensitive)

    def get_field(self, field_id: int) -> NestedField:
        """Get a field object for a given field ID

        This returns the NestedField instance for the given `field_id`.

        Args:
            field_id (int): The ID of the field

        Raises:
            ValueError: If the field ID does not exist in this schema

        Returns
            NestedField: The field object for the given field ID
        """
        for field in self.fields:
            if field.field_id == field_id:
                return field
        raise ValueError(f"Cannot get field, ID does not exist: {field_id}")

    def get_type(self, field_id: int):
        """Get the type of a field by field ID

        Args:
            field_id (int): The ID of the field

        Raises:
            ValueError: If the field ID does not exist in this schema

        Returns
            IcebergType: The type of the field with ID of `field_id`
        """
        field = self.get_field(field_id)
        return field.type

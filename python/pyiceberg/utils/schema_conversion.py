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
"""Utility class for converting between Avro and Iceberg schemas."""
from __future__ import annotations

import logging
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Union,
)

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    UUIDType,
)

logger = logging.getLogger(__name__)

PRIMITIVE_FIELD_TYPE_MAPPING: dict[str, PrimitiveType] = {
    "boolean": BooleanType(),
    "bytes": BinaryType(),
    "double": DoubleType(),
    "float": FloatType(),
    "int": IntegerType(),
    "long": LongType(),
    "string": StringType(),
    "enum": StringType(),
}

LOGICAL_FIELD_TYPE_MAPPING: dict[tuple[str, str], PrimitiveType] = {
    ("date", "int"): DateType(),
    ("time-millis", "int"): TimeType(),
    ("timestamp-millis", "long"): TimestampType(),
    ("time-micros", "int"): TimeType(),
    ("timestamp-micros", "long"): TimestampType(),
    ("uuid", "fixed"): UUIDType(),
}


class AvroSchemaConversion:
    def avro_to_iceberg(self, avro_schema: dict[str, Any]) -> Schema:
        """Converts an Apache Avro into an Apache Iceberg schema equivalent.

        This expects to have field id's to be encoded in the Avro schema:

            {
                "type": "record",
                "name": "manifest_file",
                "fields": [
                    {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
                    {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501}
                ]
            }

        Example:
            This converts an Avro schema into an Iceberg schema:

            >>> avro_schema = AvroSchemaConversion().avro_to_iceberg({
            ...     "type": "record",
            ...     "name": "manifest_file",
            ...     "fields": [
            ...         {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
            ...         {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501}
            ...     ]
            ... })
            >>> iceberg_schema = Schema(
            ...     NestedField(
            ...         field_id=500, name="manifest_path", field_type=StringType(), required=False, doc="Location URI with FS scheme"
            ...     ),
            ...     NestedField(
            ...         field_id=501, name="manifest_length", field_type=LongType(), required=False, doc="Total file size in bytes"
            ...     ),
            ...     schema_id=1
            ... )
            >>> avro_schema == iceberg_schema
            True

        Args:
            avro_schema (Dict[str, Any]): The JSON decoded Avro schema.

        Returns:
            Equivalent Iceberg schema.
        """
        return Schema(*[self._convert_field(field) for field in avro_schema["fields"]], schema_id=1)

    def _resolve_union(
        self, type_union: Union[Dict[str, str], List[Union[str, Dict[str, str]]], str]
    ) -> Tuple[Union[str, Dict[str, Any]], bool]:
        """
        Converts Unions into their type and resolves if the field is required.

        Examples:
            >>> AvroSchemaConversion()._resolve_union('str')
            ('str', True)
            >>> AvroSchemaConversion()._resolve_union(['null', 'str'])
            ('str', False)
            >>> AvroSchemaConversion()._resolve_union([{'type': 'str'}])
            ({'type': 'str'}, True)
            >>> AvroSchemaConversion()._resolve_union(['null', {'type': 'str'}])
            ({'type': 'str'}, False)

        Args:
            type_union: The field, can be a string 'str', list ['null', 'str'], or dict {"type": 'str'}.

        Returns:
            A tuple containing the type and if required.

        Raises:
            TypeError: In the case non-optional union types are encountered.
        """
        avro_types: Union[Dict[str, str], List[Union[Dict[str, str], str]]]
        if isinstance(type_union, str):
            # It is a primitive and required
            return type_union, True
        elif isinstance(type_union, dict):
            # It is a context and required
            return type_union, True
        else:
            avro_types = type_union

        if len(avro_types) > 2:
            raise TypeError(f"Non-optional types aren't part of the Iceberg specification: {avro_types}")

        # For the Iceberg spec it is required to set the default value to null
        # From https://iceberg.apache.org/spec/#avro
        # Optional fields must always set the Avro field default value to null.
        #
        # This means that null has to come first:
        # https://avro.apache.org/docs/current/spec.html
        # type of the default value must match the first element of the union.
        if "null" != avro_types[0]:
            raise TypeError("Only null-unions are supported")

        # Filter the null value and return the type
        return list(filter(lambda t: t != "null", avro_types))[0], False

    def _convert_schema(self, avro_type: Union[str, Dict[str, Any]]) -> IcebergType:
        """
        Resolves the Avro type.

        Args:
            avro_type: The Avro type, can be simple or complex.

        Returns:
            The equivalent IcebergType.

        Raises:
            ValueError: When there are unknown types
        """
        if isinstance(avro_type, str) and avro_type in PRIMITIVE_FIELD_TYPE_MAPPING:
            return PRIMITIVE_FIELD_TYPE_MAPPING[avro_type]
        elif isinstance(avro_type, dict):
            if "logicalType" in avro_type:
                return self._convert_logical_type(avro_type)
            else:
                # Resolve potential nested types
                while "type" in avro_type and isinstance(avro_type["type"], dict):
                    avro_type = avro_type["type"]
                type_identifier = avro_type["type"]
                if type_identifier == "record":
                    return self._convert_record_type(avro_type)
                elif type_identifier == "array":
                    return self._convert_array_type(avro_type)
                elif type_identifier == "map":
                    return self._convert_map_type(avro_type)
                elif type_identifier == "fixed":
                    return self._convert_fixed_type(avro_type)
                elif isinstance(type_identifier, str) and type_identifier in PRIMITIVE_FIELD_TYPE_MAPPING:
                    return PRIMITIVE_FIELD_TYPE_MAPPING[type_identifier]
                else:
                    raise TypeError(f"Unknown type: {avro_type}")
        else:
            raise TypeError(f"Unknown type: {avro_type}")

    def _convert_field(self, field: dict[str, Any]) -> NestedField:
        """
        Converts an Avro field into an Iceberg equivalent field.
        Args:
            field: The Avro field.

        Returns:
            The Iceberg equivalent field.
        """
        if "field-id" not in field:
            raise ValueError(f"Cannot convert field, missing field-id: {field}")

        plain_type, required = self._resolve_union(field["type"])

        return NestedField(
            field_id=field["field-id"],
            name=field["name"],
            field_type=self._convert_schema(plain_type),
            required=required,
            doc=field.get("doc"),
        )

    def _convert_record_type(self, record_type: dict[str, Any]) -> StructType:
        """
        Converts the fields from a record into an Iceberg struct.

        Examples:
            >>> from pyiceberg.utils.schema_conversion import AvroSchemaConversion
            >>> record_type = {
            ...     "type": "record",
            ...     "name": "r508",
            ...     "fields": [{
            ...         "name": "contains_null",
            ...         "type": "boolean",
            ...         "doc": "True if any file has a null partition value",
            ...         "field-id": 509,
            ...      }, {
            ...          "name": "contains_nan",
            ...          "type": ["null", "boolean"],
            ...          "doc": "True if any file has a nan partition value",
            ...          "default": None,
            ...          "field-id": 518,
            ...      }],
            ... }
            >>> actual = AvroSchemaConversion()._convert_record_type(record_type)
            >>> expected = StructType(
            ...     fields=(
            ...         NestedField(
            ...             field_id=509,
            ...             name="contains_null",
            ...             field_type=BooleanType(),
            ...             required=False,
            ...             doc="True if any file has a null partition value",
            ...         ),
            ...         NestedField(
            ...             field_id=518,
            ...             name="contains_nan",
            ...             field_type=BooleanType(),
            ...             required=True,
            ...             doc="True if any file has a nan partition value",
            ...         ),
            ...     )
            ... )
            >>> expected == actual
            True

        Args:
            record_type: The record type itself.

        Returns: A StructType.
        """
        if record_type["type"] != "record":
            raise ValueError(f"Expected record type, got: {record_type}")

        return StructType(*[self._convert_field(field) for field in record_type["fields"]])

    def _convert_array_type(self, array_type: dict[str, Any]) -> ListType:
        if "element-id" not in array_type:
            raise ValueError(f"Cannot convert array-type, missing element-id: {array_type}")

        plain_type, element_required = self._resolve_union(array_type["items"])

        return ListType(
            element_id=array_type["element-id"],
            element_type=self._convert_schema(plain_type),
            element_required=element_required,
        )

    def _convert_map_type(self, map_type: dict[str, Any]) -> MapType:
        """
        Converts dict to MapType.

        Args:
            map_type: The dict that describes the Avro map type.

        Examples:
            >>> from pyiceberg.utils.schema_conversion import AvroSchemaConversion
            >>> avro_field = {
            ...     "type": "map",
            ...     "values": ["null", "long"],
            ...     "key-id": 101,
            ...     "value-id": 102,
            ... }
            >>> actual = AvroSchemaConversion()._convert_map_type(avro_field)
            >>> expected = MapType(
            ...     key_id=101,
            ...     key_type=StringType(),
            ...     value_id=102,
            ...     value_type=LongType(),
            ...     value_required=True
            ... )
            >>> actual == expected
            True

        Returns: A MapType.
        """
        value_type, value_required = self._resolve_union(map_type["values"])
        return MapType(
            key_id=map_type["key-id"],
            # Avro only supports string keys
            key_type=StringType(),
            value_id=map_type["value-id"],
            value_type=self._convert_schema(value_type),
            value_required=value_required,
        )

    def _convert_logical_type(self, avro_logical_type: dict[str, Any]) -> IcebergType:
        """
        Convert a schema with a logical type annotation.
        For the decimal and map we need to fetch more keys from the dict, and for the simple ones we can just
        look it up in the mapping.

        Examples:
            >>> from pyiceberg.utils.schema_conversion import AvroSchemaConversion
            >>> avro_logical_type = {
            ...     "type": "int",
            ...     "logicalType": "date"
            ... }
            >>> actual = AvroSchemaConversion()._convert_logical_type(avro_logical_type)
            >>> actual == DateType()
            True

        Args:
            avro_logical_type: The logical type.

        Returns:
            The converted logical type.

        Raises:
            ValueError: When the logical type is unknown.
        """
        logical_type = avro_logical_type["logicalType"]
        physical_type = avro_logical_type["type"]
        if logical_type == "decimal":
            return self._convert_logical_decimal_type(avro_logical_type)
        elif logical_type == "map":
            return self._convert_logical_map_type(avro_logical_type)
        elif (logical_type, physical_type) in LOGICAL_FIELD_TYPE_MAPPING:
            return LOGICAL_FIELD_TYPE_MAPPING[(logical_type, physical_type)]
        else:
            raise ValueError(f"Unknown logical/physical type combination: {avro_logical_type}")

    def _convert_logical_decimal_type(self, avro_type: dict[str, Any]) -> DecimalType:
        """
        Converts Avro type to Iceberg DecimalType.

        Args:
            avro_type: The Avro type.

        Examples:
            >>> from pyiceberg.utils.schema_conversion import AvroSchemaConversion
            >>> avro_decimal_type = {
            ...     "type": "bytes",
            ...     "logicalType": "decimal",
            ...     "precision": 19,
            ...     "scale": 25
            ... }
            >>> actual = AvroSchemaConversion()._convert_logical_decimal_type(avro_decimal_type)
            >>> expected = DecimalType(
            ...     precision=19,
            ...     scale=25
            ... )
            >>> actual == expected
            True

        Returns:
            A Iceberg DecimalType.
        """
        return DecimalType(precision=avro_type["precision"], scale=avro_type["scale"])

    def _convert_logical_map_type(self, avro_type: dict[str, Any]) -> MapType:
        """
        In the case where a map hasn't a key as a type you can use a logical map to still encode this in Avro.

        Args:
            avro_type: The Avro Type.

        Examples:
            >>> from pyiceberg.utils.schema_conversion import AvroSchemaConversion
            >>> avro_type = {
            ...     "type": "array",
            ...     "logicalType": "map",
            ...     "items": {
            ...         "type": "record",
            ...         "name": "k101_v102",
            ...         "fields": [
            ...             {"name": "key", "type": "int", "field-id": 101},
            ...             {"name": "value", "type": "string", "field-id": 102},
            ...         ],
            ...     },
            ... }
            >>> actual = AvroSchemaConversion()._convert_logical_map_type(avro_type)
            >>> expected = MapType(
            ...         key_id=101,
            ...         key_type=IntegerType(),
            ...         value_id=102,
            ...         value_type=StringType(),
            ...         value_required=False
            ... )
            >>> actual == expected
            True

        .. _Apache Iceberg specification:
            https://iceberg.apache.org/spec/#appendix-a-format-specific-requirements

        Returns:
            The logical map.
        """
        fields = avro_type["items"]["fields"]
        if len(fields) != 2:
            raise ValueError(f'Invalid key-value pair schema: {avro_type["items"]}')
        key = self._convert_field(list(filter(lambda f: f["name"] == "key", fields))[0])
        value = self._convert_field(list(filter(lambda f: f["name"] == "value", fields))[0])
        return MapType(
            key_id=key.field_id,
            key_type=key.field_type,
            value_id=value.field_id,
            value_type=value.field_type,
            value_required=value.required,
        )

    def _convert_fixed_type(self, avro_type: dict[str, Any]) -> FixedType:
        """
        Converts Avro Type to the equivalent Iceberg fixed type.

        - https://avro.apache.org/docs/current/spec.html#Fixed

        Args:
            avro_type: The Avro type.

        Examples:
            >>> from pyiceberg.utils.schema_conversion import AvroSchemaConversion
            >>> avro_fixed_type = {
            ...     "name": "md5",
            ...     "type": "fixed",
            ...     "size": 16
            ... }
            >>> FixedType(length=16) == AvroSchemaConversion()._convert_fixed_type(avro_fixed_type)
            True

        Returns:
            An Iceberg equivalent fixed type.
        """
        return FixedType(length=avro_type["size"])

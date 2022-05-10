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
"""Util class for converting between Avro and Iceberg schemas

"""
import logging
from typing import Any, Dict, List, Tuple, Union

from iceberg.schema import Schema
from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
)

logger = logging.getLogger(__name__)


class AvroSchemaConversion:
    PRIMITIVE_FIELD_TYPE_MAP: Dict[str, PrimitiveType] = {
        "boolean": BooleanType(),
        "bytes": BinaryType(),
        "date": DateType(),
        "double": DoubleType(),
        "float": FloatType(),
        "int": IntegerType(),
        "long": LongType(),
        "string": StringType(),
        "time-millis": TimeType(),
        "timestamp-millis": TimestampType(),
    }

    def avro_to_iceberg(self, avro_schema: Dict[str, Any]) -> Schema:
        """Converts an Apache Avro into an Apache Iceberg schema equivalent

        This expect to have field id's to be encoded in the Avro schema::

            {
                "type": "record",
                "name": "manifest_file",
                "fields": [
                    {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
                    {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501}
                ]
            }

        Example:
            This converts an Avro schema into a Iceberg schema:

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
            ...         field_id=500, name="manifest_path", field_type=StringType(), is_optional=False, doc="Location URI with FS scheme"
            ...     ),
            ...     NestedField(
            ...         field_id=501, name="manifest_length", field_type=LongType(), is_optional=False, doc="Total file size in bytes"
            ...     ),
            ...     schema_id=1
            ... )
            >>> avro_schema == iceberg_schema
            True

        Args:
            avro_schema (Dict[str, Any]): The JSON decoded Avro schema

        Returns:
            Equivalent Iceberg schema

        Todo:
            * Implement full support for unions
            * Implement logical types
        """
        fields = self._parse_record(avro_schema)
        return Schema(*fields.fields, schema_id=1)  # type: ignore

    def _parse_record(self, avro_field: Dict[str, Any]) -> StructType:
        return StructType(*[self._parse_field(field) for field in avro_field["fields"]])

    def _resolve_union(self, type_union: Union[Dict, List, str]) -> Tuple[Union[str, dict], bool]:
        """
        Converts Unions into their type and resolves if the field is optional

        Examples:
            >>> AvroSchemaConversion()._resolve_union('str')
            ('str', False)
            >>> AvroSchemaConversion()._resolve_union(['null', 'str'])
            ('str', True)
            >>> AvroSchemaConversion()._resolve_union([{'type': 'str'}])
            ({'type': 'str'}, False)
            >>> AvroSchemaConversion()._resolve_union(['null', {'type': 'str'}])
            ({'type': 'str'}, True)

        Args:
            type_union: The field, can be a string 'str', list ['null', 'str'], or dict {"type": 'str'}

        Returns:
            A tuple containing the type and nullability

        """
        avro_types: Union[Dict, List]
        if isinstance(type_union, str):
            avro_types = [type_union]
        else:
            avro_types = type_union

        is_optional = "null" in avro_types

        # Filter the null value, so we know the actual type
        avro_types = list(filter(lambda t: t != "null", avro_types))

        if len(avro_types) != 1:
            raise ValueError("Support for unions is yet to be implemented")

        avro_type = avro_types[0]

        return avro_type, is_optional

    def _parse_field(self, field: Dict[str, Any]) -> NestedField:
        """
        Recursively walks through the Schema, constructing the Iceberg schema

        Examples:
            >>> avro_schema = AvroSchemaConversion().avro_to_iceberg({
            ...     "type": "record",
            ...     "name": "manifest_file",
            ...     "fields": [
            ...         {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
            ...         {
            ...             "name": "partitions",
            ...                "type": [
            ...                "null",
            ...                {
            ...                        "type": "array",
            ...                        "items": {
            ...                            "type": "record",
            ...                            "name": "r508",
            ...                            "fields": [
            ...                                {
            ...                                    "name": "contains_null",
            ...                                    "type": "boolean",
            ...                                    "doc": "True if any file has a null partition value",
            ...                                    "field-id": 509,
            ...                                },
            ...                                {
            ...                                    "name": "contains_nan",
            ...                                    "type": ["null", "boolean"],
            ...                                    "doc": "True if any file has a nan partition value",
            ...                                    "default": None,
            ...                                    "field-id": 518,
            ...                                },
            ...                            ],
            ...                        },
            ...                        "element-id": 508,
            ...                    },
            ...                ],
            ...                "doc": "Summary for each partition",
            ...                "default": None,
            ...                "field-id": 507,
            ...            },
            ...     ]
            ... })
            >>> iceberg_schema = Schema(
            ...     NestedField(
            ...         field_id=500, name="manifest_path", field_type=StringType(), is_optional=False, doc="Location URI with FS scheme"
            ...     ),
            ...     NestedField(
            ...         field_id=507,
            ...         name="partitions",
            ...         field_type=StructType(
            ...             NestedField(
            ...                 field_id=509,
            ...                 name="contains_null",
            ...                 field_type=BooleanType(),
            ...                 is_optional=False,
            ...                 doc="True if any file has a null partition value",
            ...             ),
            ...             NestedField(
            ...                 field_id=518,
            ...                 name="contains_nan",
            ...                 field_type=BooleanType(),
            ...                 is_optional=True,
            ...                 doc="True if any file has a nan partition value",
            ...             ),
            ...         ),
            ...         is_optional=True,
            ...         doc="Summary for each partition",
            ...     ),
            ...     schema_id=1
            ... )
            >>> avro_schema == iceberg_schema
            True

        Args:
            field:

        Returns:

        """
        field_id = field["field-id"]
        field_name = field["name"]
        field_doc = field.get("doc")

        avro_type, is_optional = self._resolve_union(field["type"])
        if isinstance(avro_type, dict):
            avro_type = avro_type["type"]

        if isinstance(avro_type, str) and avro_type in AvroSchemaConversion.PRIMITIVE_FIELD_TYPE_MAP:
            return NestedField(
                field_id=field_id,
                name=field_name,
                field_type=AvroSchemaConversion.PRIMITIVE_FIELD_TYPE_MAP[avro_type],
                is_optional=is_optional,
                doc=field_doc,
            )
        elif avro_type == "record":
            return NestedField(
                field_id=field_id, name=field_name, field_type=self._parse_record(field), is_optional=is_optional, doc=field_doc
            )
        elif avro_type == "array":
            inner_type, _ = self._resolve_union(field["type"])
            assert isinstance(inner_type, dict)
            inner_type["items"]["field-id"] = inner_type["element-id"]
            inner_field = self._parse_field(inner_type["items"])

            return NestedField(
                field_id=field_id, name=field_name, field_type=inner_field.type, is_optional=is_optional, doc=field_doc
            )
        elif avro_type == "map":
            return NestedField(
                field_id=field_id,
                name=field_name,
                field_type=MapType(
                    key_id=field["key-id"],
                    key_type=StringType(),
                    value_id=field["value-id"],
                    value_type=self._parse_field(field["values"]),
                ),
                is_optional=is_optional,
                doc=field_doc,
            )
        else:
            raise ValueError(f"Unknown type: {field}")

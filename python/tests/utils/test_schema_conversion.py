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
# pylint: disable=W0212
from typing import Any, Dict

import pytest

from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    FixedType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)
from pyiceberg.utils.schema_conversion import AvroSchemaConversion


def test_iceberg_to_avro(avro_schema_manifest_file: Dict[str, Any]) -> None:
    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema_manifest_file)
    expected_iceberg_schema = Schema(
        NestedField(
            field_id=500, name="manifest_path", field_type=StringType(), required=True, doc="Location URI with FS scheme"
        ),
        NestedField(field_id=501, name="manifest_length", field_type=LongType(), required=True, doc="Total file size in bytes"),
        NestedField(field_id=502, name="partition_spec_id", field_type=IntegerType(), required=True, doc="Spec ID used to write"),
        NestedField(
            field_id=503,
            name="added_snapshot_id",
            field_type=LongType(),
            required=False,
            doc="Snapshot ID that added the manifest",
        ),
        NestedField(
            field_id=504, name="added_data_files_count", field_type=IntegerType(), required=False, doc="Added entry count"
        ),
        NestedField(
            field_id=505, name="existing_data_files_count", field_type=IntegerType(), required=False, doc="Existing entry count"
        ),
        NestedField(
            field_id=506, name="deleted_data_files_count", field_type=IntegerType(), required=False, doc="Deleted entry count"
        ),
        NestedField(
            field_id=507,
            name="partitions",
            field_type=ListType(
                element_id=508,
                element_type=StructType(
                    NestedField(
                        field_id=509,
                        name="contains_null",
                        field_type=BooleanType(),
                        required=True,
                        doc="True if any file has a null partition value",
                    ),
                    NestedField(
                        field_id=518,
                        name="contains_nan",
                        field_type=BooleanType(),
                        required=False,
                        doc="True if any file has a nan partition value",
                    ),
                    NestedField(
                        field_id=510,
                        name="lower_bound",
                        field_type=BinaryType(),
                        required=False,
                        doc="Partition lower bound for all files",
                    ),
                    NestedField(
                        field_id=511,
                        name="upper_bound",
                        field_type=BinaryType(),
                        required=False,
                        doc="Partition upper bound for all files",
                    ),
                ),
                element_required=True,
            ),
            required=False,
            doc="Summary for each partition",
        ),
        NestedField(field_id=512, name="added_rows_count", field_type=LongType(), required=False, doc="Added rows count"),
        NestedField(field_id=513, name="existing_rows_count", field_type=LongType(), required=False, doc="Existing rows count"),
        NestedField(field_id=514, name="deleted_rows_count", field_type=LongType(), required=False, doc="Deleted rows count"),
        schema_id=1,
        identifier_field_ids=[],
    )
    assert iceberg_schema == expected_iceberg_schema


def test_avro_list_required_primitive() -> None:
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "array_with_string",
                "type": {
                    "type": "array",
                    "items": "string",
                    "default": [],
                    "element-id": 101,
                },
                "field-id": 100,
            },
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="array_with_string",
            field_type=ListType(element_id=101, element_type=StringType(), element_required=True),
            required=True,
        ),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_list_wrapped_primitive() -> None:
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "array_with_string",
                "type": {
                    "type": "array",
                    "items": {"type": "string"},
                    "default": [],
                    "element-id": 101,
                },
                "field-id": 100,
            },
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="array_with_string",
            field_type=ListType(element_id=101, element_type=StringType(), element_required=True),
            required=True,
        ),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_list_required_record() -> None:
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "array_with_record",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "r101",
                        "fields": [
                            {
                                "name": "contains_null",
                                "type": "boolean",
                                "field-id": 102,
                            },
                            {
                                "name": "contains_nan",
                                "type": ["null", "boolean"],
                                "field-id": 103,
                            },
                        ],
                    },
                    "element-id": 101,
                },
                "field-id": 100,
            }
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="array_with_record",
            field_type=ListType(
                element_id=101,
                element_type=StructType(
                    NestedField(field_id=102, name="contains_null", field_type=BooleanType(), required=True),
                    NestedField(field_id=103, name="contains_nan", field_type=BooleanType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        schema_id=1,
        identifier_field_ids=[],
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_resolve_union() -> None:
    with pytest.raises(TypeError) as exc_info:
        AvroSchemaConversion()._resolve_union(["null", "string", "long"])

    assert "Non-optional types aren't part of the Iceberg specification" in str(exc_info.value)


def test_nested_type() -> None:
    # In the case a primitive field is nested
    assert AvroSchemaConversion()._convert_schema({"type": {"type": "string"}}) == StringType()


def test_map_type() -> None:
    avro_type = {
        "type": "map",
        "values": ["null", "long"],
        "key-id": 101,
        "value-id": 102,
    }
    actual = AvroSchemaConversion()._convert_schema(avro_type)
    expected = MapType(key_id=101, key_type=StringType(), value_id=102, value_type=LongType(), value_required=False)
    assert actual == expected


def test_fixed_type() -> None:
    avro_type = {"type": "fixed", "size": 22}
    actual = AvroSchemaConversion()._convert_schema(avro_type)
    expected = FixedType(22)
    assert actual == expected


def test_unknown_primitive() -> None:
    with pytest.raises(TypeError) as exc_info:
        avro_type = "UnknownType"
        AvroSchemaConversion()._convert_schema(avro_type)
    assert "Unknown type: UnknownType" in str(exc_info.value)


def test_unknown_complex_type() -> None:
    with pytest.raises(TypeError) as exc_info:
        avro_type = {
            "type": "UnknownType",
        }
        AvroSchemaConversion()._convert_schema(avro_type)
    assert "Unknown type: {'type': 'UnknownType'}" in str(exc_info.value)


def test_convert_field_without_field_id() -> None:
    with pytest.raises(ValueError) as exc_info:
        avro_field = {
            "name": "contains_null",
            "type": "boolean",
        }
        AvroSchemaConversion()._convert_field(avro_field)
    assert "Cannot convert field, missing field-id" in str(exc_info.value)


def test_convert_record_type_without_record() -> None:
    with pytest.raises(ValueError) as exc_info:
        avro_field = {"type": "non-record", "name": "avro_schema", "fields": []}
        AvroSchemaConversion()._convert_record_type(avro_field)
    assert "Expected record type, got" in str(exc_info.value)


def test_avro_list_missing_element_id() -> None:
    avro_type = {
        "name": "array_with_string",
        "type": {
            "type": "array",
            "items": "string",
            "default": [],
            # "element-id": 101,
        },
        "field-id": 100,
    }

    with pytest.raises(ValueError) as exc_info:
        AvroSchemaConversion()._convert_array_type(avro_type)

    assert "Cannot convert array-type, missing element-id:" in str(exc_info.value)


def test_convert_decimal_type() -> None:
    avro_decimal_type = {"type": "bytes", "logicalType": "decimal", "precision": 19, "scale": 25}
    actual = AvroSchemaConversion()._convert_logical_type(avro_decimal_type)
    expected = DecimalType(precision=19, scale=25)
    assert actual == expected


def test_convert_date_type() -> None:
    avro_logical_type = {"type": "int", "logicalType": "date"}
    actual = AvroSchemaConversion()._convert_logical_type(avro_logical_type)
    assert actual == DateType()


def test_unknown_logical_type() -> None:
    """Test raising a ValueError when converting an unknown logical type as part of an Avro schema conversion"""
    avro_logical_type = {"type": "bytes", "logicalType": "date"}
    with pytest.raises(ValueError) as exc_info:
        AvroSchemaConversion()._convert_logical_type(avro_logical_type)

    assert "Unknown logical/physical type combination:" in str(exc_info.value)


def test_logical_map_with_invalid_fields() -> None:
    avro_type = {
        "type": "array",
        "logicalType": "map",
        "items": {
            "type": "record",
            "name": "k101_v102",
            "fields": [
                {"name": "key", "type": "int", "field-id": 101},
                {"name": "value", "type": "string", "field-id": 102},
                {"name": "other", "type": "bytes", "field-id": 103},
            ],
        },
    }

    with pytest.raises(ValueError) as exc_info:
        AvroSchemaConversion()._convert_logical_map_type(avro_type)

    assert "Invalid key-value pair schema:" in str(exc_info.value)

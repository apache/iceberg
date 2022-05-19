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

from iceberg.schema import Schema
from iceberg.types import (
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
from iceberg.utils.schema_conversion import AvroSchemaConversion


def test_iceberg_to_avro(manifest_schema):
    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(manifest_schema)
    expected_iceberg_schema = Schema(
        NestedField(field_id=500, name="manifest_path", field_type=StringType(), is_optional=False),
        NestedField(field_id=501, name="manifest_length", field_type=LongType(), is_optional=False),
        NestedField(field_id=502, name="partition_spec_id", field_type=IntegerType(), is_optional=False),
        NestedField(field_id=503, name="added_snapshot_id", field_type=LongType(), is_optional=True),
        NestedField(field_id=504, name="added_data_files_count", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=505, name="existing_data_files_count", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=506, name="deleted_data_files_count", field_type=IntegerType(), is_optional=True),
        NestedField(
            field_id=507,
            name="partitions",
            field_type=ListType(
                element_id=508,
                element_type=StructType(
                    fields=(
                        NestedField(field_id=509, name="contains_null", field_type=BooleanType(), is_optional=False),
                        NestedField(field_id=518, name="contains_nan", field_type=BooleanType(), is_optional=True),
                        NestedField(field_id=510, name="lower_bound", field_type=BinaryType(), is_optional=True),
                        NestedField(field_id=511, name="upper_bound", field_type=BinaryType(), is_optional=True),
                    )
                ),
                element_is_optional=False,
            ),
            is_optional=True,
        ),
        NestedField(field_id=512, name="added_rows_count", field_type=LongType(), is_optional=True),
        NestedField(field_id=513, name="existing_rows_count", field_type=LongType(), is_optional=True),
        NestedField(field_id=514, name="deleted_rows_count", field_type=LongType(), is_optional=True),
        schema_id=1,
        identifier_field_ids=[],
    )
    assert iceberg_schema == expected_iceberg_schema


def test_avro_list_required_primitive():
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
            field_type=ListType(element_id=101, element_type=StringType(), element_is_optional=False),
            is_optional=False,
        ),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_list_required_record():
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
                    fields=(
                        NestedField(field_id=102, name="contains_null", field_type=BooleanType(), is_optional=False),
                        NestedField(field_id=103, name="contains_nan", field_type=BooleanType(), is_optional=True),
                    )
                ),
                element_is_optional=False,
            ),
            is_optional=False,
        ),
        schema_id=1,
        identifier_field_ids=[],
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_list_optional_primitive():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "array_with_string",
                "type": [
                    {
                        "type": "array",
                        "items": ["string", "null"],
                        "default": [],
                        "element-id": 101,
                    },
                    "null",
                ],
                "field-id": 100,
            }
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="array_with_string",
            field_type=ListType(element_id=101, element_type=StringType(), element_is_optional=True),
            is_optional=True,
        ),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_map_with_required_longs():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "map_with_longs",
                "type": {
                    "type": "map",
                    "values": "long",
                    "key-id": 101,
                    "value-id": 102,
                },
                "field-id": 100,
            }
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="map_with_longs",
            field_type=MapType(key_id=101, key_type=StringType(), value_id=102, value_type=LongType(), value_is_optional=False),
            is_optional=False,
        ),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_map_with_optional_longs():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "map_with_longs",
                "type": {
                    "type": "map",
                    "values": ["long", "null"],
                    "key-id": 101,
                    "value-id": 102,
                },
                "field-id": 100,
            }
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="map_with_longs",
            field_type=MapType(key_id=101, key_type=StringType(), value_id=102, value_type=LongType(), value_is_optional=True),
            is_optional=False,
        ),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_fixed():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [{"type": "fixed", "size": 16, "name": "md5", "field-id": 100}],
    }

    expected_iceberg_schema = Schema(
        NestedField(name="md5", field_id=100, field_type=FixedType(length=16), is_optional=False), schema_id=1
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_date():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [{"name": "birthday", "type": {"type": "int", "logicalType": "date"}, "field-id": 100}],
    }

    expected_iceberg_schema = Schema(
        NestedField(name="birthday", field_id=100, field_type=DateType(), is_optional=False), schema_id=1
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_decimal():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "some_decimal",
                "type": {"type": "bytes", "logicalType": "decimal", "precision": 19, "scale": 25},
                "field-id": 100,
            }
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(name="some_decimal", field_id=100, field_type=DecimalType(precision=19, scale=25), is_optional=False),
        schema_id=1,
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema


def test_avro_non_string_key_map():
    avro_schema = {
        "type": "record",
        "name": "avro_schema",
        "fields": [
            {
                "name": "some_decimal",
                "type": {
                    "type": "array",
                    "logicalType": "map",
                    "items": {
                        "type": "record",
                        "name": "k12_v13",
                        "fields": [
                            {"name": "key", "type": "int", "field-id": 101},
                            {"name": "value", "type": "string", "field-id": 102},
                        ],
                    },
                },
                "field-id": 100,
            }
        ],
    }

    expected_iceberg_schema = Schema(
        NestedField(
            field_id=100,
            name="some_decimal",
            field_type=MapType(
                key_id=101, key_type=IntegerType(), value_id=102, value_type=StringType(), value_is_optional=False
            ),
            is_optional=False,
        ),
        schema_id=1,
        identifier_field_ids=[],
    )

    iceberg_schema = AvroSchemaConversion().avro_to_iceberg(avro_schema)

    assert expected_iceberg_schema == iceberg_schema

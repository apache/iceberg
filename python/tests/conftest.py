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

from typing import Any, Dict

import pytest

from iceberg import schema
from iceberg.types import (
    BooleanType,
    FloatType,
    IntegerType,
    ListType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


class FooStruct:
    """An example of an object that abides by StructProtocol"""

    def __init__(self):
        self.content = {}

    def get(self, pos: int) -> Any:
        return self.content[pos]

    def set(self, pos: int, value) -> None:
        self.content[pos] = value


@pytest.fixture(scope="session", autouse=True)
def table_schema_simple():
    return schema.Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        schema_id=1,
        identifier_field_ids=[1],
    )


@pytest.fixture(scope="session", autouse=True)
def table_schema_nested():
    return schema.Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), is_optional=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), is_optional=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), is_optional=False),
        NestedField(
            field_id=4,
            name="qux",
            field_type=ListType(element_id=5, element_type=StringType(), element_is_optional=True),
            is_optional=True,
        ),
        NestedField(
            field_id=6,
            name="quux",
            field_type=MapType(
                key_id=7,
                key_type=StringType(),
                value_id=8,
                value_type=MapType(
                    key_id=9, key_type=StringType(), value_id=10, value_type=IntegerType(), value_is_optional=True
                ),
                value_is_optional=True,
            ),
            is_optional=True,
        ),
        NestedField(
            field_id=11,
            name="location",
            field_type=ListType(
                element_id=12,
                element_type=StructType(
                    NestedField(field_id=13, name="latitude", field_type=FloatType(), is_optional=False),
                    NestedField(field_id=14, name="longitude", field_type=FloatType(), is_optional=False),
                ),
                element_is_optional=True,
            ),
            is_optional=True,
        ),
        NestedField(
            field_id=15,
            name="person",
            field_type=StructType(
                NestedField(field_id=16, name="name", field_type=StringType(), is_optional=False),
                NestedField(field_id=17, name="age", field_type=IntegerType(), is_optional=True),
            ),
            is_optional=False,
        ),
        schema_id=1,
        identifier_field_ids=[1],
    )


@pytest.fixture(scope="session", autouse=True)
def foo_struct():
    return FooStruct()


@pytest.fixture(scope="session")
def manifest_schema() -> Dict[str, Any]:
    return {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 500},
            {"name": "manifest_length", "type": "long", "doc": "Total file size in bytes", "field-id": 501},
            {"name": "partition_spec_id", "type": "int", "doc": "Spec ID used to write", "field-id": 502},
            {
                "name": "added_snapshot_id",
                "type": ["null", "long"],
                "doc": "Snapshot ID that added the manifest",
                "default": None,
                "field-id": 503,
            },
            {
                "name": "added_data_files_count",
                "type": ["null", "int"],
                "doc": "Added entry count",
                "default": None,
                "field-id": 504,
            },
            {
                "name": "existing_data_files_count",
                "type": ["null", "int"],
                "doc": "Existing entry count",
                "default": None,
                "field-id": 505,
            },
            {
                "name": "deleted_data_files_count",
                "type": ["null", "int"],
                "doc": "Deleted entry count",
                "default": None,
                "field-id": 506,
            },
            {
                "name": "partitions",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "r508",
                            "fields": [
                                {
                                    "name": "contains_null",
                                    "type": "boolean",
                                    "doc": "True if any file has a null partition value",
                                    "field-id": 509,
                                },
                                {
                                    "name": "contains_nan",
                                    "type": ["null", "boolean"],
                                    "doc": "True if any file has a nan partition value",
                                    "default": None,
                                    "field-id": 518,
                                },
                                {
                                    "name": "lower_bound",
                                    "type": ["null", "bytes"],
                                    "doc": "Partition lower bound for all files",
                                    "default": None,
                                    "field-id": 510,
                                },
                                {
                                    "name": "upper_bound",
                                    "type": ["null", "bytes"],
                                    "doc": "Partition upper bound for all files",
                                    "default": None,
                                    "field-id": 511,
                                },
                            ],
                        },
                        "element-id": 508,
                    },
                ],
                "doc": "Summary for each partition",
                "default": None,
                "field-id": 507,
            },
            {"name": "added_rows_count", "type": ["null", "long"], "doc": "Added rows count", "default": None, "field-id": 512},
            {
                "name": "existing_rows_count",
                "type": ["null", "long"],
                "doc": "Existing rows count",
                "default": None,
                "field-id": 513,
            },
            {
                "name": "deleted_rows_count",
                "type": ["null", "long"],
                "doc": "Deleted rows count",
                "default": None,
                "field-id": 514,
            },
        ],
    }

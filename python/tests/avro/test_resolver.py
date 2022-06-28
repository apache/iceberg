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
import pytest

from iceberg.avro.reader import (
    DoubleReader,
    LongReader,
    MapReader,
    StringReader,
    StructReader,
)
from iceberg.avro.resolver import ResolveException, resolve
from iceberg.schema import Schema
from iceberg.types import (
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


def test_resolver():
    write_schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "data", StringType()),
        NestedField(
            3,
            "location",
            StructType(
                NestedField(4, "lat", DoubleType()),
                NestedField(5, "long", DoubleType()),
            ),
        ),
        NestedField(6, "preferences", MapType(7, StringType(), 8, StringType())),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(
            3,
            "location",
            StructType(
                NestedField(4, "lat", DoubleType()),
                NestedField(5, "long", DoubleType()),
            ),
        ),
        NestedField(1, "id", LongType()),
        NestedField(6, "preferences", MapType(7, StringType(), 8, StringType())),
        schema_id=1,
    )
    read_tree = resolve(write_schema, read_schema)

    assert read_tree == StructReader(
        (
            (1, LongReader()),
            (None, StringReader()),
            (
                0,
                StructReader(
                    (
                        (0, DoubleReader()),
                        (1, DoubleReader()),
                    )
                ),
            ),
            (2, MapReader(StringReader(), StringReader())),
        )
    )


def test_resolver_new_required_field():
    write_schema = Schema(
        NestedField(1, "id", LongType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "data", StringType(), required=True),
        schema_id=1,
    )

    with pytest.raises(ResolveException) as exc_info:
        resolve(write_schema, read_schema)

    assert "2: data: optional string is in not in the write schema, and is required" in str(exc_info.value)


def test_resolver_invalid_evolution():
    write_schema = Schema(
        NestedField(1, "id", LongType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "id", IntegerType()),
        schema_id=1,
    )

    with pytest.raises(ResolveException) as exc_info:
        resolve(write_schema, read_schema)

    assert "Promotion from int to long is not allowed" in str(exc_info.value)


def test_resolver_change_type():
    write_schema = Schema(
        NestedField(1, "properties", ListType(2, StringType())),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "properties", MapType(2, StringType(), 3, StringType())),
        schema_id=1,
    )

    with pytest.raises(ResolveException) as exc_info:
        resolve(write_schema, read_schema)

    assert (
        "Write/read schema are not aligned for list<string>, got MapType(key_id=2, key_type=StringType(), value_id=3, value_type=StringType(), value_required=True)"
        in str(exc_info.value)
    )

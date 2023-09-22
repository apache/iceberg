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

from tempfile import TemporaryDirectory
from typing import Optional

import pytest
from pydantic import Field

from pyiceberg.avro.file import AvroFile
from pyiceberg.avro.reader import (
    DecimalReader,
    DefaultReader,
    DoubleReader,
    FloatReader,
    IntegerReader,
    MapReader,
    StringReader,
    StructReader,
)
from pyiceberg.avro.resolver import ResolveError, resolve
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


def test_resolver() -> None:
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

    location_struct = StructType(
        NestedField(4, "lat", DoubleType()),
        NestedField(5, "long", DoubleType()),
    )
    read_schema = Schema(
        NestedField(
            3,
            "location",
            location_struct,
        ),
        NestedField(1, "id", LongType()),
        NestedField(6, "preferences", MapType(7, StringType(), 8, StringType())),
        schema_id=1,
    )
    read_tree = resolve(write_schema, read_schema)

    assert read_tree == StructReader(
        (
            (1, IntegerReader()),
            (None, StringReader()),
            (
                0,
                StructReader(
                    (
                        (0, DoubleReader()),
                        (1, DoubleReader()),
                    ),
                    Record,
                    location_struct,
                ),
            ),
            (2, MapReader(StringReader(), StringReader())),
        ),
        Record,
        read_schema.as_struct(),
    )


def test_resolver_new_required_field() -> None:
    write_schema = Schema(
        NestedField(1, "id", LongType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "data", StringType(), required=True),
        schema_id=1,
    )

    with pytest.raises(ResolveError) as exc_info:
        resolve(write_schema, read_schema)

    assert "2: data: required string is non-optional, and not part of the file schema" in str(exc_info.value)


def test_resolver_invalid_evolution() -> None:
    write_schema = Schema(
        NestedField(1, "id", LongType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "id", DoubleType()),
        schema_id=1,
    )

    with pytest.raises(ResolveError) as exc_info:
        resolve(write_schema, read_schema)

    assert "Cannot promote long to double" in str(exc_info.value)


def test_resolver_promotion_string_to_binary() -> None:
    write_schema = Schema(
        NestedField(1, "id", StringType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "id", BinaryType()),
        schema_id=1,
    )
    resolve(write_schema, read_schema)


def test_resolver_promotion_binary_to_string() -> None:
    write_schema = Schema(
        NestedField(1, "id", BinaryType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "id", StringType()),
        schema_id=1,
    )
    resolve(write_schema, read_schema)


def test_resolver_change_type() -> None:
    write_schema = Schema(
        NestedField(1, "properties", ListType(2, StringType())),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(1, "properties", MapType(2, StringType(), 3, StringType())),
        schema_id=1,
    )

    with pytest.raises(ResolveError) as exc_info:
        resolve(write_schema, read_schema)

    assert "File/read schema are not aligned for list, got map<string, string>" in str(exc_info.value)


def test_resolve_int_to_long() -> None:
    assert resolve(IntegerType(), LongType()) == IntegerReader()


def test_resolve_float_to_double() -> None:
    # We should still read floats, because it is encoded in 4 bytes
    assert resolve(FloatType(), DoubleType()) == FloatReader()


def test_resolve_decimal_to_decimal() -> None:
    # DecimalType(P, S) to DecimalType(P2, S) where P2 > P
    assert resolve(DecimalType(19, 25), DecimalType(22, 25)) == DecimalReader(19, 25)


def test_struct_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(StructType(), StringType())


def test_map_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(MapType(1, StringType(), 2, IntegerType()), StringType())


def test_primitive_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(IntegerType(), MapType(1, StringType(), 2, IntegerType()))


def test_integer_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(IntegerType(), StringType())


def test_float_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(FloatType(), StringType())


def test_string_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(StringType(), FloatType())


def test_binary_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(BinaryType(), FloatType())


def test_decimal_not_aligned() -> None:
    with pytest.raises(ResolveError):
        assert resolve(DecimalType(22, 19), StringType())


def test_resolve_decimal_to_decimal_reduce_precision() -> None:
    # DecimalType(P, S) to DecimalType(P2, S) where P2 > P
    with pytest.raises(ResolveError) as exc_info:
        _ = resolve(DecimalType(19, 25), DecimalType(10, 25)) == DecimalReader(22, 25)

    assert "Cannot reduce precision from decimal(19, 25) to decimal(10, 25)" in str(exc_info.value)


def test_column_assignment() -> None:
    int_schema = {
        "type": "record",
        "name": "ints",
        "fields": [
            {"name": "a", "type": "int", "field-id": 1},
            {"name": "b", "type": "int", "field-id": 2},
            {"name": "c", "type": "int", "field-id": 3},
        ],
    }

    from fastavro import parse_schema, writer

    parsed_schema = parse_schema(int_schema)

    int_records = [
        {
            "a": 1,
            "b": 2,
            "c": 3,
        }
    ]

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/manifest.avro"
        with open(tmp_avro_file, "wb") as out:
            writer(out, parsed_schema, int_records)

        class Ints(Record):
            c: int = Field()
            d: Optional[int] = Field()

        MANIFEST_ENTRY_SCHEMA = Schema(
            NestedField(3, "c", IntegerType(), required=True),
            NestedField(4, "d", IntegerType(), required=False),
        )

        with AvroFile[Ints](PyArrowFileIO().new_input(tmp_avro_file), MANIFEST_ENTRY_SCHEMA, {-1: Ints}) as reader:
            records = list(reader)

    assert repr(records) == "[Ints[c=3, d=None]]"


def test_resolver_initial_value() -> None:
    write_schema = Schema(
        NestedField(1, "name", StringType()),
        schema_id=1,
    )
    read_schema = Schema(
        NestedField(2, "something", StringType(), required=False, initial_default="vo"),
        schema_id=2,
    )

    assert resolve(write_schema, read_schema) == StructReader(
        (
            (None, StringReader()),  # The one we skip
            (0, DefaultReader("vo")),
        ),
        Record,
        read_schema.as_struct(),
    )

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

from pyiceberg.avro.reader import (
    DecimalReader,
    DoubleReader,
    FloatReader,
    IntegerReader,
    MapReader,
    StringReader,
    StructReader,
)
from pyiceberg.avro.resolver import ResolveException, promote, resolve
from pyiceberg.schema import Schema
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
            (1, IntegerReader()),
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

    with pytest.raises(ResolveException) as exc_info:
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

    with pytest.raises(ResolveException) as exc_info:
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

    with pytest.raises(ResolveException) as exc_info:
        resolve(write_schema, read_schema)

    assert "File/read schema are not aligned for list<string>, got map<string, string>" in str(exc_info.value)


def test_promote_int_to_long() -> None:
    assert promote(IntegerType(), LongType()) == IntegerReader()


def test_promote_float_to_double() -> None:
    # We should still read floats, because it is encoded in 4 bytes
    assert promote(FloatType(), DoubleType()) == FloatReader()


def test_promote_decimal_to_decimal() -> None:
    # DecimalType(P, S) to DecimalType(P2, S) where P2 > P
    assert promote(DecimalType(19, 25), DecimalType(22, 25)) == DecimalReader(22, 25)


def test_struct_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(StructType(), StringType())


def test_map_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(MapType(1, StringType(), 2, IntegerType()), StringType())


def test_primitive_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(IntegerType(), MapType(1, StringType(), 2, IntegerType()))


def test_integer_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(IntegerType(), StringType())


def test_float_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(FloatType(), StringType())


def test_string_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(StringType(), FloatType())


def test_binary_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(BinaryType(), FloatType())


def test_decimal_not_aligned() -> None:
    with pytest.raises(ResolveException):
        assert promote(DecimalType(22, 19), StringType())


def test_promote_decimal_to_decimal_reduce_precision() -> None:
    # DecimalType(P, S) to DecimalType(P2, S) where P2 > P
    with pytest.raises(ResolveException) as exc_info:
        _ = promote(DecimalType(19, 25), DecimalType(10, 25)) == DecimalReader(22, 25)

    assert "Cannot reduce precision from decimal(19, 25) to decimal(10, 25)" in str(exc_info.value)

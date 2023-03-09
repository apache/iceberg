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
# pylint: disable=protected-access,unused-argument,redefined-outer-name


import pyarrow as pa

from pyiceberg.io.pyarrow import _ConvertToArrowSchema, pyarrow_to_schema, schema_to_pyarrow
from pyiceberg.schema import Schema, visit
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


def test_schema_to_pyarrow_schema(table_schema_nested: Schema) -> None:
    actual = schema_to_pyarrow(table_schema_nested)
    expected = """foo: string
  -- field metadata --
  field_id: '1'
bar: int32 not null
  -- field metadata --
  field_id: '2'
baz: bool
  -- field metadata --
  field_id: '3'
qux: list<element: string not null> not null
  child 0, element: string not null
    -- field metadata --
    field_id: '5'
  -- field metadata --
  field_id: '4'
quux: map<string, map<string, int32>> not null
  child 0, entries: struct<key: string not null, value: map<string, int32> not null> not null
      child 0, key: string not null
      -- field metadata --
      field_id: '7'
      child 1, value: map<string, int32> not null
          child 0, entries: struct<key: string not null, value: int32 not null> not null
              child 0, key: string not null
          -- field metadata --
          field_id: '9'
              child 1, value: int32 not null
          -- field metadata --
          field_id: '10'
      -- field metadata --
      field_id: '8'
  -- field metadata --
  field_id: '6'
location: list<element: struct<latitude: float, longitude: float> not null> not null
  child 0, element: struct<latitude: float, longitude: float> not null
      child 0, latitude: float
      -- field metadata --
      field_id: '13'
      child 1, longitude: float
      -- field metadata --
      field_id: '14'
    -- field metadata --
    field_id: '12'
  -- field metadata --
  field_id: '11'
person: struct<name: string, age: int32 not null>
  child 0, name: string
    -- field metadata --
    field_id: '16'
  child 1, age: int32 not null
    -- field metadata --
    field_id: '17'
  -- field metadata --
  field_id: '15'"""
    assert repr(actual) == expected


def test_fixed_type_to_pyarrow() -> None:
    length = 22
    iceberg_type = FixedType(length)
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.binary(length)


def test_decimal_type_to_pyarrow() -> None:
    precision = 25
    scale = 19
    iceberg_type = DecimalType(precision, scale)
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.decimal128(precision, scale)


def test_boolean_type_to_pyarrow() -> None:
    iceberg_type = BooleanType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.bool_()


def test_integer_type_to_pyarrow() -> None:
    iceberg_type = IntegerType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.int32()


def test_long_type_to_pyarrow() -> None:
    iceberg_type = LongType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.int64()


def test_float_type_to_pyarrow() -> None:
    iceberg_type = FloatType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.float32()


def test_double_type_to_pyarrow() -> None:
    iceberg_type = DoubleType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.float64()


def test_date_type_to_pyarrow() -> None:
    iceberg_type = DateType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.date32()


def test_time_type_to_pyarrow() -> None:
    iceberg_type = TimeType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.time64("us")


def test_timestamp_type_to_pyarrow() -> None:
    iceberg_type = TimestampType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.timestamp(unit="us")


def test_timestamptz_type_to_pyarrow() -> None:
    iceberg_type = TimestamptzType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.timestamp(unit="us", tz="UTC")


def test_string_type_to_pyarrow() -> None:
    iceberg_type = StringType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.string()


def test_binary_type_to_pyarrow() -> None:
    iceberg_type = BinaryType()
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.binary()


def test_struct_type_to_pyarrow(table_schema_simple: Schema) -> None:
    expected = pa.struct(
        [
            pa.field("foo", pa.string(), nullable=True, metadata={"id": "1"}),
            pa.field("bar", pa.int32(), nullable=False, metadata={"id": "2"}),
            pa.field("baz", pa.bool_(), nullable=True, metadata={"id": "3"}),
        ]
    )
    assert visit(table_schema_simple.as_struct(), _ConvertToArrowSchema()) == expected


def test_map_type_to_pyarrow() -> None:
    iceberg_map = MapType(
        key_id=1,
        key_type=IntegerType(),
        value_id=2,
        value_type=StringType(),
        value_required=True,
    )
    assert visit(iceberg_map, _ConvertToArrowSchema()) == pa.map_(
        pa.field("key", pa.int32(), nullable=False, metadata={"field_id": "1"}),
        pa.field("value", pa.string(), nullable=False, metadata={"field_id": "2"}),
    )


def test_list_type_to_pyarrow() -> None:
    iceberg_map = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit(iceberg_map, _ConvertToArrowSchema()) == pa.list_(
        pa.field("element", pa.int32(), nullable=False, metadata={"field_id": "1"})
    )


def test_pyarrow_to_schema_simple(table_schema_simple: Schema, pyarrow_schema_simple: pa.Schema) -> None:
    actual = str(pyarrow_to_schema(pyarrow_schema_simple))
    expected = str(table_schema_simple)
    assert actual == expected


def test_pyarrow_to_schema_nested(table_schema_nested: Schema, pyarrow_schema_nested: pa.Schema) -> None:
    actual = str(pyarrow_to_schema(pyarrow_schema_nested))
    expected = str(table_schema_nested)
    assert actual == expected


def test_round_conversion_nested(table_schema_nested: Schema) -> None:
    actual = str(pyarrow_to_schema(schema_to_pyarrow(table_schema_nested)))
    expected = str(table_schema_nested)
    assert actual == expected

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
import re

import pyarrow as pa
import pytest

from pyiceberg.io.pyarrow import (
    _ConvertToArrowSchema,
    _ConvertToIceberg,
    pyarrow_to_schema,
    schema_to_pyarrow,
    visit_pyarrow,
)
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
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


def test_schema_to_pyarrow_schema(table_schema_nested: Schema) -> None:
    actual = schema_to_pyarrow(table_schema_nested)
    expected = """foo: string
  -- field metadata --
  PYTHON:field_id: '1'
bar: int32 not null
  -- field metadata --
  PYTHON:field_id: '2'
baz: bool
  -- field metadata --
  PYTHON:field_id: '3'
qux: list<element: string not null> not null
  child 0, element: string not null
    -- field metadata --
    PYTHON:field_id: '5'
  -- field metadata --
  PYTHON:field_id: '4'
quux: map<string, map<string, int32>> not null
  child 0, entries: struct<key: string not null, value: map<string, int32> not null> not null
      child 0, key: string not null
      -- field metadata --
      PYTHON:field_id: '7'
      child 1, value: map<string, int32> not null
          child 0, entries: struct<key: string not null, value: int32 not null> not null
              child 0, key: string not null
          -- field metadata --
          PYTHON:field_id: '9'
              child 1, value: int32 not null
          -- field metadata --
          PYTHON:field_id: '10'
      -- field metadata --
      PYTHON:field_id: '8'
  -- field metadata --
  PYTHON:field_id: '6'
location: list<element: struct<latitude: float, longitude: float> not null> not null
  child 0, element: struct<latitude: float, longitude: float> not null
      child 0, latitude: float
      -- field metadata --
      PYTHON:field_id: '13'
      child 1, longitude: float
      -- field metadata --
      PYTHON:field_id: '14'
    -- field metadata --
    PYTHON:field_id: '12'
  -- field metadata --
  PYTHON:field_id: '11'
person: struct<name: string, age: int32 not null>
  child 0, name: string
    -- field metadata --
    PYTHON:field_id: '16'
  child 1, age: int32 not null
    -- field metadata --
    PYTHON:field_id: '17'
  -- field metadata --
  PYTHON:field_id: '15'"""
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


def test_pyarrow_binary_to_iceberg() -> None:
    length = 23
    pyarrow_type = pa.binary(length)
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == FixedType(length)
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_decimal128_to_iceberg() -> None:
    precision = 26
    scale = 20
    pyarrow_type = pa.decimal128(precision, scale)
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == DecimalType(precision, scale)
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_decimal256_to_iceberg() -> None:
    precision = 26
    scale = 20
    pyarrow_type = pa.decimal256(precision, scale)
    with pytest.raises(TypeError, match=re.escape("Unsupported type: decimal256(26, 20)")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_boolean_to_iceberg() -> None:
    pyarrow_type = pa.bool_()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == BooleanType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_int32_to_iceberg() -> None:
    pyarrow_type = pa.int32()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == IntegerType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_int64_to_iceberg() -> None:
    pyarrow_type = pa.int64()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == LongType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_float32_to_iceberg() -> None:
    pyarrow_type = pa.float32()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == FloatType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_float64_to_iceberg() -> None:
    pyarrow_type = pa.float64()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == DoubleType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_date32_to_iceberg() -> None:
    pyarrow_type = pa.date32()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == DateType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_date64_to_iceberg() -> None:
    pyarrow_type = pa.date64()
    with pytest.raises(TypeError, match=re.escape("Unsupported type: date64")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_time32_to_iceberg() -> None:
    pyarrow_type = pa.time32("ms")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: time32[ms]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    pyarrow_type = pa.time32("s")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: time32[s]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_time64_us_to_iceberg() -> None:
    pyarrow_type = pa.time64("us")
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == TimeType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_time64_ns_to_iceberg() -> None:
    pyarrow_type = pa.time64("ns")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: time64[ns]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_timestamp_to_iceberg() -> None:
    pyarrow_type = pa.timestamp(unit="us")
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == TimestampType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_timestamp_invalid_units() -> None:
    pyarrow_type = pa.timestamp(unit="ms")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[ms]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    pyarrow_type = pa.timestamp(unit="s")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[s]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    pyarrow_type = pa.timestamp(unit="ns")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[ns]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_timestamp_tz_to_iceberg() -> None:
    pyarrow_type = pa.timestamp(unit="us", tz="UTC")
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == TimestamptzType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_timestamp_tz_invalid_units() -> None:
    pyarrow_type = pa.timestamp(unit="ms", tz="UTC")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[ms, tz=UTC]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    pyarrow_type = pa.timestamp(unit="s", tz="UTC")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[s, tz=UTC]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    pyarrow_type = pa.timestamp(unit="ns", tz="UTC")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[ns, tz=UTC]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_timestamp_tz_invalid_tz() -> None:
    pyarrow_type = pa.timestamp(unit="us", tz="US/Pacific")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[us, tz=US/Pacific]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_string_to_iceberg() -> None:
    pyarrow_type = pa.string()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == StringType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_variable_binary_to_iceberg() -> None:
    pyarrow_type = pa.binary()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == BinaryType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_struct_to_iceberg() -> None:
    pyarrow_struct = pa.struct(
        [
            pa.field("foo", pa.string(), nullable=True, metadata={"PYTHON:field_id": "1", "PYTHON:field_doc": "foo doc"}),
            pa.field("bar", pa.int32(), nullable=False, metadata={"PYTHON:field_id": "2"}),
            pa.field("baz", pa.bool_(), nullable=True, metadata={"PYTHON:field_id": "3"}),
        ]
    )
    expected = StructType(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False, doc="foo doc"),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
    )
    assert visit_pyarrow(pyarrow_struct, _ConvertToIceberg()) == expected


def test_pyarrow_list_to_iceberg() -> None:
    pyarrow_list = pa.list_(pa.field("element", pa.int32(), nullable=False, metadata={"PYTHON:field_id": "1"}))
    expected = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit_pyarrow(pyarrow_list, _ConvertToIceberg()) == expected


def test_pyarrow_map_to_iceberg() -> None:
    pyarrow_map = pa.map_(
        pa.field("key", pa.int32(), nullable=False, metadata={"PYTHON:field_id": "1"}),
        pa.field("value", pa.string(), nullable=False, metadata={"PYTHON:field_id": "2"}),
    )
    expected = MapType(
        key_id=1,
        key_type=IntegerType(),
        value_id=2,
        value_type=StringType(),
        value_required=True,
    )
    assert visit_pyarrow(pyarrow_map, _ConvertToIceberg()) == expected


def test_round_schema_conversion_simple(table_schema_simple: Schema) -> None:
    actual = str(pyarrow_to_schema(schema_to_pyarrow(table_schema_simple)))
    expected = """table {
  1: foo: optional string
  2: bar: required int
  3: baz: optional boolean
}"""
    assert actual == expected


def test_round_schema_conversion_nested(table_schema_nested: Schema) -> None:
    actual = str(pyarrow_to_schema(schema_to_pyarrow(table_schema_nested)))
    expected = """table {
  1: foo: optional string
  2: bar: required int
  3: baz: optional boolean
  4: qux: required list<string>
  6: quux: required map<string, map<string, int>>
  11: location: required list<struct<13: latitude: optional float, 14: longitude: optional float>>
  15: person: optional struct<16: name: optional string, 17: age: required int>
}"""
    assert actual == expected

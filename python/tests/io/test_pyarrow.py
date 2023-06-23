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

import os
import tempfile
from typing import Any, List, Optional
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import FileType, LocalFileSystem

from pyiceberg.avro.resolver import ResolveError
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundNotStartsWith,
    BoundReference,
    BoundStartsWith,
    GreaterThan,
    Not,
    Or,
    literal,
)
from pyiceberg.io import InputStream, OutputStream, load_file_io
from pyiceberg.io.pyarrow import (
    PyArrowFile,
    PyArrowFileIO,
    _ConvertToArrowSchema,
    _read_deletes,
    expression_to_pyarrow,
    project_table,
    schema_to_pyarrow,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema, visit
from pyiceberg.table import FileScanTask, Table
from pyiceberg.table.metadata import TableMetadataV2
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


def test_pyarrow_input_file() -> None:
    """Test reading a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowFileIO().new_input(location=f"{absolute_file_location}")

        # Test opening and reading the file
        r = input_file.open(seekable=False)
        assert isinstance(r, InputStream)  # Test that the file object abides by the InputStream protocol
        data = r.read()
        assert data == b"foo"
        assert len(input_file) == 3
        with pytest.raises(OSError) as exc_info:
            r.seek(0, 0)
        assert "only valid on seekable files" in str(exc_info.value)


def test_pyarrow_input_file_seekable() -> None:
    """Test reading a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowFileIO().new_input(location=f"{absolute_file_location}")

        # Test opening and reading the file
        r = input_file.open(seekable=True)
        assert isinstance(r, InputStream)  # Test that the file object abides by the InputStream protocol
        data = r.read()
        assert data == b"foo"
        assert len(input_file) == 3
        r.seek(0, 0)
        data = r.read()
        assert data == b"foo"
        assert len(input_file) == 3


def test_pyarrow_output_file() -> None:
    """Test writing a file using PyArrowFile"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = PyArrowFileIO().new_output(location=f"{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        assert isinstance(f, OutputStream)  # Test that the file object abides by the OutputStream protocol
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


def test_pyarrow_invalid_scheme() -> None:
    """Test that a ValueError is raised if a location is provided with an invalid scheme"""

    with pytest.raises(ValueError) as exc_info:
        PyArrowFileIO().new_input("foo://bar/baz.txt")

    assert "Unrecognized filesystem type in URI" in str(exc_info.value)

    with pytest.raises(ValueError) as exc_info:
        PyArrowFileIO().new_output("foo://bar/baz.txt")

    assert "Unrecognized filesystem type in URI" in str(exc_info.value)


def test_pyarrow_violating_input_stream_protocol() -> None:
    """Test that a TypeError is raised if an input file is provided that violates the InputStream protocol"""

    # Missing seek, tell, closed, and close
    input_file_mock = MagicMock(spec=["read"])

    # Create a mocked filesystem that returns input_file_mock
    filesystem_mock = MagicMock()
    filesystem_mock.open_input_file.return_value = input_file_mock

    input_file = PyArrowFile("foo.txt", path="foo.txt", fs=filesystem_mock)

    f = input_file.open()
    assert not isinstance(f, InputStream)


def test_pyarrow_violating_output_stream_protocol() -> None:
    """Test that a TypeError is raised if an output stream is provided that violates the OutputStream protocol"""

    # Missing closed, and close
    output_file_mock = MagicMock(spec=["write", "exists"])
    output_file_mock.exists.return_value = False

    file_info_mock = MagicMock()
    file_info_mock.type = FileType.NotFound

    # Create a mocked filesystem that returns output_file_mock
    filesystem_mock = MagicMock()
    filesystem_mock.open_output_stream.return_value = output_file_mock
    filesystem_mock.get_file_info.return_value = file_info_mock

    output_file = PyArrowFile("foo.txt", path="foo.txt", fs=filesystem_mock)

    f = output_file.create()

    assert not isinstance(f, OutputStream)


def test_raise_on_opening_a_local_file_not_found() -> None:
    """Test that a PyArrowFile raises appropriately when a local file is not found"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        f = PyArrowFileIO().new_input(file_location)

        with pytest.raises(FileNotFoundError) as exc_info:
            f.open()

        assert "[Errno 2] Failed to open local file" in str(exc_info.value)


def test_raise_on_opening_an_s3_file_no_permission() -> None:
    """Test that opening a PyArrowFile raises a PermissionError when the pyarrow error includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.open_input_file.side_effect = OSError("AWS Error [code 15]")

    f = PyArrowFile("s3://foo/bar.txt", path="foo/bar.txt", fs=s3fs_mock)

    with pytest.raises(PermissionError) as exc_info:
        f.open()

    assert "Cannot open file, access denied:" in str(exc_info.value)


def test_raise_on_opening_an_s3_file_not_found() -> None:
    """Test that a PyArrowFile raises a FileNotFoundError when the pyarrow error includes 'Path does not exist'"""

    s3fs_mock = MagicMock()
    s3fs_mock.open_input_file.side_effect = OSError("Path does not exist")

    f = PyArrowFile("s3://foo/bar.txt", path="foo/bar.txt", fs=s3fs_mock)

    with pytest.raises(FileNotFoundError) as exc_info:
        f.open()

    assert "Cannot open file, does not exist:" in str(exc_info.value)


@patch("pyiceberg.io.pyarrow.PyArrowFile.exists", return_value=False)
def test_raise_on_creating_an_s3_file_no_permission(_: Any) -> None:
    """Test that creating a PyArrowFile raises a PermissionError when the pyarrow error includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.open_output_stream.side_effect = OSError("AWS Error [code 15]")

    f = PyArrowFile("s3://foo/bar.txt", path="foo/bar.txt", fs=s3fs_mock)

    with pytest.raises(PermissionError) as exc_info:
        f.create()

    assert "Cannot create file, access denied:" in str(exc_info.value)


def test_deleting_s3_file_no_permission() -> None:
    """Test that a PyArrowFile raises a PermissionError when the pyarrow OSError includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.delete_file.side_effect = OSError("AWS Error [code 15]")

    with patch.object(PyArrowFileIO, "_get_fs") as submocked:
        submocked.return_value = s3fs_mock

        with pytest.raises(PermissionError) as exc_info:
            PyArrowFileIO().delete("s3://foo/bar.txt")

    assert "Cannot delete file, access denied:" in str(exc_info.value)


def test_deleting_s3_file_not_found() -> None:
    """Test that a PyArrowFile raises a PermissionError when the pyarrow error includes 'AWS Error [code 15]'"""

    s3fs_mock = MagicMock()
    s3fs_mock.delete_file.side_effect = OSError("Path does not exist")

    with patch.object(PyArrowFileIO, "_get_fs") as submocked:
        submocked.return_value = s3fs_mock

        with pytest.raises(FileNotFoundError) as exc_info:
            PyArrowFileIO().delete("s3://foo/bar.txt")

        assert "Cannot delete file, does not exist:" in str(exc_info.value)


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
            pa.field("foo", pa.string(), nullable=True, metadata={"field_id": "1"}),
            pa.field("bar", pa.int32(), nullable=False, metadata={"field_id": "2"}),
            pa.field("baz", pa.bool_(), nullable=True, metadata={"field_id": "3"}),
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


@pytest.fixture
def bound_reference(table_schema_simple: Schema) -> BoundReference[str]:
    return BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))


@pytest.fixture
def bound_double_reference() -> BoundReference[float]:
    schema = Schema(
        NestedField(field_id=1, name="foo", field_type=DoubleType(), required=False),
        schema_id=1,
        identifier_field_ids=[2],
    )
    return BoundReference(schema.find_field(1), schema.accessor_for_field(1))


def test_expr_is_null_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundIsNull(bound_reference)))
        == "<pyarrow.compute.Expression is_null(foo, {nan_is_null=false})>"
    )


def test_expr_not_null_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotNull(bound_reference))) == "<pyarrow.compute.Expression is_valid(foo)>"


def test_expr_is_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundIsNaN(bound_double_reference))) == "<pyarrow.compute.Expression is_nan(foo)>"


def test_expr_not_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotNaN(bound_double_reference))) == "<pyarrow.compute.Expression invert(is_nan(foo))>"


def test_expr_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundEqualTo(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo == "hello")>'
    )


def test_expr_not_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotEqualTo(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo != "hello")>'
    )


def test_expr_greater_than_or_equal_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundGreaterThanOrEqual(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo >= "hello")>'
    )


def test_expr_greater_than_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundGreaterThan(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo > "hello")>'
    )


def test_expr_less_than_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundLessThan(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo < "hello")>'
    )


def test_expr_less_than_or_equal_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundLessThanOrEqual(bound_reference, literal("hello"))))
        == '<pyarrow.compute.Expression (foo <= "hello")>'
    )


def test_expr_in_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundIn(bound_reference, {literal("hello"), literal("world")}))) in (
        """<pyarrow.compute.Expression is_in(foo, {value_set=string:[
  "world",
  "hello"
], skip_nulls=false})>""",
        """<pyarrow.compute.Expression is_in(foo, {value_set=string:[
  "hello",
  "world"
], skip_nulls=false})>""",
    )


def test_expr_not_in_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(BoundNotIn(bound_reference, {literal("hello"), literal("world")}))) in (
        """<pyarrow.compute.Expression invert(is_in(foo, {value_set=string:[
  "world",
  "hello"
], skip_nulls=false}))>""",
        """<pyarrow.compute.Expression invert(is_in(foo, {value_set=string:[
  "hello",
  "world"
], skip_nulls=false}))>""",
    )


def test_expr_starts_with_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundStartsWith(bound_reference, literal("he"))))
        == '<pyarrow.compute.Expression starts_with(foo, {pattern="he", ignore_case=false})>'
    )


def test_expr_not_starts_with_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotStartsWith(bound_reference, literal("he"))))
        == '<pyarrow.compute.Expression invert(starts_with(foo, {pattern="he", ignore_case=false}))>'
    )


def test_and_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(And(BoundEqualTo(bound_reference, literal("hello")), BoundIsNull(bound_reference))))
        == '<pyarrow.compute.Expression ((foo == "hello") and is_null(foo, {nan_is_null=false}))>'
    )


def test_or_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(Or(BoundEqualTo(bound_reference, literal("hello")), BoundIsNull(bound_reference))))
        == '<pyarrow.compute.Expression ((foo == "hello") or is_null(foo, {nan_is_null=false}))>'
    )


def test_not_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(Not(BoundEqualTo(bound_reference, literal("hello")))))
        == '<pyarrow.compute.Expression invert((foo == "hello"))>'
    )


def test_always_true_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(AlwaysTrue())) == "<pyarrow.compute.Expression true>"


def test_always_false_to_pyarrow(bound_reference: BoundReference[str]) -> None:
    assert repr(expression_to_pyarrow(AlwaysFalse())) == "<pyarrow.compute.Expression false>"


@pytest.fixture
def schema_int() -> Schema:
    return Schema(NestedField(1, "id", IntegerType(), required=False))


@pytest.fixture
def schema_int_str() -> Schema:
    return Schema(NestedField(1, "id", IntegerType(), required=False), NestedField(2, "data", StringType(), required=False))


@pytest.fixture
def schema_str() -> Schema:
    return Schema(NestedField(2, "data", StringType(), required=False))


@pytest.fixture
def schema_long() -> Schema:
    return Schema(NestedField(3, "id", LongType(), required=False))


@pytest.fixture
def schema_struct() -> Schema:
    return Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType()),
                NestedField(42, "long", DoubleType()),
            ),
        )
    )


@pytest.fixture
def schema_list() -> Schema:
    return Schema(
        NestedField(5, "ids", ListType(51, IntegerType(), element_required=False), required=False),
    )


@pytest.fixture
def schema_list_of_structs() -> Schema:
    return Schema(
        NestedField(
            5,
            "locations",
            ListType(
                51,
                StructType(NestedField(511, "lat", DoubleType()), NestedField(512, "long", DoubleType())),
                element_required=False,
            ),
            required=False,
        ),
    )


@pytest.fixture
def schema_map() -> Schema:
    return Schema(
        NestedField(
            5,
            "properties",
            MapType(
                key_id=51,
                key_type=StringType(),
                value_id=52,
                value_type=StringType(),
                value_required=True,
            ),
            required=False,
        ),
    )


def _write_table_to_file(filepath: str, schema: pa.Schema, table: pa.Table) -> str:
    with pq.ParquetWriter(filepath, schema) as writer:
        writer.write_table(table)
    return filepath


@pytest.fixture
def file_int(schema_int: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_int), metadata={"iceberg.schema": schema_int.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/a.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array([0, 1, 2])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_int_str(schema_int_str: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_int_str), metadata={"iceberg.schema": schema_int_str.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/a.parquet",
        pyarrow_schema,
        pa.Table.from_arrays([pa.array([0, 1, 2]), pa.array(["0", "1", "2"])], schema=pyarrow_schema),
    )


@pytest.fixture
def file_string(schema_str: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_str), metadata={"iceberg.schema": schema_str.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/b.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array(["0", "1", "2"])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_long(schema_long: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_long), metadata={"iceberg.schema": schema_long.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/c.parquet", pyarrow_schema, pa.Table.from_arrays([pa.array([0, 1, 2])], schema=pyarrow_schema)
    )


@pytest.fixture
def file_struct(schema_struct: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_struct), metadata={"iceberg.schema": schema_struct.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/d.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"location": {"lat": 52.371807, "long": 4.896029}},
                {"location": {"lat": 52.387386, "long": 4.646219}},
                {"location": {"lat": 52.078663, "long": 4.288788}},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_list(schema_list: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_list), metadata={"iceberg.schema": schema_list.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"ids": list(range(1, 10))},
                {"ids": list(range(2, 20))},
                {"ids": list(range(3, 30))},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_list_of_structs(schema_list_of_structs: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(
        schema_to_pyarrow(schema_list_of_structs), metadata={"iceberg.schema": schema_list_of_structs.json()}
    )
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"locations": [{"lat": 52.371807, "long": 4.896029}, {"lat": 52.387386, "long": 4.646219}]},
                {"locations": []},
                {"locations": [{"lat": 52.078663, "long": 4.288788}, {"lat": 52.387386, "long": 4.646219}]},
            ],
            schema=pyarrow_schema,
        ),
    )


@pytest.fixture
def file_map(schema_map: Schema, tmpdir: str) -> str:
    pyarrow_schema = pa.schema(schema_to_pyarrow(schema_map), metadata={"iceberg.schema": schema_map.json()})
    return _write_table_to_file(
        f"file:{tmpdir}/e.parquet",
        pyarrow_schema,
        pa.Table.from_pylist(
            [
                {"properties": [("a", "b")]},
                {"properties": [("c", "d")]},
                {"properties": [("e", "f"), ("g", "h")]},
            ],
            schema=pyarrow_schema,
        ),
    )


def project(
    schema: Schema, files: List[str], expr: Optional[BooleanExpression] = None, table_schema: Optional[Schema] = None
) -> pa.Table:
    return project_table(
        [
            FileScanTask(
                DataFile(
                    content=DataFileContent.DATA,
                    file_path=file,
                    file_format=FileFormat.PARQUET,
                    partition={},
                    record_count=3,
                    file_size_in_bytes=3,
                )
            )
            for file in files
        ],
        Table(
            ("namespace", "table"),
            metadata=TableMetadataV2(
                location="file://a/b/",
                last_column_id=1,
                format_version=2,
                schemas=[table_schema or schema],
                partition_specs=[PartitionSpec()],
            ),
            metadata_location="file://a/b/c.json",
            io=PyArrowFileIO(),
            catalog=NoopCatalog("NoopCatalog"),
        ),
        expr or AlwaysTrue(),
        schema,
        case_sensitive=True,
    )


def test_projection_add_column(file_int: str) -> None:
    schema = Schema(
        # All new IDs
        NestedField(10, "id", IntegerType(), required=False),
        NestedField(20, "list", ListType(21, IntegerType(), element_required=False), required=False),
        NestedField(
            30,
            "map",
            MapType(key_id=31, key_type=IntegerType(), value_id=32, value_type=StringType(), value_required=False),
            required=False,
        ),
        NestedField(
            40,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False), NestedField(42, "lon", DoubleType(), required=False)
            ),
            required=False,
        ),
    )
    result_table = project(schema, [file_int])

    for col in result_table.columns:
        assert len(col) == 3

    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[1], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[2], [None, None, None]):
        assert actual.as_py() == expected

    for actual, expected in zip(result_table.columns[3], [None, None, None]):
        assert actual.as_py() == expected
    assert (
        repr(result_table.schema)
        == """id: int32
list: list<element: int32>
  child 0, element: int32
    -- field metadata --
    field_id: '21'
map: map<int32, string>
  child 0, entries: struct<key: int32 not null, value: string> not null
      child 0, key: int32 not null
      -- field metadata --
      field_id: '31'
      child 1, value: string
      -- field metadata --
      field_id: '32'
location: struct<lat: double, lon: double>
  child 0, lat: double
    -- field metadata --
    field_id: '41'
  child 1, lon: double
    -- field metadata --
    field_id: '42'"""
    )


def test_read_list(schema_list: Schema, file_list: str) -> None:
    result_table = project(schema_list, [file_list])

    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [list(range(1, 10)), list(range(2, 20)), list(range(3, 30))]):
        assert actual.as_py() == expected

    assert repr(result_table.schema) == "ids: list<item: int32>\n  child 0, item: int32"


def test_read_map(schema_map: Schema, file_map: str) -> None:
    result_table = project(schema_map, [file_map])

    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [[("a", "b")], [("c", "d")], [("e", "f"), ("g", "h")]]):
        assert actual.as_py() == expected

    assert (
        repr(result_table.schema)
        == """properties: map<string, string>
  child 0, entries: struct<key: string not null, value: string> not null
      child 0, key: string not null
      child 1, value: string"""
    )


def test_projection_add_column_struct(schema_int: Schema, file_int: str) -> None:
    schema = Schema(
        # A new ID
        NestedField(
            2,
            "id",
            MapType(key_id=3, key_type=IntegerType(), value_id=4, value_type=StringType(), value_required=False),
            required=False,
        )
    )
    result_table = project(schema, [file_int])
    # Everything should be None
    for r in result_table.columns[0]:
        assert r.as_py() is None
    assert (
        repr(result_table.schema)
        == """id: map<int32, string>
  child 0, entries: struct<key: int32 not null, value: string> not null
      child 0, key: int32 not null
      -- field metadata --
      field_id: '3'
      child 1, value: string
      -- field metadata --
      field_id: '4'"""
    )


def test_projection_add_column_struct_required(file_int: str) -> None:
    schema = Schema(
        # A new ID
        NestedField(
            2,
            "other_id",
            IntegerType(),
            required=True,
        )
    )
    with pytest.raises(ResolveError) as exc_info:
        _ = project(schema, [file_int])
    assert "Field is required, and could not be found in the file: 2: other_id: required int" in str(exc_info.value)


def test_projection_rename_column(schema_int: Schema, file_int: str) -> None:
    schema = Schema(
        # Reuses the id 1
        NestedField(1, "other_name", IntegerType())
    )
    result_table = project(schema, [file_int])
    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(result_table.columns[0], [0, 1, 2]):
        assert actual.as_py() == expected

    assert repr(result_table.schema) == "other_name: int32 not null"


def test_projection_concat_files(schema_int: Schema, file_int: str) -> None:
    result_table = project(schema_int, [file_int, file_int])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2, 0, 1, 2]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 6
    assert repr(result_table.schema) == "id: int32"


def test_projection_filter(schema_int: Schema, file_int: str) -> None:
    result_table = project(schema_int, [file_int], GreaterThan("id", 4))
    assert len(result_table.columns[0]) == 0
    assert (
        repr(result_table.schema)
        == """id: int32
  -- field metadata --
  field_id: '1'"""
    )


def test_projection_filter_renamed_column(file_int: str) -> None:
    schema = Schema(
        # Reuses the id 1
        NestedField(1, "other_id", IntegerType())
    )
    result_table = project(schema, [file_int], GreaterThan("other_id", 1))
    assert len(result_table.columns[0]) == 1
    assert repr(result_table.schema) == "other_id: int32 not null"


def test_projection_filter_add_column(schema_int: Schema, file_int: str, file_string: str) -> None:
    """We have one file that has the column, and the other one doesn't"""
    result_table = project(schema_int, [file_int, file_string])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2, None, None, None]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 6
    assert repr(result_table.schema) == "id: int32"


def test_projection_filter_add_column_promote(file_int: str) -> None:
    schema_long = Schema(NestedField(1, "id", LongType()))
    result_table = project(schema_long, [file_int])

    for actual, expected in zip(result_table.columns[0], [0, 1, 2]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "id: int64 not null"


def test_projection_filter_add_column_demote(file_long: str) -> None:
    schema_int = Schema(NestedField(3, "id", IntegerType()))
    with pytest.raises(ResolveError) as exc_info:
        _ = project(schema_int, [file_long])
    assert "Cannot promote long to int" in str(exc_info.value)


def test_projection_nested_struct_subset(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType()),
                # long is missing!
            ),
        )
    )

    result_table = project(schema, [file_struct])

    for actual, expected in zip(result_table.columns[0], [52.371807, 52.387386, 52.078663]):
        assert actual.as_py() == {"lat": expected}

    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "location: struct<lat: double not null> not null\n  child 0, lat: double not null"


def test_projection_nested_new_field(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(43, "null", DoubleType(), required=False),  # Whoa, this column doesn't exist in the file
            ),
        )
    )

    result_table = project(schema, [file_struct])

    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == {"null": expected}
    assert len(result_table.columns[0]) == 3
    assert repr(result_table.schema) == "location: struct<null: double> not null\n  child 0, null: double"


def test_projection_nested_struct(schema_struct: Schema, file_struct: str) -> None:
    schema = Schema(
        NestedField(
            4,
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False),
                NestedField(43, "null", DoubleType(), required=False),
                NestedField(42, "long", DoubleType(), required=False),
            ),
        )
    )

    result_table = project(schema, [file_struct])
    for actual, expected in zip(
        result_table.columns[0],
        [
            {"lat": 52.371807, "long": 4.896029, "null": None},
            {"lat": 52.387386, "long": 4.646219, "null": None},
            {"lat": 52.078663, "long": 4.288788, "null": None},
        ],
    ):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == "location: struct<lat: double, null: double, long: double> not null\n  child 0, lat: double\n  child 1, null: double\n  child 2, long: double"
    )


def test_projection_list_of_structs(schema_list_of_structs: Schema, file_list_of_structs: str) -> None:
    schema = Schema(
        NestedField(
            5,
            "locations",
            ListType(
                51,
                StructType(
                    NestedField(511, "latitude", DoubleType()),
                    NestedField(512, "longitude", DoubleType()),
                    NestedField(513, "altitude", DoubleType(), required=False),
                ),
                element_required=False,
            ),
            required=False,
        ),
    )

    result_table = project(schema, [file_list_of_structs])
    assert len(result_table.columns) == 1
    assert len(result_table.columns[0]) == 3
    for actual, expected in zip(
        result_table.columns[0],
        [
            [
                {"latitude": 52.371807, "longitude": 4.896029, "altitude": None},
                {"latitude": 52.387386, "longitude": 4.646219, "altitude": None},
            ],
            [],
            [
                {"latitude": 52.078663, "longitude": 4.288788, "altitude": None},
                {"latitude": 52.387386, "longitude": 4.646219, "altitude": None},
            ],
        ],
    ):
        assert actual.as_py() == expected
    assert (
        repr(result_table.schema)
        == """locations: list<item: struct<latitude: double not null, longitude: double not null, altitude: double>>
  child 0, item: struct<latitude: double not null, longitude: double not null, altitude: double>
      child 0, latitude: double not null
      child 1, longitude: double not null
      child 2, altitude: double"""
    )


def test_projection_nested_struct_different_parent_id(file_struct: str) -> None:
    schema = Schema(
        NestedField(
            5,  # 😱 this is 4 in the file, this will be fixed when projecting the file schema
            "location",
            StructType(
                NestedField(41, "lat", DoubleType(), required=False), NestedField(42, "long", DoubleType(), required=False)
            ),
            required=False,
        )
    )

    result_table = project(schema, [file_struct])
    for actual, expected in zip(result_table.columns[0], [None, None, None]):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 3
    assert (
        repr(result_table.schema)
        == """location: struct<lat: double, long: double>
  child 0, lat: double
    -- field metadata --
    field_id: '41'
  child 1, long: double
    -- field metadata --
    field_id: '42'"""
    )


def test_projection_filter_on_unprojected_field(schema_int_str: Schema, file_int_str: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType()))

    result_table = project(schema, [file_int_str], GreaterThan("data", "1"), schema_int_str)

    for actual, expected in zip(
        result_table.columns[0],
        [2],
    ):
        assert actual.as_py() == expected
    assert len(result_table.columns[0]) == 1
    assert repr(result_table.schema) == "id: int32 not null"


def test_projection_filter_on_unknown_field(schema_int_str: Schema, file_int_str: str) -> None:
    schema = Schema(NestedField(1, "id", IntegerType()))

    with pytest.raises(ValueError) as exc_info:
        _ = project(schema, [file_int_str], GreaterThan("unknown_field", "1"), schema_int_str)

    assert "Could not find field with name unknown_field, case_sensitive=True" in str(exc_info.value)


@pytest.fixture
def deletes_file(tmp_path: str, example_task: FileScanTask) -> str:
    path = example_task.file.file_path
    table = pa.table({"file_path": [path, path, path], "pos": [1, 3, 5]})

    deletes_file_path = f"{tmp_path}/deletes.parquet"
    pq.write_table(table, deletes_file_path)

    return deletes_file_path


def test_read_deletes(deletes_file: str, example_task: FileScanTask) -> None:
    deletes = _read_deletes(LocalFileSystem(), DataFile(file_path=deletes_file, file_format=FileFormat.PARQUET))
    assert set(deletes.keys()) == {example_task.file.file_path}
    assert list(deletes.values())[0] == pa.chunked_array([[1, 3, 5]])


def test_delete(deletes_file: str, example_task: FileScanTask, table_schema_simple: Schema) -> None:
    metadata_location = "file://a/b/c.json"
    example_task_with_delete = FileScanTask(
        data_file=example_task.file,
        delete_files={DataFile(content=DataFileContent.POSITION_DELETES, file_path=deletes_file, file_format=FileFormat.PARQUET)},
    )

    with_deletes = project_table(
        tasks=[example_task_with_delete],
        table=Table(
            ("namespace", "table"),
            metadata=TableMetadataV2(
                location=metadata_location,
                last_column_id=1,
                format_version=2,
                current_schema_id=1,
                schemas=[table_schema_simple],
                partition_specs=[PartitionSpec()],
            ),
            metadata_location=metadata_location,
            io=load_file_io(),
            catalog=NoopCatalog("noop"),
        ),
        row_filter=AlwaysTrue(),
        projected_schema=table_schema_simple,
    )

    assert (
        str(with_deletes)
        == """pyarrow.Table
foo: string
bar: int64 not null
baz: bool
----
foo: [["a","c"]]
bar: [[1,3]]
baz: [[true,null]]"""
    )


def test_delete_duplicates(deletes_file: str, example_task: FileScanTask, table_schema_simple: Schema) -> None:
    metadata_location = "file://a/b/c.json"
    example_task_with_delete = FileScanTask(
        data_file=example_task.file,
        delete_files={
            DataFile(content=DataFileContent.POSITION_DELETES, file_path=deletes_file, file_format=FileFormat.PARQUET),
            DataFile(content=DataFileContent.POSITION_DELETES, file_path=deletes_file, file_format=FileFormat.PARQUET),
        },
    )

    with_deletes = project_table(
        tasks=[example_task_with_delete],
        table=Table(
            ("namespace", "table"),
            metadata=TableMetadataV2(
                location=metadata_location,
                last_column_id=1,
                format_version=2,
                current_schema_id=1,
                schemas=[table_schema_simple],
                partition_specs=[PartitionSpec()],
            ),
            metadata_location=metadata_location,
            io=load_file_io(),
            catalog=NoopCatalog("noop"),
        ),
        row_filter=AlwaysTrue(),
        projected_schema=table_schema_simple,
    )

    assert (
        str(with_deletes)
        == """pyarrow.Table
foo: string
bar: int64 not null
baz: bool
----
foo: [["a","c"]]
bar: [[1,3]]
baz: [[true,null]]"""
    )


def test_pyarrow_wrap_fsspec(example_task: FileScanTask, table_schema_simple: Schema) -> None:
    metadata_location = "file://a/b/c.json"
    projection = project_table(
        [example_task],
        Table(
            ("namespace", "table"),
            metadata=TableMetadataV2(
                location=metadata_location,
                last_column_id=1,
                format_version=2,
                current_schema_id=1,
                schemas=[table_schema_simple],
                partition_specs=[PartitionSpec()],
            ),
            metadata_location=metadata_location,
            io=load_file_io(properties={"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}, location=metadata_location),
            catalog=NoopCatalog("NoopCatalog"),
        ),
        case_sensitive=True,
        projected_schema=table_schema_simple,
        row_filter=AlwaysTrue(),
    )

    assert (
        str(projection)
        == """pyarrow.Table
foo: string
bar: int64 not null
baz: bool
----
foo: [["a","b","c"]]
bar: [[1,2,3]]
baz: [[true,false,null]]"""
    )

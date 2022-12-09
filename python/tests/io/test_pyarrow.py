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
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyarrow.fs import FileType

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
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
    BoundReference,
    Not,
    Or,
    literal,
)
from pyiceberg.io import InputStream, OutputStream
from pyiceberg.io.pyarrow import (
    PyArrowFile,
    PyArrowFileIO,
    _ConvertToArrowSchema,
    expression_to_pyarrow,
    schema_to_pyarrow,
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
        r = input_file.open()
        assert isinstance(r, InputStream)  # Test that the file object abides by the InputStream protocol
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


def test_raise_on_opening_a_local_file_no_permission() -> None:
    """Test that a PyArrowFile raises appropriately when opening a local file without permission"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chmod(tmpdirname, 0o600)
        file_location = os.path.join(tmpdirname, "foo.txt")
        f = PyArrowFileIO().new_input(file_location)

        with pytest.raises(PermissionError) as exc_info:
            f.open()

        assert "[Errno 13] Failed to open local file" in str(exc_info.value)


def test_raise_on_checking_if_local_file_exists_no_permission() -> None:
    """Test that a PyArrowFile raises when checking for existence on a file without permission"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chmod(tmpdirname, 0o600)
        file_location = os.path.join(tmpdirname, "foo.txt")
        f = PyArrowFileIO().new_input(file_location)

        with pytest.raises(PermissionError) as exc_info:
            f.create()

        assert "Cannot get file info, access denied:" in str(exc_info.value)


def test_raise_on_creating_a_local_file_no_permission() -> None:
    """Test that a PyArrowFile raises appropriately when creating a local file without permission"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chmod(tmpdirname, 0o600)
        file_location = os.path.join(tmpdirname, "foo.txt")
        f = PyArrowFileIO().new_input(file_location)

        with pytest.raises(PermissionError) as exc_info:
            f.create()

        assert "Cannot get file info, access denied:" in str(exc_info.value)


def test_raise_on_delete_file_with_no_permission() -> None:
    """Test that a PyArrowFile raises when deleting a local file without permission"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.chmod(tmpdirname, 0o600)
        file_location = os.path.join(tmpdirname, "foo.txt")
        file_io = PyArrowFileIO()

        with pytest.raises(PermissionError) as exc_info:
            file_io.delete(file_location)

        assert "Cannot delete file" in str(exc_info.value)


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
bar: int32 not null
baz: bool
qux: list<item: string> not null
  child 0, item: string
quux: map<string, map<string, int32>> not null
  child 0, entries: struct<key: string not null, value: map<string, int32>> not null
      child 0, key: string not null
      child 1, value: map<string, int32>
          child 0, entries: struct<key: string not null, value: int32> not null
              child 0, key: string not null
              child 1, value: int32
location: list<item: struct<latitude: float, longitude: float>> not null
  child 0, item: struct<latitude: float, longitude: float>
      child 0, latitude: float
      child 1, longitude: float
person: struct<name: string, age: int32 not null>
  child 0, name: string
  child 1, age: int32 not null"""
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
    assert visit(iceberg_type, _ConvertToArrowSchema()) == pa.timestamp(unit="us", tz="+00:00")


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
    assert visit(iceberg_map, _ConvertToArrowSchema()) == pa.map_(pa.int32(), pa.string())


def test_list_type_to_pyarrow() -> None:
    iceberg_map = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit(iceberg_map, _ConvertToArrowSchema()) == pa.list_(pa.int32())


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
    assert (
        repr(expression_to_pyarrow(BoundIsNaN(bound_double_reference)))
        == "<pyarrow.compute.Expression (is_null(foo, {nan_is_null=true}) and is_valid(foo))>"
    )


def test_expr_not_nan_to_pyarrow(bound_double_reference: BoundReference[str]) -> None:
    assert (
        repr(expression_to_pyarrow(BoundNotNaN(bound_double_reference)))
        == "<pyarrow.compute.Expression invert((is_null(foo, {nan_is_null=true}) and is_valid(foo)))>"
    )


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

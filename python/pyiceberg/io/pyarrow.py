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
"""FileIO implementation for reading and writing table files that uses pyarrow.fs

This file contains a FileIO implementation that relies on the filesystem interface provided
by PyArrow. It relies on PyArrow's `from_uri` method that infers the correct filesystem
type to use. Theoretically, this allows the supported storage types to grow naturally
with the pyarrow library.
"""

import os
from functools import lru_cache, singledispatch
from typing import (
    Any,
    Callable,
    List,
    Set,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow.fs import (
    FileInfo,
    FileSystem,
    FileType,
    LocalFileSystem,
    S3FileSystem,
)

from pyiceberg.expressions import BooleanExpression, BoundTerm, Literal
from pyiceberg.expressions.visitors import BoundBooleanExpressionVisitor
from pyiceberg.expressions.visitors import visit as boolean_expression_visit
from pyiceberg.io import (
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)
from pyiceberg.schema import Schema, SchemaVisitor, visit
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation that uses a pyarrow filesystem to generate pyarrow.lib.NativeFile instances

    Args:
        location(str): A URI or a path to a local file

    Attributes:
        location(str): The URI or path to a local file for a PyArrowFile instance

    Examples:
        >>> from pyiceberg.io.pyarrow import PyArrowFile
        >>> # input_file = PyArrowFile("s3://foo/bar.txt")
        >>> # Read the contents of the PyArrowFile instance
        >>> # Make sure that you have permissions to read/write
        >>> # file_content = input_file.open().read()

        >>> # output_file = PyArrowFile("s3://baz/qux.txt")
        >>> # Write bytes to a file
        >>> # Make sure that you have permissions to read/write
        >>> # output_file.create().write(b'foobytes')
    """

    def __init__(self, location: str, path: str, fs: FileSystem):
        self._filesystem = fs
        self._path = path
        super().__init__(location=location)

    def _file_info(self) -> FileInfo:
        """Retrieves a pyarrow.fs.FileInfo object for the location

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        try:
            file_info = self._filesystem.get_file_info(self._path)
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot get file info, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error

        if file_info.type == FileType.NotFound:
            raise FileNotFoundError(f"Cannot get file info, file not found: {self.location}")
        return file_info

    def __len__(self) -> int:
        """Returns the total length of the file, in bytes"""
        file_info = self._file_info()
        return file_info.size

    def exists(self) -> bool:
        """Checks whether the location exists"""
        try:
            self._file_info()  # raises FileNotFoundError if it does not exist
            return True
        except FileNotFoundError:
            return False

    def open(self) -> InputStream:
        """Opens the location using a PyArrow FileSystem inferred from the location

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at `self.location`

        Raises:
            FileNotFoundError: If the file at self.location does not exist
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        try:
            input_file = self._filesystem.open_input_file(self._path)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot open file, does not exist: {self.location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot open file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return input_file

    def create(self, overwrite: bool = False) -> OutputStream:
        """Creates a writable pyarrow.lib.NativeFile for this PyArrowFile's location

        Args:
            overwrite(bool): Whether to overwrite the file if it already exists

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at self.location

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False

        Note:
            This retrieves a pyarrow NativeFile by opening an output stream. If overwrite is set to False,
            a check is first performed to verify that the file does not exist. This is not thread-safe and
            a possibility does exist that the file can be created by a concurrent process after the existence
            check yet before the output stream is created. In such a case, the default pyarrow behavior will
            truncate the contents of the existing file when opening the output stream.
        """
        try:
            if not overwrite and self.exists() is True:
                raise FileExistsError(f"Cannot create file, already exists: {self.location}")
            output_file = self._filesystem.open_output_stream(self._path)
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot create file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return output_file

    def to_input_file(self) -> "PyArrowFile":
        """Returns a new PyArrowFile for the location of an existing PyArrowFile instance

        This method is included to abide by the OutputFile abstract base class. Since this implementation uses a single
        PyArrowFile class (as opposed to separate InputFile and OutputFile implementations), this method effectively returns
        a copy of the same instance.
        """
        return self


class PyArrowFileIO(FileIO):
    def __init__(self, properties: Properties = EMPTY_DICT):
        self.get_fs: Callable[[str], FileSystem] = lru_cache(self._get_fs)
        super().__init__(properties=properties)

    @staticmethod
    def parse_location(location: str) -> Tuple[str, str]:
        """Returns the path without the scheme"""
        uri = urlparse(location)
        return uri.scheme or "file", os.path.abspath(location) if not uri.scheme else f"{uri.netloc}{uri.path}"

    def _get_fs(self, scheme: str) -> FileSystem:
        if scheme in {"s3", "s3a", "s3n"}:
            client_kwargs = {
                "endpoint_override": self.properties.get("s3.endpoint"),
                "access_key": self.properties.get("s3.access-key-id"),
                "secret_key": self.properties.get("s3.secret-access-key"),
                "session_token": self.properties.get("s3.session-token"),
            }
            return S3FileSystem(**client_kwargs)
        elif scheme == "file":
            return LocalFileSystem()
        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    def new_input(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to read bytes from the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location
        """
        scheme, path = self.parse_location(location)
        fs = self._get_fs(scheme)
        return PyArrowFile(fs=fs, location=location, path=path)

    def new_output(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to write bytes to the file at the given location

        Args:
            location(str): A URI or a path to a local file

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location
        """
        scheme, path = self.parse_location(location)
        fs = self._get_fs(scheme)
        return PyArrowFile(fs=fs, location=location, path=path)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location

        Args:
            location(str, InputFile, OutputFile): The URI to the file--if an InputFile instance or an
            OutputFile instance is provided, the location attribute for that instance is used as the location
            to delete

        Raises:
            FileNotFoundError: When the file at the provided location does not exist
            PermissionError: If the file at the provided location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        str_location = location.location if isinstance(location, (InputFile, OutputFile)) else location
        scheme, path = self.parse_location(str_location)
        fs = self._get_fs(scheme)

        try:
            fs.delete_file(path)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot delete file, does not exist: {location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot delete file, access denied: {location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error


def schema_to_pyarrow(schema: Schema) -> pa.schema:
    return visit(schema, _ConvertToArrowSchema())


class _ConvertToArrowSchema(SchemaVisitor[pa.DataType]):
    def schema(self, _: Schema, struct_result: pa.StructType) -> pa.schema:
        return pa.schema(list(struct_result))

    def struct(self, _: StructType, field_results: List[pa.DataType]) -> pa.DataType:
        return pa.struct(field_results)

    def field(self, field: NestedField, field_result: pa.DataType) -> pa.Field:
        return pa.field(
            name=field.name,
            type=field_result,
            nullable=not field.required,
            metadata={"doc": field.doc, "id": str(field.field_id)} if field.doc else {},
        )

    def list(self, _: ListType, element_result: pa.DataType) -> pa.DataType:
        return pa.list_(value_type=element_result)

    def map(self, _: MapType, key_result: pa.DataType, value_result: pa.DataType) -> pa.DataType:
        return pa.map_(key_type=key_result, item_type=value_result)

    def primitive(self, primitive: PrimitiveType) -> pa.DataType:
        return _iceberg_to_pyarrow_type(primitive)


@singledispatch
def _iceberg_to_pyarrow_type(primitive: PrimitiveType) -> pa.DataType:
    raise ValueError(f"Unknown type: {primitive}")


@_iceberg_to_pyarrow_type.register
def _(primitive: FixedType) -> pa.DataType:
    return pa.binary(len(primitive))


@_iceberg_to_pyarrow_type.register
def _(primitive: DecimalType) -> pa.DataType:
    return pa.decimal128(primitive.precision, primitive.scale)


@_iceberg_to_pyarrow_type.register
def _(_: BooleanType) -> pa.DataType:
    return pa.bool_()


@_iceberg_to_pyarrow_type.register
def _(_: IntegerType) -> pa.DataType:
    return pa.int32()


@_iceberg_to_pyarrow_type.register
def _(_: LongType) -> pa.DataType:
    return pa.int64()


@_iceberg_to_pyarrow_type.register
def _(_: FloatType) -> pa.DataType:
    # 32-bit IEEE 754 floating point
    return pa.float32()


@_iceberg_to_pyarrow_type.register
def _(_: DoubleType) -> pa.DataType:
    # 64-bit IEEE 754 floating point
    return pa.float64()


@_iceberg_to_pyarrow_type.register
def _(_: DateType) -> pa.DataType:
    # Date encoded as an int
    return pa.date32()


@_iceberg_to_pyarrow_type.register
def _(_: TimeType) -> pa.DataType:
    return pa.time64("us")


@_iceberg_to_pyarrow_type.register
def _(_: TimestampType) -> pa.DataType:
    return pa.timestamp(unit="us")


@_iceberg_to_pyarrow_type.register
def _(_: TimestamptzType) -> pa.DataType:
    return pa.timestamp(unit="us", tz="+00:00")


@_iceberg_to_pyarrow_type.register
def _(_: StringType) -> pa.DataType:
    return pa.string()


@_iceberg_to_pyarrow_type.register
def _(_: BinaryType) -> pa.DataType:
    # Variable length by default
    return pa.binary()


def _convert_scalar(value: Any, iceberg_type: IcebergType) -> pa.scalar:
    if not isinstance(iceberg_type, PrimitiveType):
        raise ValueError(f"Expected primitive type, got: {iceberg_type}")
    return pa.scalar(value).cast(_iceberg_to_pyarrow_type(iceberg_type))


class _ConvertToArrowExpression(BoundBooleanExpressionVisitor[pc.Expression]):
    def visit_in(self, term: BoundTerm[pc.Expression], literals: Set[Any]) -> pc.Expression:
        pyarrow_literals = pa.array(literals, type=_iceberg_to_pyarrow_type(term.ref().field.field_type))
        return pc.field(term.ref().field.name).isin(pyarrow_literals)

    def visit_not_in(self, term: BoundTerm[pc.Expression], literals: Set[Any]) -> pc.Expression:
        pyarrow_literals = pa.array(literals, type=_iceberg_to_pyarrow_type(term.ref().field.field_type))
        return ~pc.field(term.ref().field.name).isin(pyarrow_literals)

    def visit_is_nan(self, term: BoundTerm[Any]) -> pc.Expression:
        ref = pc.field(term.ref().field.name)
        return ref.is_null(nan_is_null=True) & ref.is_valid()

    def visit_not_nan(self, term: BoundTerm[Any]) -> pc.Expression:
        ref = pc.field(term.ref().field.name)
        return ~(ref.is_null(nan_is_null=True) & ref.is_valid())

    def visit_is_null(self, term: BoundTerm[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name).is_null(nan_is_null=False)

    def visit_not_null(self, term: BoundTerm[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name).is_valid()

    def visit_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) == _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_not_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) != _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_greater_than_or_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) >= _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_greater_than(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) > _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_less_than(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) < _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_less_than_or_equal(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.field(term.ref().field.name) <= _convert_scalar(literal.value, term.ref().field.field_type)

    def visit_true(self) -> pc.Expression:
        return pc.scalar(True)

    def visit_false(self) -> pc.Expression:
        return pc.scalar(False)

    def visit_not(self, child_result: pc.Expression) -> pc.Expression:
        return ~child_result

    def visit_and(self, left_result: pc.Expression, right_result: pc.Expression) -> pc.Expression:
        return left_result & right_result

    def visit_or(self, left_result: pc.Expression, right_result: pc.Expression) -> pc.Expression:
        return left_result | right_result


def expression_to_pyarrow(expr: BooleanExpression) -> pc.Expression:
    return boolean_expression_visit(expr, _ConvertToArrowExpression())

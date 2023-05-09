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
# pylint: disable=redefined-outer-name,arguments-renamed,fixme
"""FileIO implementation for reading and writing table files that uses pyarrow.fs

This file contains a FileIO implementation that relies on the filesystem interface provided
by PyArrow. It relies on PyArrow's `from_uri` method that infers the correct filesystem
type to use. Theoretically, this allows the supported storage types to grow naturally
with the pyarrow library.
"""
from __future__ import annotations

import multiprocessing
import os
from abc import ABC, abstractmethod
from functools import lru_cache, singledispatch
from multiprocessing.pool import ThreadPool
from multiprocessing.sharedctypes import Synchronized
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow.fs import (
    FileInfo,
    FileSystem,
    FileType,
    FSSpecHandler,
    LocalFileSystem,
    PyFileSystem,
    S3FileSystem,
)

from pyiceberg.avro.resolver import ResolveError
from pyiceberg.expressions import (
    AlwaysTrue,
    BooleanExpression,
    BoundTerm,
    Literal,
)
from pyiceberg.expressions.visitors import (
    BoundBooleanExpressionVisitor,
    bind,
    extract_field_ids,
    translate_column_names,
)
from pyiceberg.expressions.visitors import visit as boolean_expression_visit
from pyiceberg.io import (
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)
from pyiceberg.schema import (
    PartnerAccessor,
    Schema,
    SchemaVisitorPerPrimitiveType,
    SchemaWithPartnerVisitor,
    promote,
    prune_columns,
    visit,
    visit_with_partner,
)
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
    UUIDType,
)
from pyiceberg.utils.singleton import Singleton

if TYPE_CHECKING:
    from pyiceberg.table import FileScanTask, Table

ONE_MEGABYTE = 1024 * 1024
BUFFER_SIZE = "buffer-size"
ICEBERG_SCHEMA = b"iceberg.schema"
FIELD_ID = "field_id"
DOC = "doc"
PYARROW_FIELD_ID_KEYS = [b"PARQUET:field_id", b"field_id"]
PYARROW_FIELD_DOC_KEYS = [b"PARQUET:field_doc", b"field_doc", b"doc"]

T = TypeVar("T")


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation that uses a pyarrow filesystem to generate pyarrow.lib.NativeFile instances

    Args:
        location (str): A URI or a path to a local file

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

    _fs: FileSystem
    _path: str
    _buffer_size: int

    def __init__(self, location: str, path: str, fs: FileSystem, buffer_size: int = ONE_MEGABYTE):
        self._filesystem = fs
        self._path = path
        self._buffer_size = buffer_size
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

    def open(self, seekable: bool = True) -> InputStream:
        """Opens the location using a PyArrow FileSystem inferred from the location

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at `self.location`

        Raises:
            FileNotFoundError: If the file at self.location does not exist
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15
        """
        try:
            if seekable:
                input_file = self._filesystem.open_input_file(self._path)
            else:
                input_file = self._filesystem.open_input_stream(self._path, buffer_size=self._buffer_size)
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
            overwrite (bool): Whether to overwrite the file if it already exists

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
            output_file = self._filesystem.open_output_stream(self._path, buffer_size=self._buffer_size)
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot create file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return output_file

    def to_input_file(self) -> PyArrowFile:
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
                "endpoint_override": self.properties.get(S3_ENDPOINT),
                "access_key": self.properties.get(S3_ACCESS_KEY_ID),
                "secret_key": self.properties.get(S3_SECRET_ACCESS_KEY),
                "session_token": self.properties.get(S3_SESSION_TOKEN),
                "region": self.properties.get(S3_REGION),
            }
            return S3FileSystem(**client_kwargs)
        elif scheme == "file":
            return LocalFileSystem()
        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    def new_input(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to read bytes from the file at the given location

        Args:
            location (str): A URI or a path to a local file

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location
        """
        scheme, path = self.parse_location(location)
        fs = self._get_fs(scheme)
        return PyArrowFile(fs=fs, location=location, path=path, buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)))

    def new_output(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to write bytes to the file at the given location

        Args:
            location (str): A URI or a path to a local file

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location
        """
        scheme, path = self.parse_location(location)
        fs = self._get_fs(scheme)
        return PyArrowFile(fs=fs, location=location, path=path, buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)))

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location

        Args:
            location (Union[str, InputFile, OutputFile]): The URI to the file--if an InputFile instance or an OutputFile instance is provided,
                the location attribute for that instance is used as the location to delete

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


def schema_to_pyarrow(schema: Union[Schema, IcebergType]) -> pa.schema:
    return visit(schema, _ConvertToArrowSchema())


class _ConvertToArrowSchema(SchemaVisitorPerPrimitiveType[pa.DataType], Singleton):
    def schema(self, _: Schema, struct_result: pa.StructType) -> pa.schema:
        return pa.schema(list(struct_result))

    def struct(self, _: StructType, field_results: List[pa.DataType]) -> pa.DataType:
        return pa.struct(field_results)

    def field(self, field: NestedField, field_result: pa.DataType) -> pa.Field:
        return pa.field(
            name=field.name,
            type=field_result,
            nullable=field.optional,
            metadata={DOC: field.doc, FIELD_ID: str(field.field_id)} if field.doc else {FIELD_ID: str(field.field_id)},
        )

    def list(self, list_type: ListType, element_result: pa.DataType) -> pa.DataType:
        element_field = self.field(list_type.element_field, element_result)
        return pa.list_(value_type=element_field)

    def map(self, map_type: MapType, key_result: pa.DataType, value_result: pa.DataType) -> pa.DataType:
        key_field = self.field(map_type.key_field, key_result)
        value_field = self.field(map_type.value_field, value_result)
        return pa.map_(key_type=key_field, item_type=value_field)

    def visit_fixed(self, fixed_type: FixedType) -> pa.DataType:
        return pa.binary(len(fixed_type))

    def visit_decimal(self, decimal_type: DecimalType) -> pa.DataType:
        return pa.decimal128(decimal_type.precision, decimal_type.scale)

    def visit_boolean(self, _: BooleanType) -> pa.DataType:
        return pa.bool_()

    def visit_integer(self, _: IntegerType) -> pa.DataType:
        return pa.int32()

    def visit_long(self, _: LongType) -> pa.DataType:
        return pa.int64()

    def visit_float(self, _: FloatType) -> pa.DataType:
        # 32-bit IEEE 754 floating point
        return pa.float32()

    def visit_double(self, _: DoubleType) -> pa.DataType:
        # 64-bit IEEE 754 floating point
        return pa.float64()

    def visit_date(self, _: DateType) -> pa.DataType:
        # Date encoded as an int
        return pa.date32()

    def visit_time(self, _: TimeType) -> pa.DataType:
        return pa.time64("us")

    def visit_timestamp(self, _: TimestampType) -> pa.DataType:
        return pa.timestamp(unit="us")

    def visit_timestampz(self, _: TimestamptzType) -> pa.DataType:
        return pa.timestamp(unit="us", tz="UTC")

    def visit_string(self, _: StringType) -> pa.DataType:
        return pa.string()

    def visit_uuid(self, _: UUIDType) -> pa.DataType:
        return pa.binary(16)

    def visit_binary(self, _: BinaryType) -> pa.DataType:
        return pa.binary()


def _convert_scalar(value: Any, iceberg_type: IcebergType) -> pa.scalar:
    if not isinstance(iceberg_type, PrimitiveType):
        raise ValueError(f"Expected primitive type, got: {iceberg_type}")
    return pa.scalar(value).cast(schema_to_pyarrow(iceberg_type))


class _ConvertToArrowExpression(BoundBooleanExpressionVisitor[pc.Expression]):
    def visit_in(self, term: BoundTerm[pc.Expression], literals: Set[Any]) -> pc.Expression:
        pyarrow_literals = pa.array(literals, type=schema_to_pyarrow(term.ref().field.field_type))
        return pc.field(term.ref().field.name).isin(pyarrow_literals)

    def visit_not_in(self, term: BoundTerm[pc.Expression], literals: Set[Any]) -> pc.Expression:
        pyarrow_literals = pa.array(literals, type=schema_to_pyarrow(term.ref().field.field_type))
        return ~pc.field(term.ref().field.name).isin(pyarrow_literals)

    def visit_is_nan(self, term: BoundTerm[Any]) -> pc.Expression:
        ref = pc.field(term.ref().field.name)
        return pc.is_nan(ref)

    def visit_not_nan(self, term: BoundTerm[Any]) -> pc.Expression:
        ref = pc.field(term.ref().field.name)
        return ~pc.is_nan(ref)

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

    def visit_starts_with(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return pc.starts_with(pc.field(term.ref().field.name), literal.value)

    def visit_not_starts_with(self, term: BoundTerm[Any], literal: Literal[Any]) -> pc.Expression:
        return ~pc.starts_with(pc.field(term.ref().field.name), literal.value)

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


def pyarrow_to_schema(schema: pa.Schema) -> Schema:
    visitor = _ConvertToIceberg()
    return visit_pyarrow(schema, visitor)


@singledispatch
def visit_pyarrow(obj: pa.DataType | pa.Schema, visitor: PyArrowSchemaVisitor[T]) -> T:
    """A generic function for applying a pyarrow schema visitor to any point within a schema

    The function traverses the schema in post-order fashion

    Args:
        obj(pa.DataType): An instance of a Schema or an IcebergType
        visitor (PyArrowSchemaVisitor[T]): An instance of an implementation of the generic PyarrowSchemaVisitor base class

    Raises:
        NotImplementedError: If attempting to visit an unrecognized object type
    """
    raise NotImplementedError("Cannot visit non-type: %s" % obj)


@visit_pyarrow.register(pa.Schema)
def _(obj: pa.Schema, visitor: PyArrowSchemaVisitor[T]) -> Optional[T]:
    struct_results: List[Optional[T]] = []
    for field in obj:
        visitor.before_field(field)
        struct_result = visit_pyarrow(field.type, visitor)
        visitor.after_field(field)
        struct_results.append(struct_result)

    return visitor.schema(obj, struct_results)


@visit_pyarrow.register(pa.StructType)
def _(obj: pa.StructType, visitor: PyArrowSchemaVisitor[T]) -> Optional[T]:
    struct_results: List[Optional[T]] = []
    for field in obj:
        visitor.before_field(field)
        struct_result = visit_pyarrow(field.type, visitor)
        visitor.after_field(field)
        struct_results.append(struct_result)

    return visitor.struct(obj, struct_results)


@visit_pyarrow.register(pa.ListType)
def _(obj: pa.ListType, visitor: PyArrowSchemaVisitor[T]) -> Optional[T]:
    visitor.before_field(obj.value_field)
    list_result = visit_pyarrow(obj.value_field.type, visitor)
    visitor.after_field(obj.value_field)
    return visitor.list(obj, list_result)


@visit_pyarrow.register(pa.MapType)
def _(obj: pa.MapType, visitor: PyArrowSchemaVisitor[T]) -> Optional[T]:
    visitor.before_field(obj.key_field)
    key_result = visit_pyarrow(obj.key_field.type, visitor)
    visitor.after_field(obj.key_field)
    visitor.before_field(obj.item_field)
    value_result = visit_pyarrow(obj.item_field.type, visitor)
    visitor.after_field(obj.item_field)
    return visitor.map(obj, key_result, value_result)


@visit_pyarrow.register(pa.DataType)
def _(obj: pa.DataType, visitor: PyArrowSchemaVisitor[T]) -> Optional[T]:
    if pa.types.is_nested(obj):
        raise TypeError(f"Expected primitive type, got: {type(obj)}")
    return visitor.primitive(obj)


class PyArrowSchemaVisitor(Generic[T], ABC):
    def before_field(self, field: pa.Field) -> None:
        """Override this method to perform an action immediately before visiting a field."""

    def after_field(self, field: pa.Field) -> None:
        """Override this method to perform an action immediately after visiting a field."""

    @abstractmethod
    def schema(self, schema: pa.Schema, field_results: List[Optional[T]]) -> Optional[T]:
        """visit a schema"""

    @abstractmethod
    def struct(self, struct: pa.StructType, field_results: List[Optional[T]]) -> Optional[T]:
        """visit a struct"""

    @abstractmethod
    def list(self, list_type: pa.ListType, element_result: Optional[T]) -> Optional[T]:
        """visit a list"""

    @abstractmethod
    def map(self, map_type: pa.MapType, key_result: Optional[T], value_result: Optional[T]) -> Optional[T]:
        """visit a map"""

    @abstractmethod
    def primitive(self, primitive: pa.DataType) -> Optional[T]:
        """visit a primitive type"""


def _get_field_id(field: pa.Field) -> Optional[int]:
    for pyarrow_field_id_key in PYARROW_FIELD_ID_KEYS:
        if field_id_str := field.metadata.get(pyarrow_field_id_key):
            return int(field_id_str.decode())
    return None


def _get_field_doc(field: pa.Field) -> Optional[str]:
    for pyarrow_doc_key in PYARROW_FIELD_DOC_KEYS:
        if doc_str := field.metadata.get(pyarrow_doc_key):
            return doc_str.decode()
    return None


class _ConvertToIceberg(PyArrowSchemaVisitor[Union[IcebergType, Schema]]):
    def _convert_fields(self, arrow_fields: Iterable[pa.Field], field_results: List[Optional[IcebergType]]) -> List[NestedField]:
        fields = []
        for i, field in enumerate(arrow_fields):
            field_id = _get_field_id(field)
            field_doc = _get_field_doc(field)
            field_type = field_results[i]
            if field_type is not None and field_id is not None:
                fields.append(NestedField(field_id, field.name, field_type, required=not field.nullable, doc=field_doc))
        return fields

    def schema(self, schema: pa.Schema, field_results: List[Optional[IcebergType]]) -> Schema:
        return Schema(*self._convert_fields(schema, field_results))

    def struct(self, struct: pa.StructType, field_results: List[Optional[IcebergType]]) -> IcebergType:
        return StructType(*self._convert_fields(struct, field_results))

    def list(self, list_type: pa.ListType, element_result: Optional[IcebergType]) -> Optional[IcebergType]:
        element_field = list_type.value_field
        element_id = _get_field_id(element_field)
        if element_result is not None and element_id is not None:
            return ListType(element_id, element_result, element_required=not element_field.nullable)
        return None

    def map(
        self, map_type: pa.MapType, key_result: Optional[IcebergType], value_result: Optional[IcebergType]
    ) -> Optional[IcebergType]:
        key_field = map_type.key_field
        key_id = _get_field_id(key_field)
        value_field = map_type.item_field
        value_id = _get_field_id(value_field)
        if key_result is not None and value_result is not None and key_id is not None and value_id is not None:
            return MapType(key_id, key_result, value_id, value_result, value_required=not value_field.nullable)
        return None

    def primitive(self, primitive: pa.DataType) -> IcebergType:
        if pa.types.is_boolean(primitive):
            return BooleanType()
        elif pa.types.is_int32(primitive):
            return IntegerType()
        elif pa.types.is_int64(primitive):
            return LongType()
        elif pa.types.is_float32(primitive):
            return FloatType()
        elif pa.types.is_float64(primitive):
            return DoubleType()
        elif isinstance(primitive, pa.Decimal128Type):
            primitive = cast(pa.Decimal128Type, primitive)
            return DecimalType(primitive.precision, primitive.scale)
        elif pa.types.is_string(primitive):
            return StringType()
        elif pa.types.is_date32(primitive):
            return DateType()
        elif isinstance(primitive, pa.Time64Type) and primitive.unit == "us":
            return TimeType()
        elif pa.types.is_timestamp(primitive):
            primitive = cast(pa.TimestampType, primitive)
            if primitive.unit == "us":
                if primitive.tz == "UTC" or primitive.tz == "+00:00":
                    return TimestamptzType()
                elif primitive.tz is None:
                    return TimestampType()
        elif pa.types.is_binary(primitive):
            return BinaryType()
        elif pa.types.is_fixed_size_binary(primitive):
            primitive = cast(pa.FixedSizeBinaryType, primitive)
            return FixedType(primitive.byte_width)

        raise TypeError(f"Unsupported type: {primitive}")


def _file_to_table(
    fs: FileSystem,
    task: FileScanTask,
    bound_row_filter: BooleanExpression,
    projected_schema: Schema,
    projected_field_ids: Set[int],
    case_sensitive: bool,
    rows_counter: Synchronized[int],
    limit: Optional[int] = None,
) -> Optional[pa.Table]:
    if limit and rows_counter.value >= limit:
        return None

    _, path = PyArrowFileIO.parse_location(task.file.file_path)
    arrow_format = ds.ParquetFileFormat(pre_buffer=True, buffer_size=(ONE_MEGABYTE * 8))
    with fs.open_input_file(path) as fin:
        fragment = arrow_format.make_fragment(fin)
        physical_schema = fragment.physical_schema
        schema_raw = None
        if metadata := physical_schema.metadata:
            schema_raw = metadata.get(ICEBERG_SCHEMA)
        # TODO: if field_ids are not present, Name Mapping should be implemented to look them up in the table schema,
        #  see https://github.com/apache/iceberg/issues/7451
        file_schema = Schema.parse_raw(schema_raw) if schema_raw is not None else pyarrow_to_schema(physical_schema)

        pyarrow_filter = None
        if bound_row_filter is not AlwaysTrue():
            translated_row_filter = translate_column_names(bound_row_filter, file_schema, case_sensitive=case_sensitive)
            bound_file_filter = bind(file_schema, translated_row_filter, case_sensitive=case_sensitive)
            pyarrow_filter = expression_to_pyarrow(bound_file_filter)

        file_project_schema = prune_columns(file_schema, projected_field_ids, select_full_types=False)

        if file_schema is None:
            raise ValueError(f"Missing Iceberg schema in Metadata for file: {path}")

        fragment_scanner = ds.Scanner.from_fragment(
            fragment=fragment,
            schema=physical_schema,
            filter=pyarrow_filter,
            columns=[col.name for col in file_project_schema.columns],
        )

        if limit:
            arrow_table = fragment_scanner.head(limit)
            with rows_counter.get_lock():
                if rows_counter.value >= limit:
                    return None
                rows_counter.value += len(arrow_table)
        else:
            arrow_table = fragment_scanner.to_table()

        # If there is no data, we don't have to go through the schema
        if len(arrow_table) > 0:
            return to_requested_schema(projected_schema, file_project_schema, arrow_table)
        else:
            return None


def project_table(
    tasks: Iterable[FileScanTask],
    table: Table,
    row_filter: BooleanExpression,
    projected_schema: Schema,
    case_sensitive: bool,
    limit: Optional[int] = None,
) -> pa.Table:
    """Resolves the right columns based on the identifier

    Args:
        tasks (Iterable[FileScanTask]): A URI or a path to a local file
        table (Table): The table that's being queried
        row_filter (BooleanExpression): The expression for filtering rows
        projected_schema (Schema): The output schema
        case_sensitive (bool): Case sensitivity when looking up column names

    Raises:
        ResolveError: When an incompatible query is done
    """

    scheme, _ = PyArrowFileIO.parse_location(table.location())
    if isinstance(table.io, PyArrowFileIO):
        fs = table.io.get_fs(scheme)
    else:
        try:
            from pyiceberg.io.fsspec import FsspecFileIO

            if isinstance(table.io, FsspecFileIO):
                fs = PyFileSystem(FSSpecHandler(table.io.get_fs(scheme)))
            else:
                raise ValueError(f"Expected PyArrowFileIO or FsspecFileIO, got: {table.io}")
        except ModuleNotFoundError as e:
            # When FsSpec is not installed
            raise ValueError(f"Expected PyArrowFileIO or FsspecFileIO, got: {table.io}") from e

    bound_row_filter = bind(table.schema(), row_filter, case_sensitive=case_sensitive)

    projected_field_ids = {
        id for id in projected_schema.field_ids if not isinstance(projected_schema.find_type(id), (MapType, ListType))
    }.union(extract_field_ids(bound_row_filter))

    rows_counter = multiprocessing.Value("i", 0)

    with ThreadPool() as pool:
        tables = [
            table
            for table in pool.starmap(
                func=_file_to_table,
                iterable=[
                    (fs, task, bound_row_filter, projected_schema, projected_field_ids, case_sensitive, rows_counter, limit)
                    for task in tasks
                ],
                chunksize=None,  # we could use this to control how to materialize the generator of tasks (we should also make the expression above lazy)
            )
            if table is not None
        ]

    if len(tables) > 1:
        final_table = pa.concat_tables(tables)
    elif len(tables) == 1:
        final_table = tables[0]
    else:
        final_table = pa.Table.from_batches([], schema=schema_to_pyarrow(projected_schema))

    return final_table.slice(0, limit)


def to_requested_schema(requested_schema: Schema, file_schema: Schema, table: pa.Table) -> pa.Table:
    struct_array = visit_with_partner(requested_schema, table, ArrowProjectionVisitor(file_schema), ArrowAccessor(file_schema))

    arrays = []
    fields = []
    for pos, field in enumerate(requested_schema.fields):
        array = struct_array.field(pos)
        arrays.append(array)
        fields.append(pa.field(field.name, array.type, field.optional))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


class ArrowProjectionVisitor(SchemaWithPartnerVisitor[pa.Array, Optional[pa.Array]]):
    file_schema: Schema

    def __init__(self, file_schema: Schema):
        self.file_schema = file_schema

    def cast_if_needed(self, field: NestedField, values: pa.Array) -> pa.Array:
        file_field = self.file_schema.find_field(field.field_id)
        if field.field_type.is_primitive and field.field_type != file_field.field_type:
            return values.cast(schema_to_pyarrow(promote(file_field.field_type, field.field_type)))
        return values

    def schema(self, schema: Schema, schema_partner: Optional[pa.Array], struct_result: Optional[pa.Array]) -> Optional[pa.Array]:
        return struct_result

    def struct(
        self, struct: StructType, struct_array: Optional[pa.Array], field_results: List[Optional[pa.Array]]
    ) -> Optional[pa.Array]:
        if struct_array is None:
            return None
        field_arrays: List[pa.Array] = []
        fields: List[pa.Field] = []
        for field, field_array in zip(struct.fields, field_results):
            if field_array is not None:
                array = self.cast_if_needed(field, field_array)
                field_arrays.append(array)
                fields.append(pa.field(field.name, array.type, field.optional))
            elif field.optional:
                arrow_type = schema_to_pyarrow(field.field_type)
                field_arrays.append(pa.nulls(len(struct_array), type=arrow_type))
                fields.append(pa.field(field.name, arrow_type, field.optional))
            else:
                raise ResolveError(f"Field is required, and could not be found in the file: {field}")

        return pa.StructArray.from_arrays(arrays=field_arrays, fields=pa.struct(fields))

    def field(self, field: NestedField, _: Optional[pa.Array], field_array: Optional[pa.Array]) -> Optional[pa.Array]:
        return field_array

    def list(self, list_type: ListType, list_array: Optional[pa.Array], value_array: Optional[pa.Array]) -> Optional[pa.Array]:
        return (
            pa.ListArray.from_arrays(list_array.offsets, self.cast_if_needed(list_type.element_field, value_array))
            if isinstance(list_array, pa.ListArray)
            else None
        )

    def map(
        self, map_type: MapType, map_array: Optional[pa.Array], key_result: Optional[pa.Array], value_result: Optional[pa.Array]
    ) -> Optional[pa.Array]:
        return (
            pa.MapArray.from_arrays(
                map_array.offsets,
                self.cast_if_needed(map_type.key_field, key_result),
                self.cast_if_needed(map_type.value_field, value_result),
            )
            if isinstance(map_array, pa.MapArray)
            else None
        )

    def primitive(self, _: PrimitiveType, array: Optional[pa.Array]) -> Optional[pa.Array]:
        return array


class ArrowAccessor(PartnerAccessor[pa.Array]):
    file_schema: Schema

    def __init__(self, file_schema: Schema):
        self.file_schema = file_schema

    def schema_partner(self, partner: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner

    def field_partner(self, partner_struct: Optional[pa.Array], field_id: int, _: str) -> Optional[pa.Array]:
        if partner_struct:
            # use the field name from the file schema
            try:
                name = self.file_schema.find_field(field_id).name
            except ValueError:
                return None

            if isinstance(partner_struct, pa.StructArray):
                return partner_struct.field(name)
            elif isinstance(partner_struct, pa.Table):
                return partner_struct.column(name).combine_chunks()

        return None

    def list_element_partner(self, partner_list: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner_list.values if isinstance(partner_list, pa.ListArray) else None

    def map_key_partner(self, partner_map: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner_map.keys if isinstance(partner_map, pa.MapArray) else None

    def map_value_partner(self, partner_map: Optional[pa.Array]) -> Optional[pa.Array]:
        return partner_map.items if isinstance(partner_map, pa.MapArray) else None

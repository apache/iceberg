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
# pylint: disable=redefined-outer-name
"""FileIO implementation for reading and writing table files that uses pyarrow.fs

This file contains a FileIO implementation that relies on the filesystem interface provided
by PyArrow. It relies on PyArrow's `from_uri` method that infers the correct filesystem
type to use. Theoretically, this allows the supported storage types to grow naturally
with the pyarrow library.
"""
from __future__ import annotations

import os
from functools import lru_cache, singledispatchmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.fs import (
    FileInfo,
    FileSystem,
    FileType,
    LocalFileSystem,
    S3FileSystem,
)

from pyiceberg.avro.resolver import ResolveException, promote
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
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)
from pyiceberg.schema import (
    Schema,
    SchemaVisitorPerPrimitiveType,
    prune_columns,
    visit,
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

ICEBERG_SCHEMA = b"iceberg.schema"


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


def schema_to_pyarrow(schema: Union[Schema, IcebergType]) -> pa.schema:
    return visit(schema, _ConvertToArrowSchema())


class UuidType(pa.PyExtensionType):
    """Custom type for UUID

    For more information:
    https://arrow.apache.org/docs/python/extending_types.html#defining-extension-types-user-defined-types
    """

    def __init__(self) -> None:
        pa.PyExtensionType.__init__(self, pa.binary(16))

    def __reduce__(self) -> Tuple[pa.PyExtensionType, Tuple[Any, ...]]:
        return UuidType, ()


class _ConvertToArrowSchema(SchemaVisitorPerPrimitiveType[pa.DataType], Singleton):
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
        return pa.timestamp(unit="us", tz="+00:00")

    def visit_string(self, _: StringType) -> pa.DataType:
        return pa.string()

    def visit_uuid(self, _: UUIDType) -> pa.DataType:
        return UuidType()

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


def project_table(
    files: Iterable[FileScanTask], table: Table, row_filter: BooleanExpression, projected_schema: Schema, case_sensitive: bool
) -> pa.Table:
    """Resolves the right columns based on the identifier

    Args:
        files(Iterable[FileScanTask]): A URI or a path to a local file
        table(Table): The table that's being queried
        row_filter(BooleanExpression): The expression for filtering rows
        projected_schema(Schema): The output schema
        case_sensitive(bool): Case sensitivity when looking up column names

    Raises:
        ResolveException: When an incompatible query is done
    """

    if isinstance(table.io, PyArrowFileIO):
        scheme, path = PyArrowFileIO.parse_location(table.location())
        fs = table.io.get_fs(scheme)
    else:
        raise ValueError(f"Expected PyArrowFileIO, got: {table.io}")

    bound_row_filter = bind(table.schema(), row_filter, case_sensitive=case_sensitive)

    projected_field_ids = {
        id for id in projected_schema.field_ids if not isinstance(projected_schema.find_type(id), (MapType, ListType))
    }.union(extract_field_ids(bound_row_filter))

    tables = []
    for task in files:
        _, path = PyArrowFileIO.parse_location(task.file.file_path)

        # Get the schema
        with fs.open_input_file(path) as fout:
            parquet_schema = pq.read_schema(fout)
            schema_raw = parquet_schema.metadata.get(ICEBERG_SCHEMA)
            file_schema = Schema.parse_raw(schema_raw)

        pyarrow_filter = None
        if row_filter is not AlwaysTrue():
            translated_row_filter = translate_column_names(bound_row_filter, file_schema, case_sensitive=case_sensitive)
            bound_row_filter = bind(file_schema, translated_row_filter, case_sensitive=case_sensitive)
            pyarrow_filter = expression_to_pyarrow(bound_row_filter)

        file_project_schema = prune_columns(file_schema, projected_field_ids, select_full_types=False)

        if file_schema is None:
            raise ValueError(f"Missing Iceberg schema in Metadata for file: {path}")

        # Prune the stuff that we don't need anyway
        file_project_schema_arrow = schema_to_pyarrow(file_project_schema)

        arrow_table = ds.dataset(
            source=[path], schema=file_project_schema_arrow, format=ds.ParquetFileFormat(), filesystem=fs
        ).to_table(filter=pyarrow_filter)

        tables.append(to_requested_schema(projected_schema, file_project_schema, arrow_table))

    if len(tables) > 1:
        return pa.concat_tables(tables)
    else:
        return tables[0]


def to_requested_schema(requested_schema: Schema, file_schema: Schema, table: pa.Table) -> pa.Table:
    return VisitWithArrow(requested_schema, file_schema, table).visit()


class VisitWithArrow:
    requested_schema: Schema
    file_schema: Schema
    table: pa.Table

    def __init__(self, requested_schema: Schema, file_schema: Schema, table: pa.Table) -> None:
        self.requested_schema = requested_schema
        self.file_schema = file_schema
        self.table = table

    def visit(self) -> pa.Table:
        return self.visit_with_arrow(self.requested_schema, self.file_schema)

    @singledispatchmethod
    def visit_with_arrow(self, requested_schema: Union[Schema, IcebergType], file_schema: Union[Schema, IcebergType]) -> pa.Table:
        """A generic function for applying a schema visitor to any point within a schema

        The function traverses the schema in post-order fashion

        Args:
            obj(Schema | IcebergType): An instance of a Schema or an IcebergType
            visitor (VisitWithArrow[T]): An instance of an implementation of the generic VisitWithArrow base class

        Raises:
            NotImplementedError: If attempting to visit an unrecognized object type
        """
        raise NotImplementedError(f"Cannot visit non-type: {requested_schema}")

    @visit_with_arrow.register(Schema)
    def _(self, requested_schema: Schema, file_schema: Schema) -> pa.Table:
        """Visit a Schema with a concrete SchemaVisitorWithPartner"""
        struct_result = self.visit_with_arrow(requested_schema.as_struct(), file_schema.as_struct())
        pyarrow_schema = schema_to_pyarrow(requested_schema)
        return pa.Table.from_arrays(struct_result.flatten(), schema=pyarrow_schema)

    def _get_field_by_id(self, field_id: int) -> Optional[NestedField]:
        try:
            return self.file_schema.find_field(field_id)
        except ValueError:
            # Field is not in the file
            return None

    @visit_with_arrow.register(StructType)
    def _(self, requested_struct: StructType, file_struct: Optional[IcebergType]) -> pa.Array:  # pylint: disable=unused-argument
        """Visit a StructType with a concrete SchemaVisitorWithPartner"""
        results = []

        for requested_field in requested_struct.fields:
            file_field = self._get_field_by_id(requested_field.field_id)

            if file_field is None and requested_field.required:
                raise ResolveException(f"Field is required, and could not be found in the file: {requested_field}")

            results.append(self.visit_with_arrow(requested_field.field_type, file_field))

        pyarrow_schema = schema_to_pyarrow(requested_struct)
        return pa.StructArray.from_arrays(arrays=results, fields=pyarrow_schema)

    @visit_with_arrow.register(ListType)
    def _(self, requested_list: ListType, file_field: Optional[NestedField]) -> pa.Array:
        """Visit a ListType with a concrete SchemaVisitorWithPartner"""

        if file_field is not None:
            if not isinstance(file_field.field_type, ListType):
                raise ValueError(f"Expected list, got: {file_field}")

            return self.visit_with_arrow(requested_list.element_type, self._get_field_by_id(file_field.field_type.element_id))
        else:
            # Not in the file, fill in with nulls
            return pa.nulls(len(self.table), type=pa.list_(schema_to_pyarrow(requested_list.element_type)))

    @visit_with_arrow.register(MapType)
    def _(self, requested_map: MapType, file_map: Optional[NestedField]) -> pa.Array:
        """Visit a MapType with a concrete SchemaVisitorWithPartner"""

        if file_map is not None:
            if not isinstance(file_map.field_type, MapType):
                raise ValueError(f"Expected map, got: {file_map}")

            key = self._get_field_by_id(file_map.field_type.key_id)
            return self.visit_with_arrow(requested_map.key_type, key)
        else:
            # Not in the file, fill in with nulls
            return pa.nulls(
                len(self.table),
                type=pa.map_(schema_to_pyarrow(requested_map.key_type), schema_to_pyarrow(requested_map.value_type)),
            )

    def _get_column_data(self, file_field: NestedField) -> pa.Array:
        column_name = self.file_schema.find_column_name(file_field.field_id)
        column_data = self.table
        struct_schema = self.table.schema

        if column_name is None:
            # Should not happen
            raise ValueError(f"Could not find column: {column_name}")

        column_parts = list(reversed(column_name.split(".")))
        while len(column_parts) > 1:
            part = column_parts.pop()
            column_data = column_data.column(part)
            struct_schema = struct_schema[struct_schema.get_field_index(part)].type

        if not isinstance(struct_schema, (pa.ListType, pa.MapType)):
            # PyArrow does not have an element
            idx = struct_schema.get_field_index(column_parts.pop())
            column_data = column_data.flatten()[idx]

        return column_data.combine_chunks()

    @visit_with_arrow.register(PrimitiveType)
    def _(self, requested_primitive: PrimitiveType, file_field: Optional[NestedField]) -> pa.Array:
        """Visit a PrimitiveType with a concrete SchemaVisitorWithPartner"""

        if file_field is not None:
            column_data = self._get_column_data(file_field)

            if requested_primitive != file_field.field_type:
                # To check if the promotion is allowed
                expected = promote(file_field.field_type, requested_primitive)
                column_data = column_data.cast(schema_to_pyarrow(expected))
        else:
            column_data = pa.nulls(len(self.table), schema_to_pyarrow(requested_primitive))

        return column_data

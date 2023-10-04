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
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from enum import Enum
from functools import singledispatch
from types import TracebackType
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Type,
)

from pyiceberg.avro.file import AvroFile, AvroOutputFile
from pyiceberg.conversions import to_bytes
from pyiceberg.exceptions import ValidationError
from pyiceberg.io import FileIO, InputFile, OutputFile
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
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

UNASSIGNED_SEQ = -1
DEFAULT_BLOCK_SIZE = 67108864  # 64 * 1024 * 1024


class DataFileContent(int, Enum):
    DATA = 0
    POSITION_DELETES = 1
    EQUALITY_DELETES = 2

    def __repr__(self) -> str:
        """Return the string representation of the DataFileContent class."""
        return f"DataFileContent.{self.name}"


class ManifestContent(int, Enum):
    DATA = 0
    DELETES = 1

    def __repr__(self) -> str:
        """Return the string representation of the ManifestContent class."""
        return f"ManifestContent.{self.name}"


class ManifestEntryStatus(int, Enum):
    EXISTING = 0
    ADDED = 1
    DELETED = 2

    def __repr__(self) -> str:
        """Return the string representation of the ManifestEntryStatus class."""
        return f"ManifestEntryStatus.{self.name}"


class FileFormat(str, Enum):
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"

    def __repr__(self) -> str:
        """Return the string representation of the FileFormat class."""
        return f"FileFormat.{self.name}"


DATA_FILE_TYPE_V1 = StructType(
    NestedField(
        field_id=134,
        name="content",
        field_type=IntegerType(),
        required=False,
        doc="Contents of the file: 0=data, 1=position deletes, 2=equality deletes",
        initial_default=DataFileContent.DATA,
    ),
    NestedField(field_id=100, name="file_path", field_type=StringType(), required=True, doc="Location URI with FS scheme"),
    NestedField(
        field_id=101,
        name="file_format",
        field_type=StringType(),
        required=True,
        doc="File format name: avro, orc, or parquet",
    ),
    NestedField(
        field_id=102,
        name="partition",
        field_type=StructType(),
        required=True,
        doc="Partition data tuple, schema based on the partition spec",
    ),
    NestedField(field_id=103, name="record_count", field_type=LongType(), required=True, doc="Number of records in the file"),
    NestedField(field_id=104, name="file_size_in_bytes", field_type=LongType(), required=True, doc="Total file size in bytes"),
    NestedField(
        field_id=105,
        name="block_size_in_bytes",
        field_type=LongType(),
        required=False,
        doc="Deprecated. Always write a default in v1. Do not write in v2.",
    ),
    NestedField(
        field_id=108,
        name="column_sizes",
        field_type=MapType(key_id=117, key_type=IntegerType(), value_id=118, value_type=LongType()),
        required=False,
        doc="Map of column id to total size on disk",
    ),
    NestedField(
        field_id=109,
        name="value_counts",
        field_type=MapType(key_id=119, key_type=IntegerType(), value_id=120, value_type=LongType()),
        required=False,
        doc="Map of column id to total count, including null and NaN",
    ),
    NestedField(
        field_id=110,
        name="null_value_counts",
        field_type=MapType(key_id=121, key_type=IntegerType(), value_id=122, value_type=LongType()),
        required=False,
        doc="Map of column id to null value count",
    ),
    NestedField(
        field_id=137,
        name="nan_value_counts",
        field_type=MapType(key_id=138, key_type=IntegerType(), value_id=139, value_type=LongType()),
        required=False,
        doc="Map of column id to number of NaN values in the column",
    ),
    NestedField(
        field_id=125,
        name="lower_bounds",
        field_type=MapType(key_id=126, key_type=IntegerType(), value_id=127, value_type=BinaryType()),
        required=False,
        doc="Map of column id to lower bound",
    ),
    NestedField(
        field_id=128,
        name="upper_bounds",
        field_type=MapType(key_id=129, key_type=IntegerType(), value_id=130, value_type=BinaryType()),
        required=False,
        doc="Map of column id to upper bound",
    ),
    NestedField(field_id=131, name="key_metadata", field_type=BinaryType(), required=False, doc="Encryption key metadata blob"),
    NestedField(
        field_id=132,
        name="split_offsets",
        field_type=ListType(element_id=133, element_type=LongType(), element_required=True),
        required=False,
        doc="Splittable offsets",
    ),
    NestedField(
        field_id=135,
        name="equality_ids",
        field_type=ListType(element_id=136, element_type=LongType(), element_required=True),
        required=False,
        doc="Equality comparison field IDs",
    ),
    NestedField(field_id=140, name="sort_order_id", field_type=IntegerType(), required=False, doc="Sort order ID"),
    NestedField(field_id=141, name="spec_id", field_type=IntegerType(), required=False, doc="Partition spec ID"),
)

DATA_FILE_TYPE_V2 = StructType(*[field for field in DATA_FILE_TYPE_V1.fields if field.field_id != 105])


@singledispatch
def partition_field_to_data_file_partition_field(partition_field_type: IcebergType) -> PrimitiveType:
    raise TypeError(f"Unsupported partition field type: {partition_field_type}")


@partition_field_to_data_file_partition_field.register(LongType)
@partition_field_to_data_file_partition_field.register(DateType)
@partition_field_to_data_file_partition_field.register(TimeType)
@partition_field_to_data_file_partition_field.register(TimestampType)
@partition_field_to_data_file_partition_field.register(TimestamptzType)
def _(partition_field_type: PrimitiveType) -> IntegerType:
    return IntegerType()


@partition_field_to_data_file_partition_field.register(PrimitiveType)
def _(partition_field_type: PrimitiveType) -> PrimitiveType:
    return partition_field_type


def data_file_with_partition(partition_type: StructType, format_version: Literal[1, 2]) -> StructType:
    data_file_partition_type = StructType(
        *[
            NestedField(
                field_id=field.field_id,
                name=field.name,
                field_type=partition_field_to_data_file_partition_field(field.field_type),
            )
            for field in partition_type.fields
        ]
    )

    return StructType(
        *[
            NestedField(
                field_id=102,
                name="partition",
                field_type=data_file_partition_type,
                required=True,
                doc="Partition data tuple, schema based on the partition spec",
            )
            if field.field_id == 102
            else field
            for field in (DATA_FILE_TYPE_V1.fields if format_version == 1 else DATA_FILE_TYPE_V2.fields)
        ]
    )


class DataFile(Record):
    __slots__ = (
        "content",
        "file_path",
        "file_format",
        "partition",
        "record_count",
        "file_size_in_bytes",
        "block_size_in_bytes",
        "column_sizes",
        "value_counts",
        "null_value_counts",
        "nan_value_counts",
        "lower_bounds",
        "upper_bounds",
        "key_metadata",
        "split_offsets",
        "equality_ids",
        "sort_order_id",
        "spec_id",
    )
    content: DataFileContent
    file_path: str
    file_format: FileFormat
    partition: Record
    record_count: int
    file_size_in_bytes: int
    block_size_in_bytes: Optional[int]
    column_sizes: Dict[int, int]
    value_counts: Dict[int, int]
    null_value_counts: Dict[int, int]
    nan_value_counts: Dict[int, int]
    lower_bounds: Dict[int, bytes]
    upper_bounds: Dict[int, bytes]
    key_metadata: Optional[bytes]
    split_offsets: Optional[List[int]]
    equality_ids: Optional[List[int]]
    sort_order_id: Optional[int]
    spec_id: Optional[int]

    def __setattr__(self, name: str, value: Any) -> None:
        """Assign a key/value to a DataFile."""
        # The file_format is written as a string, so we need to cast it to the Enum
        if name == "file_format":
            value = FileFormat[value]
        super().__setattr__(name, value)

    def __init__(self, format_version: Literal[1, 2] = 1, *data: Any, **named_data: Any) -> None:
        super().__init__(
            *data,
            **{"struct": DATA_FILE_TYPE_V1 if format_version == 1 else DATA_FILE_TYPE_V2, **named_data},
        )

    def __hash__(self) -> int:
        """Return the hash of the file path."""
        return hash(self.file_path)

    def __eq__(self, other: Any) -> bool:
        """Compare the datafile with another object.

        If it is a datafile, it will compare based on the file_path.
        """
        return self.file_path == other.file_path if isinstance(other, DataFile) else False


MANIFEST_ENTRY_SCHEMA = Schema(
    NestedField(0, "status", IntegerType(), required=True),
    NestedField(1, "snapshot_id", LongType(), required=False),
    NestedField(3, "data_sequence_number", LongType(), required=False),
    NestedField(4, "file_sequence_number", LongType(), required=False),
    NestedField(2, "data_file", DATA_FILE_TYPE_V1, required=True),
)

MANIFEST_ENTRY_SCHEMA_STRUCT = MANIFEST_ENTRY_SCHEMA.as_struct()


def manifest_entry_schema_with_data_file(data_file: StructType) -> Schema:
    return Schema(
        *[
            NestedField(2, "data_file", data_file, required=True) if field.field_id == 2 else field
            for field in MANIFEST_ENTRY_SCHEMA.fields
        ]
    )


class ManifestEntry(Record):
    __slots__ = ("status", "snapshot_id", "data_sequence_number", "file_sequence_number", "data_file")
    status: ManifestEntryStatus
    snapshot_id: Optional[int]
    data_sequence_number: Optional[int]
    file_sequence_number: Optional[int]
    data_file: DataFile

    def __init__(self, *data: Any, **named_data: Any) -> None:
        super().__init__(*data, **{"struct": MANIFEST_ENTRY_SCHEMA_STRUCT, **named_data})


PARTITION_FIELD_SUMMARY_TYPE = StructType(
    NestedField(509, "contains_null", BooleanType(), required=True),
    NestedField(518, "contains_nan", BooleanType(), required=False),
    NestedField(510, "lower_bound", BinaryType(), required=False),
    NestedField(511, "upper_bound", BinaryType(), required=False),
)


class PartitionFieldSummary(Record):
    __slots__ = ("contains_null", "contains_nan", "lower_bound", "upper_bound")
    contains_null: bool
    contains_nan: Optional[bool]
    lower_bound: Optional[bytes]
    upper_bound: Optional[bytes]

    def __init__(self, *data: Any, **named_data: Any) -> None:
        super().__init__(*data, **{"struct": PARTITION_FIELD_SUMMARY_TYPE, **named_data})


class PartitionFieldStats:
    _type: PrimitiveType
    _contains_null: bool
    _contains_nan: bool
    _min: Optional[Any]
    _max: Optional[Any]

    def __init__(self, iceberg_type: PrimitiveType) -> None:
        self._type = iceberg_type
        self._contains_null = False
        self._contains_nan = False
        self._min = None
        self._max = None

    def to_summary(self) -> PartitionFieldSummary:
        return PartitionFieldSummary(
            contains_null=self._contains_null,
            contains_nan=self._contains_nan,
            lower_bound=to_bytes(self._type, self._min) if self._min is not None else None,
            upper_bound=to_bytes(self._type, self._max) if self._max is not None else None,
        )

    def update(self, value: Any) -> None:
        if value is None:
            self._contains_null = True
        elif isinstance(value, float) and math.isnan(value):
            self._contains_nan = True
        else:
            if self._min is None:
                self._min = value
                self._max = value
            else:
                self._max = max(self._max, value)
                self._min = min(self._min, value)


def construct_partition_summaries(spec: PartitionSpec, schema: Schema, partitions: List[Record]) -> List[PartitionFieldSummary]:
    types = [field.field_type for field in spec.partition_type(schema).fields]
    field_stats = [PartitionFieldStats(field_type) for field_type in types]
    for partition_keys in partitions:
        for i, field_type in enumerate(types):
            if not isinstance(field_type, PrimitiveType):
                raise ValueError(f"Expected a primitive type for the partition field, got {field_type}")
            partition_key = partition_keys[i]
            field_stats[i].update(partition_key)
    return [field.to_summary() for field in field_stats]


MANIFEST_FILE_SCHEMA: Schema = Schema(
    NestedField(500, "manifest_path", StringType(), required=True, doc="Location URI with FS scheme"),
    NestedField(501, "manifest_length", LongType(), required=True),
    NestedField(502, "partition_spec_id", IntegerType(), required=True),
    NestedField(517, "content", IntegerType(), required=False, initial_default=ManifestContent.DATA),
    NestedField(515, "sequence_number", LongType(), required=False, initial_default=0),
    NestedField(516, "min_sequence_number", LongType(), required=False, initial_default=0),
    NestedField(503, "added_snapshot_id", LongType(), required=False),
    NestedField(504, "added_files_count", IntegerType(), required=False),
    NestedField(505, "existing_files_count", IntegerType(), required=False),
    NestedField(506, "deleted_files_count", IntegerType(), required=False),
    NestedField(512, "added_rows_count", LongType(), required=False),
    NestedField(513, "existing_rows_count", LongType(), required=False),
    NestedField(514, "deleted_rows_count", LongType(), required=False),
    NestedField(507, "partitions", ListType(508, PARTITION_FIELD_SUMMARY_TYPE, element_required=True), required=False),
    NestedField(519, "key_metadata", BinaryType(), required=False),
)

MANIFEST_FILE_SCHEMA_STRUCT = MANIFEST_FILE_SCHEMA.as_struct()

POSITIONAL_DELETE_SCHEMA = Schema(
    NestedField(2147483546, "file_path", StringType()), NestedField(2147483545, "pos", IntegerType())
)


class ManifestFile(Record):
    __slots__ = (
        "manifest_path",
        "manifest_length",
        "partition_spec_id",
        "content",
        "sequence_number",
        "min_sequence_number",
        "added_snapshot_id",
        "added_files_count",
        "existing_files_count",
        "deleted_files_count",
        "added_rows_count",
        "existing_rows_count",
        "deleted_rows_count",
        "partitions",
        "key_metadata",
    )
    manifest_path: str
    manifest_length: int
    partition_spec_id: int
    content: ManifestContent
    sequence_number: int
    min_sequence_number: int
    added_snapshot_id: int
    added_files_count: Optional[int]
    existing_files_count: Optional[int]
    deleted_files_count: Optional[int]
    added_rows_count: Optional[int]
    existing_rows_count: Optional[int]
    deleted_rows_count: Optional[int]
    partitions: Optional[List[PartitionFieldSummary]]
    key_metadata: Optional[bytes]

    def __init__(self, *data: Any, **named_data: Any) -> None:
        super().__init__(*data, **{"struct": MANIFEST_FILE_SCHEMA_STRUCT, **named_data})

    def has_added_files(self) -> bool:
        return self.added_files_count is None or self.added_files_count > 0

    def has_existing_files(self) -> bool:
        return self.existing_files_count is None or self.existing_files_count > 0

    def fetch_manifest_entry(self, io: FileIO, discard_deleted: bool = True) -> List[ManifestEntry]:
        """
        Read the manifest entries from the manifest file.

        Args:
            io: The FileIO to fetch the file.
            discard_deleted: Filter on live entries.

        Returns:
            An Iterator of manifest entries.
        """
        input_file = io.new_input(self.manifest_path)
        with AvroFile[ManifestEntry](
            input_file,
            MANIFEST_ENTRY_SCHEMA,
            read_types={-1: ManifestEntry, 2: DataFile},
            read_enums={0: ManifestEntryStatus, 101: FileFormat, 134: DataFileContent},
        ) as reader:
            return [
                _inherit_sequence_number(entry, self)
                for entry in reader
                if not discard_deleted or entry.status != ManifestEntryStatus.DELETED
            ]


def read_manifest_list(input_file: InputFile) -> Iterator[ManifestFile]:
    """
    Read the manifests from the manifest list.

    Args:
        input_file: The input file where the stream can be read from.

    Returns:
        An iterator of ManifestFiles that are part of the list.
    """
    with AvroFile[ManifestFile](
        input_file,
        MANIFEST_FILE_SCHEMA,
        read_types={-1: ManifestFile, 508: PartitionFieldSummary},
        read_enums={517: ManifestContent},
    ) as reader:
        yield from reader


def _inherit_sequence_number(entry: ManifestEntry, manifest: ManifestFile) -> ManifestEntry:
    """Inherits the sequence numbers.

    More information in the spec: https://iceberg.apache.org/spec/#sequence-number-inheritance

    Args:
        entry: The manifest entry that has null sequence numbers.
        manifest: The manifest that has a sequence number.

    Returns:
        The manifest entry with the sequence numbers set.
    """
    # The snapshot_id is required in V1, inherit with V2 when null
    if entry.snapshot_id is None:
        entry.snapshot_id = manifest.added_snapshot_id

    # in v1 tables, the data sequence number is not persisted and can be safely defaulted to 0
    # in v2 tables, the data sequence number should be inherited iff the entry status is ADDED
    if entry.data_sequence_number is None and (manifest.sequence_number == 0 or entry.status == ManifestEntryStatus.ADDED):
        entry.data_sequence_number = manifest.sequence_number

    # in v1 tables, the file sequence number is not persisted and can be safely defaulted to 0
    # in v2 tables, the file sequence number should be inherited iff the entry status is ADDED
    if entry.file_sequence_number is None and (manifest.sequence_number == 0 or entry.status == ManifestEntryStatus.ADDED):
        # Only available in V2, always 0 in V1
        entry.file_sequence_number = manifest.sequence_number

    return entry


class ManifestWriter(ABC):
    closed: bool
    _spec: PartitionSpec
    _schema: Schema
    _output_file: OutputFile
    _writer: AvroOutputFile[ManifestEntry]
    _snapshot_id: int
    _meta: Dict[str, str]
    _added_files: int
    _added_rows: int
    _existing_files: int
    _existing_rows: int
    _deleted_files: int
    _deleted_rows: int
    _min_data_sequence_number: Optional[int]
    _partitions: List[Record]

    def __init__(self, spec: PartitionSpec, schema: Schema, output_file: OutputFile, snapshot_id: int, meta: Dict[str, str]):
        self.closed = False
        self._spec = spec
        self._schema = schema
        self._output_file = output_file
        self._snapshot_id = snapshot_id
        self._meta = meta

        self._added_files = 0
        self._added_rows = 0
        self._existing_files = 0
        self._existing_rows = 0
        self._deleted_files = 0
        self._deleted_rows = 0
        self._min_data_sequence_number = None
        self._partitions = []

    def __enter__(self) -> ManifestWriter:
        """Open the writer."""
        self._writer = self.new_writer()
        self._writer.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Close the writer."""
        self.closed = True
        self._writer.__exit__(exc_type, exc_value, traceback)

    @abstractmethod
    def content(self) -> ManifestContent:
        ...

    @abstractmethod
    def new_writer(self) -> AvroOutputFile[ManifestEntry]:
        ...

    @abstractmethod
    def prepare_entry(self, entry: ManifestEntry) -> ManifestEntry:
        ...

    def to_manifest_file(self) -> ManifestFile:
        """Return the manifest file."""
        # once the manifest file is generated, no more entries can be added
        self.closed = True
        min_sequence_number = self._min_data_sequence_number or UNASSIGNED_SEQ
        return ManifestFile(
            manifest_path=self._output_file.location,
            manifest_length=len(self._writer.output_file),
            partition_spec_id=self._spec.spec_id,
            content=self.content(),
            sequence_number=UNASSIGNED_SEQ,
            min_sequence_number=min_sequence_number,
            added_snapshot_id=self._snapshot_id,
            added_files_count=self._added_files,
            existing_files_count=self._existing_files,
            deleted_files_count=self._deleted_files,
            added_rows_count=self._added_rows,
            existing_rows_count=self._existing_rows,
            deleted_rows_count=self._deleted_rows,
            partitions=construct_partition_summaries(self._spec, self._schema, self._partitions),
            key_metadatas=None,
        )

    def add_entry(self, entry: ManifestEntry) -> ManifestWriter:
        if self.closed:
            raise RuntimeError("Cannot add entry to closed manifest writer")
        if entry.status == ManifestEntryStatus.ADDED:
            self._added_files += 1
            self._added_rows += entry.data_file.record_count
        elif entry.status == ManifestEntryStatus.EXISTING:
            self._existing_files += 1
            self._existing_rows += entry.data_file.record_count
        elif entry.status == ManifestEntryStatus.DELETED:
            self._deleted_files += 1
            self._deleted_rows += entry.data_file.record_count

        self._partitions.append(entry.data_file.partition)

        if (
            (entry.status == ManifestEntryStatus.ADDED or entry.status == ManifestEntryStatus.EXISTING)
            and entry.data_sequence_number is not None
            and (self._min_data_sequence_number is None or entry.data_sequence_number < self._min_data_sequence_number)
        ):
            self._min_data_sequence_number = entry.data_sequence_number

        self._writer.write_block([self.prepare_entry(entry)])
        return self


class ManifestWriterV1(ManifestWriter):
    def __init__(self, spec: PartitionSpec, schema: Schema, output_file: OutputFile, snapshot_id: int):
        super().__init__(
            spec,
            schema,
            output_file,
            snapshot_id,
            {
                "schema": schema.json(),
                "partition-spec": spec.json(),
                "partition-spec-id": str(spec.spec_id),
                "format-version": "1",
            },
        )

    def content(self) -> ManifestContent:
        return ManifestContent.DATA

    def new_writer(self) -> AvroOutputFile[ManifestEntry]:
        v1_data_file_type = data_file_with_partition(self._spec.partition_type(self._schema), format_version=1)
        v1_manifest_entry_schema = manifest_entry_schema_with_data_file(v1_data_file_type)
        return AvroOutputFile[ManifestEntry](self._output_file, v1_manifest_entry_schema, "manifest_entry", self._meta)

    def prepare_entry(self, entry: ManifestEntry) -> ManifestEntry:
        wrapped_entry = ManifestEntry(*entry.record_fields())
        wrapped_entry.data_file.block_size_in_bytes = DEFAULT_BLOCK_SIZE
        return wrapped_entry


class ManifestWriterV2(ManifestWriter):
    def __init__(self, spec: PartitionSpec, schema: Schema, output_file: OutputFile, snapshot_id: int):
        super().__init__(
            spec,
            schema,
            output_file,
            snapshot_id,
            {
                "schema": schema.json(),
                "partition-spec": spec.json(),
                "partition-spec-id": str(spec.spec_id),
                "format-version": "2",
                "content": "data",
            },
        )

    def content(self) -> ManifestContent:
        return ManifestContent.DATA

    def new_writer(self) -> AvroOutputFile[ManifestEntry]:
        v2_data_file_type = data_file_with_partition(self._spec.partition_type(self._schema), format_version=2)
        v2_manifest_entry_schema = manifest_entry_schema_with_data_file(v2_data_file_type)
        return AvroOutputFile[ManifestEntry](self._output_file, v2_manifest_entry_schema, "manifest_entry", self._meta)

    def prepare_entry(self, entry: ManifestEntry) -> ManifestEntry:
        if entry.data_sequence_number is None:
            if entry.snapshot_id is not None and entry.snapshot_id != self._snapshot_id:
                raise ValueError(f"Found unassigned sequence number for an entry from snapshot: {entry.snapshot_id}")
            if entry.status != ManifestEntryStatus.ADDED:
                raise ValueError("Only entries with status ADDED can have null sequence number")
        # In v2, we should not write block_size_in_bytes field
        wrapped_data_file_v2_debug = DataFile(
            format_version=2,
            content=entry.data_file.content,
            file_path=entry.data_file.file_path,
            file_format=entry.data_file.file_format,
            partition=entry.data_file.partition,
            record_count=entry.data_file.record_count,
            file_size_in_bytes=entry.data_file.file_size_in_bytes,
            column_sizes=entry.data_file.column_sizes,
            value_counts=entry.data_file.value_counts,
            null_value_counts=entry.data_file.null_value_counts,
            nan_value_counts=entry.data_file.nan_value_counts,
            lower_bounds=entry.data_file.lower_bounds,
            upper_bounds=entry.data_file.upper_bounds,
            key_metadata=entry.data_file.key_metadata,
            split_offsets=entry.data_file.split_offsets,
            equality_ids=entry.data_file.equality_ids,
            sort_order_id=entry.data_file.sort_order_id,
            spec_id=entry.data_file.spec_id,
        )
        wrapped_entry = ManifestEntry(
            status=entry.status,
            snapshot_id=entry.snapshot_id,
            data_sequence_number=entry.data_sequence_number,
            file_sequence_number=entry.file_sequence_number,
            data_file=wrapped_data_file_v2_debug,
        )
        return wrapped_entry


def write_manifest(
    format_version: Literal[1, 2], spec: PartitionSpec, schema: Schema, output_file: OutputFile, snapshot_id: int
) -> ManifestWriter:
    if format_version == 1:
        return ManifestWriterV1(spec, schema, output_file, snapshot_id)
    elif format_version == 2:
        return ManifestWriterV2(spec, schema, output_file, snapshot_id)
    else:
        raise ValueError(f"Cannot write manifest for table version: {format_version}")


class ManifestListWriter(ABC):
    _output_file: OutputFile
    _meta: Dict[str, str]
    _manifest_files: List[ManifestFile]
    _commit_snapshot_id: int
    _writer: AvroOutputFile[ManifestFile]

    def __init__(self, output_file: OutputFile, meta: Dict[str, str]):
        self._output_file = output_file
        self._meta = meta
        self._manifest_files = []

    def __enter__(self) -> ManifestListWriter:
        """Open the writer for writing."""
        self._writer = AvroOutputFile[ManifestFile](self._output_file, MANIFEST_FILE_SCHEMA, "manifest_file", self._meta)
        self._writer.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Close the writer."""
        self._writer.__exit__(exc_type, exc_value, traceback)
        return

    @abstractmethod
    def prepare_manifest(self, manifest_file: ManifestFile) -> ManifestFile:
        ...

    def add_manifests(self, manifest_files: List[ManifestFile]) -> ManifestListWriter:
        self._writer.write_block([self.prepare_manifest(manifest_file) for manifest_file in manifest_files])
        return self


class ManifestListWriterV1(ManifestListWriter):
    def __init__(self, output_file: OutputFile, snapshot_id: int, parent_snapshot_id: int):
        super().__init__(
            output_file, {"snapshot-id": str(snapshot_id), "parent-snapshot-id": str(parent_snapshot_id), "format-version": "1"}
        )

    def prepare_manifest(self, manifest_file: ManifestFile) -> ManifestFile:
        if manifest_file.content != ManifestContent.DATA:
            raise ValidationError("Cannot store delete manifests in a v1 table")
        return manifest_file


class ManifestListWriterV2(ManifestListWriter):
    _commit_snapshot_id: int
    _sequence_number: int

    def __init__(self, output_file: OutputFile, snapshot_id: int, parent_snapshot_id: int, sequence_number: int):
        super().__init__(
            output_file,
            {
                "snapshot-id": str(snapshot_id),
                "parent-snapshot-id": str(parent_snapshot_id),
                "sequence-number": str(sequence_number),
                "format-version": "2",
            },
        )
        self._commit_snapshot_id = snapshot_id
        self._sequence_number = sequence_number

    def prepare_manifest(self, manifest_file: ManifestFile) -> ManifestFile:
        wrapped_manifest_file = ManifestFile(*manifest_file.record_fields())

        if wrapped_manifest_file.sequence_number == UNASSIGNED_SEQ:
            # if the sequence number is being assigned here, then the manifest must be created by the current operation.
            # To validate this, check that the snapshot id matches the current commit
            if self._commit_snapshot_id != wrapped_manifest_file.added_snapshot_id:
                raise ValueError(
                    f"Found unassigned sequence number for a manifest from snapshot: {wrapped_manifest_file.added_snapshot_id}"
                )
            wrapped_manifest_file.sequence_number = self._sequence_number

        if wrapped_manifest_file.min_sequence_number == UNASSIGNED_SEQ:
            if self._commit_snapshot_id != wrapped_manifest_file.added_snapshot_id:
                raise ValueError(
                    f"Found unassigned sequence number for a manifest from snapshot: {wrapped_manifest_file.added_snapshot_id}"
                )
            # if the min sequence number is not determined, then there was no assigned sequence number for any file
            # written to the wrapped manifest. Replace the unassigned sequence number with the one for this commit
            wrapped_manifest_file.min_sequence_number = self._sequence_number
        return wrapped_manifest_file


def write_manifest_list(
    format_version: Literal[1, 2], output_file: OutputFile, snapshot_id: int, parent_snapshot_id: int, sequence_number: int
) -> ManifestListWriter:
    if format_version == 1:
        return ManifestListWriterV1(output_file, snapshot_id, parent_snapshot_id)
    elif format_version == 2:
        return ManifestListWriterV2(output_file, snapshot_id, parent_snapshot_id, sequence_number)
    else:
        raise ValueError(f"Cannot write manifest list for table version: {format_version}")

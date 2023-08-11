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
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
)

from pyiceberg.avro.file import AvroFile
from pyiceberg.io import FileIO, InputFile
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)


class DataFileContent(int, Enum):
    DATA = 0
    POSITION_DELETES = 1
    EQUALITY_DELETES = 2

    def __repr__(self) -> str:
        """Returns the string representation of the DataFileContent class."""
        return f"DataFileContent.{self.name}"


class ManifestContent(int, Enum):
    DATA = 0
    DELETES = 1

    def __repr__(self) -> str:
        """Returns the string representation of the ManifestContent class."""
        return f"ManifestContent.{self.name}"


class ManifestEntryStatus(int, Enum):
    EXISTING = 0
    ADDED = 1
    DELETED = 2

    def __repr__(self) -> str:
        """Returns the string representation of the ManifestEntryStatus class."""
        return f"ManifestEntryStatus.{self.name}"


class FileFormat(str, Enum):
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"

    def __repr__(self) -> str:
        """Returns the string representation of the FileFormat class."""
        return f"FileFormat.{self.name}"


DATA_FILE_TYPE = StructType(
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
        field_id=101, name="file_format", field_type=StringType(), required=True, doc="File format name: avro, orc, or parquet"
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


class DataFile(Record):
    __slots__ = (
        "content",
        "file_path",
        "file_format",
        "partition",
        "record_count",
        "file_size_in_bytes",
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
        """Used for assigning a key/value to a DataFile."""
        # The file_format is written as a string, so we need to cast it to the Enum
        if name == "file_format":
            value = FileFormat[value]
        super().__setattr__(name, value)

    def __init__(self, *data: Any, **named_data: Any) -> None:
        super().__init__(*data, **{"struct": DATA_FILE_TYPE, **named_data})

    def __hash__(self) -> int:
        """Returns the hash of the file path."""
        return hash(self.file_path)

    def __eq__(self, other: Any) -> bool:
        """Compares the datafile with another object.

        If it is a datafile, it will compare based on the file_path.
        """
        return self.file_path == other.file_path if isinstance(other, DataFile) else False


MANIFEST_ENTRY_SCHEMA = Schema(
    NestedField(field_id=0, name="status", field_type=IntegerType(), required=True),
    NestedField(field_id=1, name="snapshot_id", field_type=LongType(), required=False),
    NestedField(field_id=3, name="data_sequence_number", field_type=LongType(), required=False),
    NestedField(field_id=4, name="file_sequence_number", field_type=LongType(), required=False),
    NestedField(field_id=2, name="data_file", field_type=DATA_FILE_TYPE, required=True),
)

MANIFEST_ENTRY_SCHEMA_STRUCT = MANIFEST_ENTRY_SCHEMA.as_struct()


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
    NestedField(field_id=509, name="contains_null", field_type=BooleanType(), required=True),
    NestedField(field_id=518, name="contains_nan", field_type=BooleanType(), required=False),
    NestedField(field_id=510, name="lower_bound", field_type=BinaryType(), required=False),
    NestedField(field_id=511, name="upper_bound", field_type=BinaryType(), required=False),
)


class PartitionFieldSummary(Record):
    __slots__ = ("contains_null", "contains_nan", "lower_bound", "upper_bound")
    contains_null: bool
    contains_nan: Optional[bool]
    lower_bound: Optional[bytes]
    upper_bound: Optional[bytes]

    def __init__(self, *data: Any, **named_data: Any) -> None:
        super().__init__(*data, **{"struct": PARTITION_FIELD_SUMMARY_TYPE, **named_data})


MANIFEST_FILE_SCHEMA: Schema = Schema(
    NestedField(field_id=500, name="manifest_path", field_type=StringType(), required=True, doc="Location URI with FS scheme"),
    NestedField(field_id=501, name="manifest_length", field_type=LongType(), required=True),
    NestedField(field_id=502, name="partition_spec_id", field_type=IntegerType(), required=True),
    NestedField(field_id=517, name="content", field_type=IntegerType(), required=False, initial_default=ManifestContent.DATA),
    NestedField(field_id=515, name="sequence_number", field_type=LongType(), required=False, initial_default=0),
    NestedField(field_id=516, name="min_sequence_number", field_type=LongType(), required=False, initial_default=0),
    NestedField(field_id=503, name="added_snapshot_id", field_type=LongType(), required=False),
    NestedField(field_id=504, name="added_files_count", field_type=IntegerType(), required=False),
    NestedField(field_id=505, name="existing_files_count", field_type=IntegerType(), required=False),
    NestedField(field_id=506, name="deleted_files_count", field_type=IntegerType(), required=False),
    NestedField(field_id=512, name="added_rows_count", field_type=LongType(), required=False),
    NestedField(field_id=513, name="existing_rows_count", field_type=LongType(), required=False),
    NestedField(field_id=514, name="deleted_rows_count", field_type=LongType(), required=False),
    NestedField(
        field_id=507,
        name="partitions",
        field_type=ListType(508, PARTITION_FIELD_SUMMARY_TYPE, element_required=True),
        required=False,
    ),
    NestedField(field_id=519, name="key_metadata", field_type=BinaryType(), required=False),
)

MANIFEST_FILE_SCHEMA_STRUCT = MANIFEST_FILE_SCHEMA.as_struct()

POSITIONAL_DELETE_SCHEMA = Schema(
    NestedField(field_id=2147483546, name="file_path", field_type=StringType()),
    NestedField(field_id=2147483545, name="pos", field_type=IntegerType()),
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
        Reads the manifest entries from the manifest file.

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
    Reads the manifests from the manifest list.

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

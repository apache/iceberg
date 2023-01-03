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
    Callable,
)

from pydantic import Field, PrivateAttr

from pyiceberg.avro.file import AvroFile
from pyiceberg.io import FileIO, InputFile
from pyiceberg.schema import Schema
from pyiceberg.types import (
    ListType,
    StructType,
    NestedField,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    BinaryType,
)
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class DataFileContent(int, Enum):
    DATA = 0
    POSITION_DELETES = 1
    EQUALITY_DELETES = 2

    def __repr__(self) -> str:
        return f"DataFileContent.{self.name}"


class ManifestContent(int, Enum):
    DATA = 0
    DELETES = 1

    def __repr__(self) -> str:
        return f"ManifestContent.{self.name}"


class ManifestEntryStatus(int, Enum):
    EXISTING = 0
    ADDED = 1
    DELETED = 2

    def __repr__(self) -> str:
        return f"ManifestEntryStatus.{self.name}"


class FileFormat(str, Enum):
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"

    def __repr__(self) -> str:
        return f"FileFormat.{self.name}"


class DataFile(IcebergBaseModel):
    content: DataFileContent = Field(default=DataFileContent.DATA)
    file_path: str = Field()
    file_format: FileFormat = Field()
    partition: Dict[str, Any] = Field()
    record_count: int = Field()
    file_size_in_bytes: int = Field()
    block_size_in_bytes: Optional[int] = Field()
    column_sizes: Optional[Dict[int, int]] = Field()
    value_counts: Optional[Dict[int, int]] = Field()
    null_value_counts: Optional[Dict[int, int]] = Field()
    nan_value_counts: Optional[Dict[int, int]] = Field()
    distinct_counts: Optional[Dict[int, int]] = Field()
    lower_bounds: Optional[Dict[int, bytes]] = Field()
    upper_bounds: Optional[Dict[int, bytes]] = Field()
    key_metadata: Optional[bytes] = Field()
    split_offsets: Optional[List[int]] = Field()
    equality_ids: Optional[List[int]] = Field()
    sort_order_id: Optional[int] = Field()


class ManifestEntry(IcebergBaseModel):
    status: ManifestEntryStatus = Field()
    snapshot_id: Optional[int] = Field()
    sequence_number: Optional[int] = Field()
    data_file: DataFile = Field()


PARTITION_FIELD_SUMMARY_TYPE = StructType(
    NestedField(509, "contains_null", BooleanType(), required=True),
    NestedField(518, "contains_nan", BooleanType(), required=False),
    NestedField(510, "lower_bound", BinaryType(), required=False),
    NestedField(511, "upper_bound", BinaryType(), required=False),
)


class PartitionFieldSummary(IcebergBaseModel):
    contains_null: bool = Field()
    contains_nan: Optional[bool] = Field()
    lower_bound: Optional[bytes] = Field()
    upper_bound: Optional[bytes] = Field()


MANIFEST_FILE_SCHEMA: Schema = Schema(
    NestedField(500, "manifest_path", StringType(), required=True, doc="Location URI with FS scheme"),
    NestedField(501, "manifest_length", LongType(), required=True),
    NestedField(502, "partition_spec_id", IntegerType(), required=True),
    NestedField(517, "content", IntegerType(), required=False),
    NestedField(515, "sequence_number", LongType(), required=False),
    NestedField(516, "min_sequence_number", LongType(), required=False),
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

MANIFEST_FILE_FIELD_NAMES: Dict[int, str] = {pos: field.name for pos, field in enumerate(MANIFEST_FILE_SCHEMA.fields)}
IN_TRANSFORMS: Dict[int, Callable[[Any], Any]] = {3: lambda id: ManifestContent.DATA if id == 0 else ManifestContent.DELETES}
OUT_TRANSFORMS: Dict[int, Callable[[Any], Any]] = {3: lambda content: content.id}

class ManifestList(IcebergBaseModel):
    pass


class ManifestFile(IcebergBaseModel):
    manifest_path: str = Field()
    manifest_length: int = Field()
    partition_spec_id: int = Field()
    content: ManifestContent = Field()
    sequence_number: int = Field(default=0)
    min_sequence_number: int = Field(default=0)
    added_snapshot_id: Optional[int] = Field()
    added_files_count: Optional[int] = Field()
    existing_files_count: Optional[int] = Field()
    deleted_files_count: Optional[int] = Field()
    added_rows_count: Optional[int] = Field()
    existing_rows_count: Optional[int] = Field()
    deleted_rows_count: Optional[int] = Field()
    partitions: List[PartitionFieldSummary] = Field(default_factory=[])
    key_metadata: Optional[bytes] = Field()

    _projection_positions: Dict[int, int] = PrivateAttr()

    def __init__(self, read_schema: Optional[Schema] = None, **data: Any):
        if read_schema:
            id_to_pos = {field.field_id: pos for pos, field in enumerate(MANIFEST_FILE_SCHEMA.fields)}
            self._projection_positions = {pos: id_to_pos[field.field_id] for pos, field in enumerate(read_schema.fields)}
        else:
            # all fields are projected
            self._projection_positions = {pos: pos for pos in range(len(MANIFEST_FILE_SCHEMA.fields))}
        super().__init__(**data)

    def __getitem__(self, read_pos: int) -> Any:
        pos = self._projection_positions[read_pos]
        value = getattr(self, MANIFEST_FILE_FIELD_NAMES[pos])
        if transform := OUT_TRANSFORMS.get(pos):
            return transform[1](value)
        else:
            return value

    def __setitem__(self, read_pos: int, value: Any) -> None:
        pos = self._projection_positions[read_pos]
        if transform := IN_TRANSFORMS.get(pos):
            self._set_by_position(pos, transform[0](value))
        else:
            self._set_by_position(pos, value)

    def _set_by_position(self, pos: int, value: Any):
        setattr(self, MANIFEST_FILE_FIELD_NAMES[pos], value)

    def fetch_manifest_entry(self, io: FileIO) -> List[ManifestEntry]:
        file = io.new_input(self.manifest_path)
        return list(read_manifest_entry(file))


def read_manifest_entry(input_file: InputFile) -> Iterator[ManifestEntry]:
    with AvroFile(input_file, None, {-1: ManifestFile, 508: PartitionFieldSummary}) as reader:
        return list(reader)


def live_entries(input_file: InputFile) -> Iterator[ManifestEntry]:
    return (entry for entry in read_manifest_entry(input_file) if entry.status != ManifestEntryStatus.DELETED)


def files(input_file: InputFile) -> Iterator[DataFile]:
    return (entry.data_file for entry in live_entries(input_file))


def read_manifest_list(input_file: InputFile) -> Iterator[ManifestFile]:
    with AvroFile(input_file, None, {-1: ManifestFile, 508: PartitionFieldSummary}) as reader:
        return list(reader)



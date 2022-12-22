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

from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
)

from pyiceberg.avro.file import AvroFile
from pyiceberg.io import FileIO, InputFile
from pyiceberg.typedef import Record


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


@dataclass
class DataFile(Record):
    @staticmethod
    def from_record(record: Record, format_version: int) -> Union[DataFileV1, DataFileV2]:
        if format_version == 1:
            return DataFileV1().wrap(record)  # type: ignore
        elif format_version == 2:
            return DataFileV2().wrap(record)  # type: ignore
        else:
            raise ValueError(f"Unknown format-version: {format_version}")

    file_path: str
    file_format: FileFormat
    partition: Record
    record_count: int
    file_size_in_bytes: int
    block_size_in_bytes: Optional[int] = field(default=None)
    column_sizes: Optional[Dict[int, int]] = field(default=None)
    value_counts: Optional[Dict[int, int]] = field(default=None)
    null_value_counts: Optional[Dict[int, int]] = field(default=None)
    nan_value_counts: Optional[Dict[int, int]] = field(default=None)
    distinct_counts: Optional[Dict[int, int]] = field(default=None)  # Does not seem to be used on the Java side!?
    lower_bounds: Optional[Dict[int, bytes]] = field(default=None)
    upper_bounds: Optional[Dict[int, bytes]] = field(default=None)
    key_metadata: Optional[bytes] = field(default=None)
    split_offsets: Optional[List[int]] = field(default=None)
    equality_ids: Optional[List[int]] = field(default=None)
    sort_order_id: Optional[int] = field(default=None)
    content: DataFileContent = field(default=DataFileContent.DATA)


class DataFileV1(DataFile):
    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.file_path = value
        elif pos == 1:
            self.file_format = value
        elif pos == 2:
            self.partition = value
        elif pos == 3:
            self.record_count = value
        elif pos == 4:
            self.file_size_in_bytes = value
        elif pos == 5:
            self.block_size_in_bytes = value
        elif pos == 6:
            self.column_sizes = value
        elif pos == 7:
            self.value_counts = value
        elif pos == 8:
            self.null_value_counts = value
        elif pos == 9:
            self.nan_value_counts = value
        elif pos == 10:
            self.lower_bounds = value
        elif pos == 11:
            self.upper_bounds = value
        elif pos == 12:
            self.key_metadata = value
        elif pos == 13:
            self.split_offsets = value
        elif pos == 14:
            self.sort_order_id = value


class DataFileV2(DataFile):
    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.content = value
        elif pos == 1:
            self.file_path = value
        elif pos == 2:
            self.file_format = value
        elif pos == 3:
            self.partition = value
        elif pos == 4:
            self.record_count = value
        elif pos == 5:
            self.file_size_in_bytes = value
        elif pos == 6:
            self.column_sizes = value
        elif pos == 7:
            self.value_counts = value
        elif pos == 8:
            self.null_value_counts = value
        elif pos == 9:
            self.nan_value_counts = value
        elif pos == 10:
            self.lower_bounds = value
        elif pos == 11:
            self.upper_bounds = value
        elif pos == 12:
            self.key_metadata = value
        elif pos == 13:
            self.split_offsets = value
        elif pos == 14:
            self.equality_ids = value
        elif pos == 15:
            self.sort_order_id = value


@dataclass
class ManifestEntry(Record):
    status: ManifestEntryStatus
    data_file: DataFile
    snapshot_id: Optional[int] = None
    sequence_number: Optional[int] = None
    file_sequence_number: Optional[int] = None

    @staticmethod
    def from_record(record: Record, format_version: int) -> Union[ManifestEntryV1, ManifestEntryV2]:
        if format_version == 1:
            return ManifestEntryV1().wrap(record)  # type: ignore
        elif format_version == 2:
            return ManifestEntryV2().wrap(record)  # type: ignore
        else:
            raise ValueError(f"Unknown format-version: {format_version}")


class ManifestEntryV1(ManifestEntry):
    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.status = ManifestEntryStatus(value)
        elif pos == 1:
            self.snapshot_id = value
        elif pos == 2:
            self.data_file = DataFile.from_record(value, format_version=1)


class ManifestEntryV2(ManifestEntry):
    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.status = ManifestEntryStatus(value)
        elif pos == 1:
            self.snapshot_id = value
        elif pos == 2:
            self.sequence_number = value
        elif pos == 3:
            self.file_sequence_number = value
        elif pos == 4:
            self.data_file = DataFile.from_record(value, format_version=2)


@dataclass
class PartitionFieldSummary(Record):
    contains_null: bool
    contains_nan: Optional[bool]
    lower_bound: Optional[bytes]
    upper_bound: Optional[bytes]

    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.contains_null = value
        elif pos == 1:
            self.contains_nan = value
        elif pos == 2:
            self.lower_bound = value
        elif pos == 3:
            self.upper_bound = value
        # ignore the object, it must be from a newer version of the format


@dataclass
class ManifestFile(Record):
    @staticmethod
    def from_record(record: Record, format_version: int) -> ManifestFile:
        if format_version == 1:
            return ManifestFileV1().wrap(record)  # type: ignore
        elif format_version == 2:
            return ManifestFileV2().wrap(record)  # type: ignore
        else:
            raise ValueError(f"Unknown format-version: {format_version}")

    manifest_path: str
    manifest_length: int
    partition_spec_id: int
    added_snapshot_id: int
    content: ManifestContent = field(default=ManifestContent.DATA)  # v2
    sequence_number: Optional[int] = field(default=None)  # v2
    min_sequence_number: Optional[int] = field(default=None)  # v2
    added_files_count: Optional[int] = field(default=None)
    existing_files_count: Optional[int] = field(default=None)
    deleted_files_count: Optional[int] = field(default=None)
    added_rows_count: Optional[int] = field(default=None)
    existing_rows_count: Optional[int] = field(default=None)
    deleted_rows_count: Optional[int] = field(default=None)
    partitions: List[PartitionFieldSummary] = field(default_factory=list)
    key_metadata: Optional[bytes] = field(default=None)

    def fetch_manifest_entry(self, io: FileIO, format_version: int) -> List[ManifestEntry]:
        file = io.new_input(self.manifest_path)
        return read_manifest_entry(file, format_version)


class ManifestFileV1(ManifestFile):
    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.manifest_path = value
        elif pos == 1:
            self.manifest_length = value
        elif pos == 2:
            self.partition_spec_id = value
        elif pos == 3:
            self.snapshot_id = value
        elif pos == 4:
            self.added_files_count = value
        elif pos == 5:
            self.existing_files_count = value
        elif pos == 6:
            self.deleted_files_count = value
        elif pos == 7:
            self.partitions = [
                element if isinstance(element, PartitionFieldSummary) else PartitionFieldSummary().wrap(element)  # type: ignore
                for element in value
            ]
        elif pos == 8:
            self.added_rows_count = value
        elif pos == 9:
            self.existing_rows_count = value
        elif pos == 10:
            self.deleted_rows_count = value
        # ignore the object, it must be from a newer version of the format


class ManifestFileV2(ManifestFile):
    def __setitem__(self, pos: int, value: Any) -> None:
        if pos == 0:
            self.manifest_path = value
        elif pos == 1:
            self.manifest_length = value
        elif pos == 2:
            self.partition_spec_id = value
        elif pos == 3:
            self.content = value
        elif pos == 4:
            self.sequence_number = value
        elif pos == 5:
            self.min_sequence_number = value
        elif pos == 6:
            self.snapshot_id = value
        elif pos == 7:
            self.added_files_count = value
        elif pos == 8:
            self.existing_files_count = value
        elif pos == 9:
            self.deleted_files_count = value
        elif pos == 10:
            self.added_rows_count = value
        elif pos == 11:
            self.existing_rows_count = value
        elif pos == 12:
            self.deleted_rows_count = value
        elif pos == 13:
            self.partitions = value
        elif pos == 14:
            self.key_metadata = value
        # ignore the object, it must be from a newer version of the format


def read_manifest_entry(input_file: InputFile, format_version: int) -> List[ManifestEntry]:
    with AvroFile(input_file) as reader:
        return [ManifestEntry.from_record(record, format_version) for record in reader]


def live_entries(input_file: InputFile, format_version: int) -> Iterator[ManifestEntry]:
    return (entry for entry in read_manifest_entry(input_file, format_version) if entry.status != ManifestEntryStatus.DELETED)


def files(input_file: InputFile, format_version: int) -> Iterator[DataFile]:
    return (entry.data_file for entry in live_entries(input_file, format_version))


def read_manifest_list(input_file: InputFile, format_version: int) -> List[ManifestFile]:
    with AvroFile(input_file) as reader:
        return [ManifestFile.from_record(record, format_version) for record in reader]


def read_manifest_list_record(input_file: InputFile, format_version: int) -> List[ManifestFile]:
    with AvroFile(input_file) as reader:
        return [ManifestFile.from_record(record, format_version) for record in reader]

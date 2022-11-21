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
from functools import singledispatch
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
)

from pydantic import Field

from pyiceberg.avro.file import AvroFile
from pyiceberg.avro.reader import AvroStruct
from pyiceberg.io import FileIO, InputFile
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    PrimitiveType,
    StructType,
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


class PartitionFieldSummary(IcebergBaseModel):
    contains_null: bool = Field()
    contains_nan: Optional[bool] = Field()
    lower_bound: Optional[bytes] = Field()
    upper_bound: Optional[bytes] = Field()


class ManifestFile(IcebergBaseModel):
    manifest_path: str = Field()
    manifest_length: int = Field()
    partition_spec_id: int = Field()
    content: ManifestContent = Field(default=ManifestContent.DATA)
    sequence_number: int = Field(default=0)
    min_sequence_number: int = Field(default=0)
    added_snapshot_id: Optional[int] = Field()
    added_data_files_count: Optional[int] = Field()
    existing_data_files_count: Optional[int] = Field()
    deleted_data_files_count: Optional[int] = Field()
    added_rows_count: Optional[int] = Field()
    existing_rows_counts: Optional[int] = Field()
    deleted_rows_count: Optional[int] = Field()
    partitions: Optional[List[PartitionFieldSummary]] = Field()
    key_metadata: Optional[bytes] = Field()

    def fetch_manifest_entry(self, io: FileIO) -> List[ManifestEntry]:
        file = io.new_input(self.manifest_path)
        return list(read_manifest_entry(file))


def read_manifest_entry(input_file: InputFile) -> Iterator[ManifestEntry]:
    with AvroFile(input_file) as reader:
        schema = reader.schema
        for record in reader:
            dict_repr = _convert_pos_to_dict(schema, record)
            yield ManifestEntry(**dict_repr)


def live_entries(input_file: InputFile) -> Iterator[ManifestEntry]:
    return (entry for entry in read_manifest_entry(input_file) if entry.status != ManifestEntryStatus.DELETED)


def files(input_file: InputFile) -> Iterator[DataFile]:
    return (entry.data_file for entry in live_entries(input_file))


def read_manifest_list(input_file: InputFile) -> Iterator[ManifestFile]:
    with AvroFile(input_file) as reader:
        schema = reader.schema
        for record in reader:
            dict_repr = _convert_pos_to_dict(schema, record)
            yield ManifestFile(**dict_repr)


@singledispatch
def _convert_pos_to_dict(schema: Union[Schema, IcebergType], struct: AvroStruct) -> Dict[str, Any]:
    """Converts the positions in the field names

    This makes it easy to map it onto a Pydantic model. Might change later on depending on the performance

     Args:
         schema (Schema | IcebergType): The schema of the file
         struct (AvroStruct): The struct containing the data by positions

     Raises:
         NotImplementedError: If attempting to handle an unknown type in the schema
    """
    raise NotImplementedError(f"Cannot traverse non-type: {schema}")


@_convert_pos_to_dict.register
def _(schema: Schema, struct: AvroStruct) -> Dict[str, Any]:
    return _convert_pos_to_dict(schema.as_struct(), struct)


@_convert_pos_to_dict.register
def _(struct_type: StructType, values: AvroStruct) -> Dict[str, Any]:
    """Iterates over all the fields in the dict, and gets the data from the struct"""
    return (
        {field.name: _convert_pos_to_dict(field.field_type, values.get(pos)) for pos, field in enumerate(struct_type.fields)}
        if values is not None
        else None
    )


@_convert_pos_to_dict.register
def _(list_type: ListType, values: List[Any]) -> Any:
    """In the case of a list, we'll go over the elements in the list to handle complex types"""
    return [_convert_pos_to_dict(list_type.element_type, value) for value in values] if values is not None else None


@_convert_pos_to_dict.register
def _(map_type: MapType, values: Dict[Any, Any]) -> Dict[Any, Any]:
    """In the case of a map, we both traverse over the key and value to handle complex types"""
    return (
        {
            _convert_pos_to_dict(map_type.key_type, key): _convert_pos_to_dict(map_type.value_type, value)
            for key, value in values.items()
        }
        if values is not None
        else None
    )


@_convert_pos_to_dict.register
def _(_: PrimitiveType, value: Any) -> Any:
    return value

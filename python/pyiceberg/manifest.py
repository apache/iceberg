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

from pyiceberg.avro.file import AvroFile
from pyiceberg.avro.reader import AvroStruct
from pyiceberg.io.base import InputFile
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IcebergType,
    ListType,
    MapType,
    PrimitiveType,
    StructType,
)
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel


class FileFormat(str, Enum):
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    ORC = "ORC"


class DataFile(IcebergBaseModel):
    file_path: str
    file_format: FileFormat
    partition: Dict[str, Any]
    record_count: int
    file_size_in_bytes: int
    block_size_in_bytes: int
    column_sizes: Optional[Dict[int, int]]
    value_counts: Optional[Dict[int, int]]
    null_value_counts: Optional[Dict[int, int]]
    nan_value_counts: Optional[Dict[int, int]]
    lower_bounds: Optional[Dict[int, bytes]]
    upper_bounds: Optional[Dict[int, bytes]]
    key_metadata: Optional[bytes]
    split_offsets: Optional[List[int]]
    sort_order_id: Optional[int]


class ManifestEntry(IcebergBaseModel):
    status: int
    snapshot_id: Optional[int]
    data_file: DataFile


class Partition(IcebergBaseModel):
    contains_null: bool
    contains_nan: Optional[bool]
    lower_bound: Optional[bytes]
    upper_bound: Optional[bytes]


class ManifestFile(IcebergBaseModel):
    manifest_path: str
    manifest_length: int
    partition_spec_id: int
    added_snapshot_id: Optional[int]
    added_data_files_count: Optional[int]
    existing_data_files_count: Optional[int]
    deleted_data_files_count: Optional[int]
    partitions: Optional[List[Partition]]
    added_rows_count: Optional[int]
    existing_rows_counts: Optional[int]
    deleted_rows_count: Optional[int]


class Manifest:
    @staticmethod
    def read_manifest_entry(input_file: InputFile) -> Iterator[ManifestEntry]:
        with AvroFile(input_file) as reader:
            schema = reader.schema
            for record in reader:
                dict_repr = convert_pos_to_dict(schema, record)
                yield ManifestEntry(**dict_repr)

    @staticmethod
    def read_manifest_list(input_file: InputFile) -> Iterator[ManifestFile]:
        with AvroFile(input_file) as reader:
            schema = reader.schema
            for record in reader:
                dict_repr = convert_pos_to_dict(schema, record)
                yield ManifestFile(**dict_repr)


@singledispatch
def convert_pos_to_dict(schema: Union[Schema, IcebergType], struct: AvroStruct) -> Dict[str, Any]:
    """Converts the positions in the field names

    This makes it easy to map it onto a Pydantic model. Might change later on depending on the performance

     Args:
         schema (Schema | IcebergType): The schema of the file
         struct (AvroStruct): The struct containing the data by positions

     Raises:
         NotImplementedError: If attempting to handle an unknown type in the schema
    """
    raise NotImplementedError(f"Cannot traverse non-type: {schema}")


@convert_pos_to_dict.register(Schema)
def _(schema: Schema, struct: AvroStruct) -> Dict[str, Any]:
    return convert_pos_to_dict(schema.as_struct(), struct)


@convert_pos_to_dict.register(StructType)
def _(struct_type: StructType, values: AvroStruct) -> Dict[str, Any]:
    """Iterates over all the fields in the dict, and gets the data from the struct"""
    return {field.name: convert_pos_to_dict(field.field_type, values.get(pos)) for pos, field in enumerate(struct_type.fields)}


@convert_pos_to_dict.register(ListType)
def _(list_type: ListType, values: List[Any]) -> List[Any]:
    """In the case of a list, we'll go over the elements in the list to handle complex types"""
    return [convert_pos_to_dict(list_type.element_type, value) for value in values]


@convert_pos_to_dict.register(MapType)
def _(map_type: MapType, values: Dict) -> Dict:
    """In the case of a map, we both traverse over the key and value to handle complex types"""
    return {
        convert_pos_to_dict(map_type.key_type, key): convert_pos_to_dict(map_type.value_type, value)
        for key, value in values.items()
    }


@convert_pos_to_dict.register(PrimitiveType)
def _(primitive: PrimitiveType, value: Any) -> Any:
    return value

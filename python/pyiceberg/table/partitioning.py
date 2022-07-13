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
from functools import cached_property
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

from pydantic import Field

from pyiceberg.transforms import Transform
from pyiceberg.utils.iceberg_base_model import IcebergBaseModel

_PARTITION_DATA_ID_START: int = 1000


class PartitionField(IcebergBaseModel):
    """
    PartitionField is a single element with name and unique id,
    It represents how one partition value is derived from the source column via transformation

    Attributes:
        source_id(int): The source column id of table's schema
        field_id(int): The partition field id across all the table partition specs
        transform(Transform): The transform used to produce partition values from source column
        name(str): The name of this partition field
    """

    source_id: int = Field(alias="source-id")
    field_id: int = Field(alias="field-id")
    transform: Transform = Field()
    name: str = Field()

    def __init__(
        self,
        source_id: Optional[int] = None,
        field_id: Optional[int] = None,
        transform: Optional[Transform] = None,
        name: Optional[str] = None,
        **data: Any,
    ):
        if source_id is not None:
            data["source-id"] = source_id
        if field_id is not None:
            data["field-id"] = field_id
        if transform is not None:
            data["transform"] = transform
        if name is not None:
            data["name"] = name
        super().__init__(**data)

    def __str__(self):
        return f"{self.field_id}: {self.name}: {self.transform}({self.source_id})"


class PartitionSpec(IcebergBaseModel):
    """
    PartitionSpec captures the transformation from table data to partition values

    Attributes:
        schema(Schema): the schema of data table
        spec_id(int): any change to PartitionSpec will produce a new specId
        fields(List[PartitionField): list of partition fields to produce partition values
    """

    spec_id: int = Field(alias="spec-id")
    fields: Tuple[PartitionField, ...] = Field()

    def __init__(
        self,
        spec_id: Optional[int] = None,
        fields: Optional[Tuple[PartitionField, ...]] = None,
        **data: Any,
    ):
        if spec_id is not None:
            data["spec-id"] = spec_id
        if fields is not None:
            data["fields"] = fields
        super().__init__(**data)

    def __eq__(self, other: Any) -> bool:
        """
        Produce a boolean to return True if two objects are considered equal

        Note:
            Equality of PartitionSpec is determined by spec_id and partition fields only
        """
        if not isinstance(other, PartitionSpec):
            return False
        return self.spec_id == other.spec_id and self.fields == other.fields

    def __str__(self):
        """
        Produce a human-readable string representation of PartitionSpec

        Note:
            Only include list of partition fields in the PartitionSpec's string representation
        """
        result_str = "["
        if self.fields:
            result_str += "\n  " + "\n  ".join([str(field) for field in self.fields]) + "\n"
        result_str += "]"
        return result_str

    def is_unpartitioned(self) -> bool:
        return not self.fields

    @property
    def last_assigned_field_id(self) -> int:
        if self.fields:
            return max(pf.field_id for pf in self.fields)
        return _PARTITION_DATA_ID_START

    @cached_property
    def source_id_to_fields_map(self) -> Dict[int, List[PartitionField]]:
        source_id_to_fields_map: Dict[int, List[PartitionField]] = {}
        for partition_field in self.fields:
            existing = source_id_to_fields_map.get(partition_field.source_id, [])
            existing.append(partition_field)
            source_id_to_fields_map[partition_field.source_id] = existing
        return source_id_to_fields_map

    def fields_by_source_id(self, field_id: int) -> List[PartitionField]:
        return self.source_id_to_fields_map[field_id]

    def compatible_with(self, other: "PartitionSpec") -> bool:
        """
        Produce a boolean to return True if two PartitionSpec are considered compatible
        """
        if self == other:
            return True
        if len(self.fields) != len(other.fields):
            return False
        return all(
            this_field.source_id == that_field.source_id
            and this_field.transform == that_field.transform
            and this_field.name == that_field.name
            for this_field, that_field in zip(self.fields, other.fields)
        )

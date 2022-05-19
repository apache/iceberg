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
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from iceberg.schema import Schema
from iceberg.transforms import Transform

_PARTITION_DATA_ID_START: int = 1000


@dataclass(frozen=True)
class PartitionField:
    """
    PartitionField is a single element with name and unique id,
    It represents how one partition value is derived from the source column via transformation

    Attributes:
        source_id(int): The source column id of table's schema
        field_id(int): The partition field id across all the table partition specs
        transform(Transform): The transform used to produce partition values from source column
        name(str): The name of this partition field
    """

    source_id: int
    field_id: int
    transform: Transform
    name: str

    def __str__(self):
        return f"{self.field_id}: {self.name}: {self.transform}({self.source_id})"


@dataclass(eq=False, frozen=True)
class PartitionSpec:
    """
    PartitionSpec capture the transformation from table data to partition values

    Attributes:
        schema(Schema): the schema of data table
        spec_id(int): any change to PartitionSpec will produce a new specId
        fields(List[PartitionField): list of partition fields to produce partition values
        last_assigned_field_id(int): auto-increment partition field id starting from PARTITION_DATA_ID_START
    """

    schema: Schema
    spec_id: int
    fields: Tuple[PartitionField]
    last_assigned_field_id: int
    source_id_to_fields_map: Dict[int, List[PartitionField]] = field(init=False)

    def __post_init__(self):
        source_id_to_fields_map = dict()
        for partition_field in self.fields:
            source_column = self.schema.find_column_name(partition_field.source_id)
            if not source_column:
                raise ValueError(f"Cannot find source column: {partition_field.source_id}")
            existing = source_id_to_fields_map.get(partition_field.source_id, [])
            existing.append(partition_field)
            source_id_to_fields_map[partition_field.source_id] = existing
        object.__setattr__(self, "source_id_to_fields_map", source_id_to_fields_map)

    def __eq__(self, other):
        """
        Equality check on spec_id and partition fields only
        """
        return self.spec_id == other.spec_id and self.fields == other.fields

    def __str__(self):
        """
        PartitionSpec str method highlight the partition field only
        """
        result_str = "["
        for partition_field in self.fields:
            result_str += f"\n  {str(partition_field)}"
        if len(self.fields) > 0:
            result_str += "\n"
        result_str += "]"
        return result_str

    def is_unpartitioned(self) -> bool:
        return len(self.fields) < 1

    def fields_by_source_id(self, field_id: int) -> List[PartitionField]:
        return self.source_id_to_fields_map[field_id]

    def compatible_with(self, other: "PartitionSpec") -> bool:
        """
        Returns true if this partition spec is equivalent to the other, with partition field_id ignored.
        That is, if both specs have the same number of fields, field order, field name, source column ids, and transforms.
        """
        return all(
            this_field.source_id == that_field.source_id
            and this_field.transform == that_field.transform
            and this_field.name == that_field.name
            for this_field, that_field in zip(self.fields, other.fields)
        )

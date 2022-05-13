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
from typing import Dict, Iterable, List, Tuple

from iceberg.schema import Schema
from iceberg.transforms import Transform

_PARTITION_DATA_ID_START: int = 1000


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

    def __init__(self, source_id: int, field_id: int, transform: Transform, name: str):
        self._source_id = source_id
        self._field_id = field_id
        self._transform = transform
        self._name = name

    @property
    def source_id(self) -> int:
        return self._source_id

    @property
    def field_id(self) -> int:
        return self._field_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def transform(self) -> Transform:
        return self._transform

    def __eq__(self, other):
        return (
            self.field_id == other.field_id
            and self.source_id == other.source_id
            and self.name == other.name
            and self.transform == other.transform
        )

    def __str__(self):
        return f"{self.field_id}: {self.name}: {self.transform}({self.source_id})"

    def __repr__(self):
        return f"PartitionField(field_id={self.field_id}, name={self.name}, transform={repr(self.transform)}, source_id={self.source_id})"


class PartitionSpec:
    """
    PartitionSpec capture the transformation from table data to partition values

    Attributes:
        schema(Schema): the schema of data table
        spec_id(int): any change to PartitionSpec will produce a new specId
        fields(List[PartitionField): list of partition fields to produce partition values
        last_assigned_field_id(int): auto-increment partition field id starting from PARTITION_DATA_ID_START
    """

    def __init__(self, schema: Schema, spec_id: int, fields: Iterable[PartitionField], last_assigned_field_id: int):
        self._schema = schema
        self._spec_id = spec_id
        self._fields = tuple(fields)
        self._last_assigned_field_id = last_assigned_field_id
        # derived
        self._fields_by_source_id: Dict[int, List[PartitionField]] = {}

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def spec_id(self) -> int:
        return self._spec_id

    @property
    def fields(self) -> Tuple[PartitionField, ...]:
        return self._fields

    @property
    def last_assigned_field_id(self) -> int:
        return self._last_assigned_field_id

    def __eq__(self, other):
        return self.spec_id == other.spec_id and self.fields == other.fields

    def __str__(self):
        result_str = "["
        for partition_field in self.fields:
            result_str += f"\n  {str(partition_field)}"
        if len(self.fields) > 0:
            result_str += "\n"
        result_str += "]"
        return result_str

    def __repr__(self):
        return f"PartitionSpec: {str(self)}"

    def is_unpartitioned(self) -> bool:
        return len(self.fields) < 1

    def fields_by_source_id(self, field_id: int) -> List[PartitionField]:
        if not self._fields_by_source_id:
            for partition_field in self.fields:
                source_column = self.schema.find_column_name(partition_field.source_id)
                if not source_column:
                    raise ValueError(f"Cannot find source column: {partition_field.source_id}")
                existing = self._fields_by_source_id.get(partition_field.source_id, [])
                existing.append(partition_field)
                self._fields_by_source_id[partition_field.source_id] = existing
        return self._fields_by_source_id[field_id]

    def compatible_with(self, other) -> bool:
        """
        Returns true if this partition spec is equivalent to the other, with partition field_id ignored.
        That is, if both specs have the same number of fields, field order, field name, source column ids, and transforms.
        """
        if self == other:
            return True
        if len(self.fields) != len(other.fields):
            return False
        for index in range(len(self.fields)):
            this_field = self.fields[index]
            that_field = other.fields[index]
            if (
                this_field.source_id != that_field.source_id
                or this_field.transform != that_field.transform
                or this_field.name != that_field.name
            ):
                return False
        return True

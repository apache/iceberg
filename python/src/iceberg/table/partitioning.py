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
from iceberg.transforms import Transform


class PartitionField:
    """
    PartitionField is a single element with name and unique id,
    It represents how one partition value is derived from the source column via transformation

    Attributes:
        source_id(int): The source column id of table's schema
        field_id(int): The partition field id across all the table metadata's partition specs
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

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


class PartitionField(object):
    def __init__(self, source_id: int, field_id: int, name: str):
        self._source_id = source_id
        self._field_id = field_id
        self._name = name
        # TODO: add transform field

    @property
    def source_id(self) -> int:
        return self._source_id

    @property
    def field_id(self) -> int:
        return self._field_id

    @property
    def name(self) -> str:
        return self._name

    def __eq__(self, other):
        if isinstance(other, PartitionField):
            return (
                self.source_id == other.source_id
                and self.field_id == other.field_id
                and self.name == other.name
            )
        return False

    def __str__(self):
        # TODO: add transform to str representation
        return f"{self.field_id}: {self.name} ({self._source_id})"

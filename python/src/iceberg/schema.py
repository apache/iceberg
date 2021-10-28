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

from .types import StructType, Type, NestedField


class Schema(object):
    alias_to_id: dict = None
    id_to_field = {}
    name_to_id: dict = None
    lowercase_name_to_id: dict = None

    def __init__(self, struct: StructType, schema_id: int, identifier_field_ids: [int]):
        self._struct = struct
        self._schema_id = schema_id
        self._identifier_field_ids = identifier_field_ids

    @property
    def struct(self):
        return self._struct

    @property
    def schema_id(self):
        return self._schema_id

    @property
    def identifier_field_ids(self):
        return self._identifier_field_ids

    def _generate_id_to_field(self):
        if self.id_to_field is None:
            index = 0
            temp_id_to_field = {}
            for field in self.struct.fields:
                temp_id_to_field[index] = field
                index += 1
            self.id_to_field = temp_id_to_field
        return self.id_to_field

    def find_type(self, field_id):
        field: NestedField = self._generate_id_to_field().get(field_id)
        if field:
            return field.type
        return None

    def _name_to_id(self) -> dict:
        # TODO: needs TypeUtil Methods
        pass

    def find_field(self, name: str) -> NestedField:
        # TODO: needs TypeUtil Methods
        pass


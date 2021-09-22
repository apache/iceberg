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

from typing import Any, TYPE_CHECKING

from iceberg.exceptions import ValidationException

from ..types import StructType

if TYPE_CHECKING:
    from iceberg.api import StructLike


class BoundReference:

    def __init__(self, struct, field):
        self.field = field
        self.pos = self.find(field.field_id, struct)
        self._type = struct.fields[self.pos].type

    @property
    def type(self):
        return self._type

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, BoundReference):
            return False

        return self.field.field_id == other.field.field_id and self.pos == other.pos and self._type == other._type

    def __ne__(self, other):
        return not self.__eq__(other)

    def find(self, field_id, struct):
        fields = struct.fields
        for i, field in enumerate(fields):
            if field.field_id == self.field.field_id:
                return i

        raise ValidationException("Cannot find top-level field id %d in struct: %s", (field_id, struct))

    def get(self, struct):
        return struct.get(self.pos)

    def __str__(self):
        return "ref(id={id}, pos={pos}, type={_type})".format(id=self.field.field_id,
                                                              pos=self.pos,
                                                              _type=self._type)

    @property
    def ref(self):
        return self

    def eval(self, struct: StructLike) -> Any:
        return self.get(struct)


class NamedReference:

    def __init__(self, name):
        super(NamedReference, self).__init__()
        if name is None:
            raise RuntimeError("Name cannot be null")

        self.name = name

    @property
    def ref(self):
        return self

    def bind(self, struct: StructType, case_sensitive: bool = True) -> BoundReference:
        from iceberg.api import Schema
        schema = Schema(struct.fields)
        field = schema.find_field(self.name) if case_sensitive else schema.case_insensitive_find_field(self.name)

        ValidationException.check(field is not None, "Cannot find field '%s' in struct: %s", (self.name,
                                                                                              schema.as_struct()))

        return BoundReference(struct, field)

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, NamedReference):
            return False

        return self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "NamedReference({})".format(self.name)

    def __str__(self):
        return 'ref(name="{}")'.format(self.name)

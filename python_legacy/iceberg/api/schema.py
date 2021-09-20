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

from .types import StructType
from .types import type_util
"""
  The schema of a data table.

"""


class Schema(object):
    NEWLINE = '\n'
    ALL_COLUMNS = "*"

    def __init__(self, *argv):
        aliases = None
        if len(argv) == 1 and isinstance(argv[0], (list, tuple)):
            columns = argv[0]
        elif len(argv) == 2 and isinstance(argv[0], list) and isinstance(argv[1], dict):
            columns = argv[0]
            aliases = argv[1]
        else:
            columns = argv

        self.struct = StructType.of(columns)
        self._alias_to_id = None
        self._id_to_alias = None
        if aliases is not None:
            self._alias_to_id = dict(aliases)
            self._id_to_alias = {v: k for k, v in self._alias_to_id.items()}

        self._id_to_field = None
        self._name_to_id = type_util.index_by_name(self.struct)
        self._id_to_name = {v: k for k, v in self._name_to_id.items()}
        self._lowercase_name_to_id = {k.lower(): v for k, v in self._name_to_id.items()}

    def as_struct(self):
        return self.struct

    def get_aliases(self):
        return self._alias_to_id

    def lazy_id_to_field(self):
        from .types import index_by_id
        if self._id_to_field is None:
            self._id_to_field = index_by_id(self.struct) # noqa

        return self._id_to_field

    def lazy_name_to_id(self):
        from .types import index_by_name
        if self._name_to_id is None:
            self._name_to_id = index_by_name(self.struct)
            self._id_to_name = {v: k for k, v in self._name_to_id.items()}
            self._lowercase_name_to_id = {k.lower(): v for k, v in self._name_to_id.items()}

        return self._name_to_id

    def lazy_lowercase_name_to_id(self):
        from .types import index_by_name
        if self._lowercase_name_to_id is None:
            self._name_to_id = index_by_name(self.struct)
            self._id_to_name = {v: k for k, v in self._name_to_id.items()}
            self._lowercase_name_to_id = {k.lower(): v for k, v in self._name_to_id.items()}

        return self._lowercase_name_to_id

    def columns(self):
        return self.struct.fields

    def find_type(self, name):
        if isinstance(name, int):
            field = self.lazy_id_to_field().get(name)
            if field:
                return field.type
        elif isinstance(name, str):
            id = self.lazy_name_to_id().get(name)
            if id:
                return self.find_type(id)
        else:
            raise RuntimeError("Invalid Column (could not find): %s" % name)

    def find_field(self, id):
        if isinstance(id, int):
            return self.lazy_id_to_field().get(id)

        if not id:
            raise ValueError("Invalid Column Name (empty)")

        id = self.lazy_name_to_id().get(id)
        if id is not None:
            return self.lazy_id_to_field().get(id)

    def case_insensitive_find_field(self, name):
        if name is None:
            raise ValueError("Invalid Column Name (empty)")

        id = self.lazy_lowercase_name_to_id().get(name.lower())
        if id is not None:
            return self.lazy_id_to_field().get(id)

        return None

    def find_column_name(self, id):
        if isinstance(id, int):
            field = self.lazy_id_to_field().get(id)

            return None if field is None else field.name

    def alias_to_id(self, alias):
        if self._alias_to_id:
            return self._alias_to_id.get(alias)

    def id_to_alias(self, field_id):
        if self._id_to_alias:
            return self._id_to_alias.get(field_id)

    def select(self, cols):
        return self._internal_select(True, cols)

    def case_insensitive_select(self, cols):
        return self._internal_select(False, cols)

    def _internal_select(self, case_sensitive, cols):
        from .types import select

        if Schema.ALL_COLUMNS in cols:
            return self

        selected = set()
        for name in cols:
            if case_sensitive:
                field_id = self.lazy_name_to_id().get(name)
            else:
                field_id = self.lazy_lowercase_name_to_id().get(name.lower())

            if field_id is not None:
                selected.add(field_id)

        return select(self, selected)

    def __len__(self):
        return len(self.struct.fields)

    def __repr__(self):
        return "Schema(%s)" % ",".join([str(field) for field in self.struct.fields])

    def __str__(self):
        return "table {\n%s\n}" % Schema.NEWLINE.join([" " + str(field) for field in self.struct.fields])

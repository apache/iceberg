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

"""
  The schema of a data table.

"""


class Schema(object):
    NEWLINE = '\n'
    ALL_COLUMNS = "*"

    def __init__(self, *argv):
        aliases = dict()
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
        self._name_to_id = None
        self._id_to_name = None

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
        return self._name_to_id

    def columns(self):
        return self.struct.fields

    def find_type(self, name):
        if not name:
            raise RuntimeError("Invalid Column Name (empty)")

        if isinstance(name, int):
            field = self.lazy_id_to_field().get(name)
            if field:
                return field.type

        id = self.lazy_name_to_id().get(name)
        if id:
            return self.find_type(id)

        raise RuntimeError("Invalid Column (could not find): %s" % name)

    def find_field(self, id):
        if isinstance(id, int):
            return self.lazy_id_to_field().get(id)

        if not id:
            raise RuntimeError("Invalid Column Name (empty)")

        id = self.lazy_name_to_id().get(id)
        if id:
            return self.lazy_id_to_field().get(id)

    def find_column_name(self, id):
        if isinstance(id, int):
            return self._id_to_name.get(id)

    def alias_to_id(self, alias):
        if self._alias_to_id:
            return self._alias_to_id.get(alias)

    def id_to_alias(self, field_id):
        if self._id_to_alias:
            return self._id_to_alias.get(field_id)

    def select(self, *argv):
        from .types import select
        if not(len(argv) == 1 and isinstance(argv[0], list)):
            return self.select(argv)

        if len(argv) == 1:
            names = argv[0]
            if Schema.ALL_COLUMNS in names:
                return self
            else:
                selected = list()
                for name in names:
                    id = self.lazy_name_to_id().get(name)
                    if id:
                        selected.append(id)

                return select(self, selected) # noqa

        raise RuntimeError("Illegal argument for select %s", argv)

    def __repr__(self):
        return "Schema(%s)" % self.struct.fields

    def __str__(self):
        return "table {\n%s\n}" % Schema.NEWLINE.join([" " + str(field) for field in self.struct.fields])

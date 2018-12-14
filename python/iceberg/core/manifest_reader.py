# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from iceberg.api import Filterable
from iceberg.api.expressions import Expressions
from iceberg.api.io import CloseableGroup

from .manifest_entry import Status

LOG = logging.getLogger("iceberg.core.manifest_reader")


class ManifestReader(CloseableGroup, Filterable):
    ALL_COLUMNS = ["*"]
    CHANGE_COLUMNS = ["file_path", "file_format", "partition", "record_count", "file_size_in_bytes"]

    @staticmethod
    def read(file):
        return ManifestReader(file=file)

    @staticmethod
    def in_memory(spec, entries):
        return ManifestReader(spec=spec, entries=entries)

    def __init__(self, file=None, spec=None, entries=None):
        self.file = file
        self.schema = None
        self.metadata = None
        self.spec = spec
        self.entries = entries
        if self.file is not None:
            self.__init_from_file()
        else:
            self.__init_from_spec()

    def __init_from_file(self):
        pass
        # try:
        #         reader =
        #     self.metadata = header_reader.get_metadata()
        # except Exception as e:
        #     raise e
        # self.schema = SchemaParser.from_json(self.metadata.get("schema"));
        # self.spec = PartitionSpecParser.from_json(self.schema, self.metadata.get("partition-spec"))

    def __init_from_spec(self):
        self.metadata = dict()
        self.schema = self.spec.schema

    def iterator(self, part_filter=None, columns=None):
        if part_filter is None and columns is None:
            return self.iterator(Expressions.always_true(), Filterable.ALL_COLUMNS)

        return iter([entry.file for entry in self.entries if entry.status != Status.DELETED])

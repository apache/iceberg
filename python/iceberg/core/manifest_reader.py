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

import logging

import fastavro
from iceberg.api import FileFormat, Filterable
from iceberg.api.expressions import Expressions, inclusive
from iceberg.api.io import CloseableGroup

from .avro import AvroToIceberg
from .filtered_manifest import FilteredManifest
from .manifest_entry import ManifestEntry, Status
from .partition_spec_parser import PartitionSpecParser
from .schema_parser import SchemaParser
from .table_metadata import TableMetadata

_logger = logging.getLogger(__name__)


class ManifestReader(CloseableGroup, Filterable):
    ALL_COLUMNS = ["*"]
    CHANGE_COLUMNS = ["file_path", "file_format", "partition", "record_count", "file_size_in_bytes"]

    @staticmethod
    def read(file, spec_lookup=None):
        return ManifestReader(file=file, spec_lookup=spec_lookup)

    def select(self, columns):
        return FilteredManifest(self,
                                Expressions.always_true(),
                                Expressions.always_true(),
                                list(columns),
                                self.case_sensitive)

    def filter_partitions(self, expr):
        return FilteredManifest(self,
                                expr,
                                Expressions.always_true(),
                                ManifestReader.ALL_COLUMNS,
                                self.case_sensitive)

    def filter_rows(self, expr):
        return FilteredManifest(self,
                                inclusive(self.spec).project(expr),
                                expr,
                                ManifestReader.ALL_COLUMNS,
                                self.case_sensitive)

    @staticmethod
    def in_memory(spec, entries):
        return ManifestReader(spec=spec, entries=entries)

    def __init__(self, file=None, spec=None, metadata=None, schema=None, case_sensitive=True, spec_lookup=None):
        self.file = file
        self.schema = schema
        self.metadata = metadata
        self.spec = spec
        self._case_sensitive = case_sensitive

        self._entries = None
        self._avro_rows = list()
        self._fo = None
        self._avro_reader = None
        self._avro_rows = None

        if not all([item is not None for item in [self.file, self.metadata, self.spec, self.schema]]):
            if self.spec is not None:
                self.__init_from_spec()
            else:
                self.__init_from_file(spec_lookup)

        self._adds = None
        self._deletes = None

    def __init_from_file(self, spec_lookup):
        self._fo = self.file.new_fo()
        self._avro_reader = fastavro.reader(self._fo)
        self.metadata = self._avro_reader.metadata
        spec_id = int(self.metadata.get("partition-spec-id", TableMetadata.INITIAL_SPEC_ID))

        if spec_lookup is not None:
            self.spec = spec_lookup(spec_id)
            self.schema = self.spec.schema
        else:
            self.schema = SchemaParser.from_json(self.metadata.get("schema"))
            self.spec = PartitionSpecParser.from_json_fields(self.schema, spec_id, self.metadata.get("partition-spec"))

    def __init_from_spec(self):
        self.metadata = dict()
        self.schema = self.spec.schema

    def case_sensitive(self, case_sensitive):
        return ManifestReader(file=self.file, metadata=self.metadata, spec=self.spec,
                              schema=self.schema, case_sensitive=case_sensitive)

    def cache_changes(self):
        adds = list()
        deletes = list()
        for entry in self.entries(ManifestReader.CHANGE_COLUMNS):
            if entry.status == "ADDED":
                adds.append(entry.copy())
            elif entry.status == "DELETED":
                deletes.append(entry.copy())

        self._adds = adds
        self._deletes = deletes

    def added_files(self):
        if self._adds is None:
            self.cache_changes()

        return self._adds

    def deleted_files(self):
        if self._adds is None:
            self.cache_changes()

        return self._deletes

    def entries(self, columns=None):
        if columns is None:
            columns = ManifestReader.ALL_COLUMNS

        file_format = FileFormat.from_file_name(self.file.location())
        if file_format is None:
            raise RuntimeError("Unable to determine format of manifest: %s" % self.file)

        proj_schema = ManifestEntry.project_schema(self.spec.partition_type(), columns)

        if self._entries is None:
            if file_format is FileFormat.AVRO:
                self._entries = list()
                for read_entry in AvroToIceberg.read_avro_row(proj_schema, self._avro_reader):
                    entry = ManifestEntry(schema=proj_schema, partition_type=self.spec.partition_type())
                    for i, key in enumerate(read_entry.keys()):
                        entry.put(i, read_entry[key])
                    self._entries.append(entry)
                self._fo.close()
                self._avro_reader = None

        return self._entries

    def iterator(self, part_filter=None, columns=None):
        if part_filter is None and columns is None:
            return self.iterator(Expressions.always_true(), Filterable.ALL_COLUMNS)

        return [entry.file for entry in self.entries() if entry.status != Status.DELETED]

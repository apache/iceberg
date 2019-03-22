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

import fastavro
from iceberg.api import FileFormat, Filterable
from iceberg.api.expressions import Expressions
from iceberg.api.io import CloseableGroup

from .avro import AvroToIceberg
from .manifest_entry import ManifestEntry, Status
from .partition_spec_parser import PartitionSpecParser
from .schema_parser import SchemaParser
from .table_metadata import TableMetadata


class ManifestReader(CloseableGroup, Filterable):
    ALL_COLUMNS = ["*"]
    CHANGE_COLUMNS = ["file_path", "file_format", "partition", "record_count", "file_size_in_bytes"]

    @staticmethod
    def read(file):
        return ManifestReader(file=file)

    def select(self, columns):
        raise NotImplementedError()

    def filter_partitions(self, expr):
        raise NotImplementedError()

    def filter_rows(self, expr):
        raise NotImplementedError()

    @staticmethod
    def in_memory(spec, entries):
        return ManifestReader(spec=spec, entries=entries)

    def __init__(self, file=None, spec=None, entries=None):
        self.file = file
        self.schema = None
        self.metadata = None
        self.spec = spec
        self._entries = entries
        if self.file is not None:
            self.__init_from_file()
        else:
            self.__init_from_spec()

    def __init_from_file(self):
        try:
            with self.file.new_fo() as fo:
                avro_reader = fastavro.reader(fo)
                self.metadata = avro_reader.metadata
        except Exception as e:
            raise e

        self.schema = SchemaParser.from_json(self.metadata.get("schema"))
        spec_id = int(self.metadata.get("partition-spec-id", TableMetadata.INITIAL_SPEC_ID))
        self.spec = PartitionSpecParser.from_json_fields(self.schema, spec_id, self.metadata.get("partition-spec"))

    def __init_from_spec(self):
        self.metadata = dict()
        self.schema = self.spec.schema

    def entries(self, columns=None):
        if columns is None:
            columns = ManifestReader.ALL_COLUMNS

        format = FileFormat.from_file_name(self.file.location())
        if format is None:
            raise RuntimeError("Unable to determine format of manifest: " + self.file)

        proj_schema = ManifestEntry.project_schema(self.spec.partition_type(), columns)
        read_entries = list()
        if format == FileFormat.AVRO:
            with self.file.new_fo() as fo:
                avro_reader = fastavro.reader(fo)

                for read_entry in AvroToIceberg.read_avro_row(proj_schema, avro_reader):
                    entry = ManifestEntry(schema=proj_schema, partition_type=self.spec.partition_type())
                    for i, key in enumerate(read_entry.keys()):
                        entry.put(i, read_entry[key])
                    read_entries.append(entry)
        else:
            raise RuntimeError("Invalid format for manifest file: " + format)

        return read_entries

    def iterator(self, part_filter=None, columns=None):
        if part_filter is None and columns is None:
            return self.iterator(Expressions.always_true(), Filterable.ALL_COLUMNS)

        return (entry.file for entry in self.entries if entry.status != Status.DELETED)

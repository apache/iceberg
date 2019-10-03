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

from enum import Enum

from iceberg.api import (DataFile,
                         FileFormat,
                         Metrics,
                         Schema)
from iceberg.api.types import (IntegerType,
                               LongType,
                               NestedField)
from iceberg.core.avro import IcebergToAvro

from .generic_data_file import GenericDataFile
from .partition_data import PartitionData


class ManifestEntry():
    AVRO_NAME = "manifest_entry"

    def __init__(self, schema=None, partition_type=None, to_copy=None):
        self.schema = schema
        self.snapshot_id = 0
        self.file = None
        self.status = Status.EXISTING

        if self.schema is None and partition_type is not None:
            self.schema = IcebergToAvro.type_to_schema(ManifestEntry.get_schema(partition_type))

        elif self.schema is None and partition_type is None and to_copy is not None:
            self.schema = to_copy.schema
            self.status = to_copy.status
            self.snapshot_id = to_copy.snapshot_id
            self.file = to_copy.file.copy()

        if self.schema is None:
            raise RuntimeError("Invalid arguments schema=%s, partition_type=%s, to_copy=%s" % (self.schema,
                                                                                               partition_type,
                                                                                               to_copy))

    def wrap_existing(self, snapshot_id, file):
        self.status = Status.EXISTING
        self.snapshot_id = snapshot_id
        self.file = file
        return self

    def wrap_append(self, snapshot_id, file):
        self.status = Status.ADDED
        self.snapshot_id = snapshot_id
        self.file = file
        return self

    def wrap_delete(self, snapshot_id, file):
        self.status = Status.DELETED
        self.snapshot_id = snapshot_id
        self.file = file
        return self

    def copy(self):
        return ManifestEntry(to_copy=self)

    def put(self, i, v):
        if i == 0:
            self.status = Status.from_id(i)
        elif i == 1:
            self.snapshot_id = v
        elif i == 2:
            if isinstance(v, dict):
                metrics = Metrics(row_count=v.get("record_count"),
                                  column_sizes=v.get("column_sizes"),
                                  value_counts=v.get("value_counts"),
                                  null_value_counts=v.get("null_value_counts"),
                                  lower_bounds=v.get("lower_bounds"),
                                  upper_bounds=v.get("upper_bounds"))

                data_file_schema = self.schema.as_struct().field(name="data_file")
                part_data = PartitionData.from_json(data_file_schema
                                                    .type
                                                    .field(name="partition").type, v.get("partition"))

                v = GenericDataFile(v.get("file_path"),
                                    FileFormat[v.get("file_format")],
                                    v.get("file_size_in_bytes"),
                                    v.get("block_size_in_byte"),
                                    row_count=v.get("record_count"),
                                    partition=part_data,
                                    metrics=metrics
                                    )
            self.file = v

    def get(self, i):
        if i == 0:
            return self.status.value
        elif i == 1:
            return self.snapshot_id
        elif i == 2:
            return self.file

    def __repr__(self):
        return "ManifestEntry(status=%s, snapshot_id=%s, file=%s" % (self.status, self.snapshot_id, self.file)

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def project_schema(part_type, columns):
        return ManifestEntry.wrap_file_schema(Schema(DataFile.get_type(part_type).fields)
                                              .select(columns)
                                              .as_struct())

    @staticmethod
    def get_schema(partition_type):
        return ManifestEntry.wrap_file_schema(DataFile.get_type(partition_type))

    @staticmethod
    def wrap_file_schema(file_struct):
        return Schema(NestedField.required(0, "status", IntegerType.get()),
                      NestedField.required(1, "snapshot_id", LongType.get()),
                      NestedField.required(2, "data_file", file_struct))


class Status(Enum):

    EXISTING = 0
    ADDED = 1
    DELETED = 2

    @staticmethod
    def from_id(id):
        return Status(id)

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


from iceberg.api import (FileFormat,
                         Metrics)
from iceberg.api.types import Conversions

from .generic_data_file import GenericDataFile
from .partition_data import PartitionData


class DataFiles(object):
    DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024

    @staticmethod
    def new_partition_data(spec):
        return PartitionData(spec.partition_type())

    @staticmethod
    def copy_partition_data(spec, partition_data, reuse):
        data = reuse
        if data is None:
            data = DataFiles.new_partition_data(spec)

        for i, _ in enumerate(spec.fields):
            data.set(i, partition_data.get(i))

        return data

    @staticmethod
    def fill_from_path(spec, partition_path, reuse):
        data = reuse
        if data is None:
            data = DataFiles.new_partition_data(spec)

        partitions = partition_path.split("/")
        if len(partitions) > len(spec.fields):
            raise RuntimeError("Invalid partition data, too many fields (expecting %s): %s" % (len(spec.fields),
                                                                                               partition_path))

        if len(partitions) < len(spec.fields):
            raise RuntimeError("Invalid partition data, not enough fields(expecting %s): %s" % (len(spec.fields),
                                                                                                partition_path))

        for i, part in enumerate(partitions):
            field = spec.fields[i]
            parts = part.split("=")
            if len(parts) != 2 or parts[0] is None or parts[0] != field.name:
                raise RuntimeError("Invalid partition: %s" % part)

            data.set(i, Conversions.from_partition_string(data.get_type(i), parts[1]))

        return data

    @staticmethod
    def data(spec, partition_path):
        return DataFiles.fill_from_path(spec, partition_path, None)

    @staticmethod
    def copy(spec, partition):
        return DataFiles.copy_partition_data(spec, partition, None)

    @staticmethod
    def from_input_file(input_file, row_count, partition_data=None, metrics=None):
        from .filesystem import FileSystemInputFile
        if isinstance(input_file, FileSystemInputFile):
            return DataFiles.from_stat(input_file.get_stat(), row_count,
                                       partition_data=partition_data, metrics=metrics)

    @staticmethod
    def from_stat(stat, row_count, partition_data=None, metrics=None):
        location = stat.path
        format = FileFormat.from_file_name(location)
        return GenericDataFile(location, format, stat.length, stat.block_size,
                               row_count=row_count, partition=partition_data, metrics=metrics)

    @staticmethod
    def builder(spec=None):
        return DataFileBuilder(spec)


class DataFileBuilder(object):

    def __init__(self, spec=None):
        self.spec = spec
        self.partition_data = DataFiles.new_partition_data(spec) if spec is not None else None
        self.is_partitioned = spec is not None
        self.file_path = None
        self.format = None
        self.record_count = -1
        self.file_size_in_bytes = -1
        self.block_size_in_bytes = -1
        self.column_sizes = None
        self.value_counts = None
        self.null_value_counts = None
        self.lower_bounds = None
        self.upper_bounds = None

    def clear(self):
        if self.is_partitioned:
            self.partition_data.clear()
        self.file_path = None
        self.format = None
        self.record_count = -1
        self.file_size_in_bytes = -1
        self.block_size_in_bytes = -1
        self.column_sizes = None
        self.value_counts = None
        self.null_value_counts = None
        self.lower_bounds = None
        self.upper_bounds = None
        return self

    def copy(self, to_copy):
        if self.is_partitioned:
            self.partition_data = DataFiles.copy_partition_data(self.spec, to_copy.partition, self.partition_data)

        self.file_path = str(to_copy.path())
        self.format = to_copy.format
        self.record_count = to_copy.record_count
        self.file_size_in_bytes = to_copy.file_size_in_bytes
        self.block_size_in_bytes = to_copy.block_size_in_bytes
        self.column_sizes = to_copy.column_sizes
        self.value_counts = to_copy.value_counts
        self.null_value_counts = to_copy.null_value_counts
        self.lower_bounds = to_copy.lower_bounds
        self.upper_bounds = to_copy.upper_bounds
        return self

    def with_status(self, stat):
        self.file_path = stat.path
        self.file_size_in_bytes = stat.length
        self.block_size_in_bytes = stat.blocksize
        return self

    def with_input_file(self, input_file):
        from .filesystem import FileSystemInputFile
        if isinstance(input_file, FileSystemInputFile):
            self.with_status(input_file.get_stat())

        self.file_path = self.location()
        self.file_size_in_bytes = self.get_length()

        return self

    def with_path(self, path):
        self.file_path = path
        return self

    def with_format(self, fmt):
        if isinstance(fmt, FileFormat):
            self.format = fmt
        else:
            self.format = FileFormat[str(fmt).upper()]

        return self

    def with_partition(self, partition):
        self.partition_data = DataFiles.copy_partition_data(self.spec, partition, self.partition_data)
        return self

    def with_record_count(self, record_count):
        self.record_count = record_count
        return self

    def with_file_size_in_bytes(self, file_size_in_bytes):
        self.file_size_in_bytes = file_size_in_bytes
        return self

    def with_block_size_in_bytes(self, block_size_in_bytes):
        self.block_size_in_bytes = block_size_in_bytes
        return self

    def with_partition_path(self, partition_path):
        if not self.is_partitioned:
            raise RuntimeError("Cannot add partition data for an unpartitioned table")

        self.partition_data = DataFiles.fill_from_path(self.spec, partition_path, self.partition_data)
        return self

    def with_metrics(self, metrics):
        self.record_count = metrics.row_count if metrics.row_count is not None else -1
        self.column_sizes = metrics.column_sizes
        self.value_counts = metrics.value_counts
        self.null_value_counts = metrics.null_value_counts
        self.lower_bounds = metrics.lower_bounds
        self.upper_bounds = metrics.upper_bounds
        return self

    def build(self):
        if self.file_path is None:
            raise RuntimeError("File path is required")
        if self.format is None:
            self.format = FileFormat.from_file_name(self.file_path)

        if self.format is None:
            raise RuntimeError("File format is required")

        if self.file_size_in_bytes < 0:
            raise RuntimeError("File size is required")

        if self.record_count < 0:
            raise RuntimeError("Record count is required")

        if self.block_size_in_bytes is None:
            self.block_size_in_bytes = DataFiles.DEFAULT_BLOCK_SIZE

        return GenericDataFile(self.file_path, self.format, self.file_size_in_bytes, self.block_size_in_bytes,
                               partition=self.partition_data.copy() if self.is_partitioned else None,
                               metrics=Metrics(row_count=self.record_count, column_sizes=self.column_sizes,
                                               value_counts=self.value_counts,
                                               null_value_counts=self.null_value_counts,
                                               lower_bounds=self.lower_bounds, upper_bounds=self.upper_bounds))

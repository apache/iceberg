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

import copy

from iceberg.api import (DataFile,
                         StructLike)
from iceberg.api.types import StructType

from .avro.iceberg_to_avro import IcebergToAvro
from .partition_data import PartitionData


class GenericDataFile(DataFile, StructLike):

    EMPTY_STRUCT_TYPE = StructType.of([])
    EMPTY_PARTITION_DATA = PartitionData(EMPTY_STRUCT_TYPE)

    def __init__(self, file_path, format, file_size_in_bytes, block_size_in_bytes,
                 row_count=None, partition=None, metrics=None):

        self._file_path = file_path
        self._format = format
        self._row_count = row_count
        self._file_size_in_bytes = file_size_in_bytes
        self._block_size_in_bytes = block_size_in_bytes
        self._file_ordinal = None
        self._sort_columns = None

        if partition is None:
            self._partition_data = GenericDataFile.EMPTY_PARTITION_DATA
            self._partition_type = GenericDataFile.EMPTY_PARTITION_DATA.partition_type
        else:
            self._partition_data = partition
            self._partition_type = partition.get_partition_type()
        if metrics is None:
            self._row_count = row_count
            self._column_sizes = None
            self._value_counts = None
            self._null_value_counts = None
            self._lower_bounds = None
            self._upper_bounds = None
        else:
            self._row_count = metrics.row_count
            self._column_sizes = metrics.column_sizes
            self._value_counts = metrics.value_counts
            self._null_value_counts = metrics.null_value_counts
            self._lower_bounds = metrics.lower_bounds
            self._upper_bounds = metrics.upper_bounds

    def partition(self):
        return self._partition_data

    def path(self):
        return self._file_path

    def format(self):
        return self._format

    def record_count(self):
        return self._row_count

    def file_size_in_bytes(self):
        return self._file_size_in_bytes

    def block_size_in_bytes(self):
        return self._block_size_in_bytes

    def file_ordinal(self):
        return self._file_ordinal

    def sort_columns(self):
        return self._sort_columns

    def column_sizes(self):
        return self._column_sizes

    def value_counts(self):
        return self._value_counts

    def null_value_counts(self):
        return self._null_value_counts

    def lower_bounds(self):
        return self._lower_bounds

    def upper_bounds(self):
        return self._upper_bounds

    def copy(self):
        return copy.deepcopy(self)

    @staticmethod
    def get_avro_schema(partition_type):
        return IcebergToAvro.type_to_schema(DataFile.get_type(partition_type), DataFile.__class__.__name__)

    def __repr__(self):
        fields = ["file_path: {}".format(self._file_path),
                  "file_format: {}".format(self._format),
                  "partition: {}".format(self._partition_data),
                  "record_count: {}".format(self._row_count),
                  "file_size_in_bytes: {}".format(self._file_size_in_bytes),
                  "block_size_in_bytes: {}".format(self._block_size_in_bytes),
                  "column_sizes: {}".format(self._column_sizes),
                  "value_counts: {}".format(self._value_counts),
                  "null_value_counts: {}".format(self._null_value_counts),
                  "lower_bounds: {}".format(self._lower_bounds),
                  "upper_bounds: {}".format(self._upper_bounds),
                  ]
        return "GenericDataFile({})".format("\n,".join(fields))

    def __str__(self):
        return self.__repr__()

    def __deepcopy__(self, memodict):
        cls = self.__class__
        result = cls.__new__(cls)
        memodict[id(self)] = result

        for k, v in self.__dict__.items():
            setattr(result, k, copy.deepcopy(v, memodict))

        return result

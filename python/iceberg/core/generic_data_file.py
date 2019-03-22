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
        self.file_path = file_path
        self.format = format
        self.row_count = row_count
        self.file_size_in_bytes = file_size_in_bytes
        self.block_size_in_bytes = block_size_in_bytes
        self.file_ordinal = None
        self.sort_columns = None

        if partition is None:
            self.partition_data = GenericDataFile.EMPTY_PARTITION_DATA
            self.partition_type = GenericDataFile.EMPTY_PARTITION_DATA.partition_type
        else:
            self.partition_data = partition
            self.partition_type = partition.get_partition_type()
        if metrics is None:
            self.row_count = row_count
            self.column_sizes = None
            self.value_counts = None
            self.null_value_counts = None
            self.lower_bounds = None
            self.upper_bounds = None
        else:
            self.row_count = metrics.row_count
            self.column_sizes = metrics.column_sizes
            self.value_counts = metrics.value_counts
            self.null_value_counts = metrics.null_value_counts
            self.lower_bounds = metrics.lower_bounds
            self.upper_bounds = metrics.upper_bounds

    def partition(self):
        return self.partition_data

    @staticmethod
    def get_avro_schema(partition_type):
        return IcebergToAvro.type_to_schema(DataFile.get_type(partition_type), DataFile.__class__.__name__)

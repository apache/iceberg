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

from iceberg.api.types import (BinaryType,
                               IntegerType,
                               ListType,
                               LongType,
                               MapType,
                               NestedField,
                               StringType,
                               StructType)


class DataFile(object):

    @staticmethod
    def get_type(partition_type):
        return StructType.of([NestedField.required(100, "file_path", StringType.get()),
                             NestedField.required(101, "file_format", StringType.get()),
                             NestedField.required(102, "partition", partition_type),
                             NestedField.required(103, "record_count", LongType.get()),
                             NestedField.required(104, "file_size_in_bytes", LongType.get()),
                             NestedField.required(105, "block_size_in_bytes", LongType.get()),
                             NestedField.optional(106, "file_ordinal", IntegerType.get()),
                             NestedField.optional(107, "sort_columns", ListType.of_required(112, IntegerType.get())),
                             NestedField.optional(108, "column_sizes", MapType.of_required(117, 118,
                                                                                           IntegerType.get(),
                                                                                           LongType.get())),
                             NestedField.optional(109, "value_counts", MapType.of_required(119, 120,
                                                                                           IntegerType.get(),
                                                                                           LongType.get())),
                             NestedField.optional(110, "null_value_counts", MapType.of_required(121, 122,
                                                                                                IntegerType.get(),
                                                                                                LongType.get())),
                             NestedField.optional(125, "lower_bounds", MapType.of_required(126, 127,
                                                                                           IntegerType.get(),
                                                                                           BinaryType.get())),
                             NestedField.optional(128, "upper_bounds", MapType.of_required(129, 130,
                                                                                           IntegerType.get(),
                                                                                           BinaryType.get()))]
                             # NEXT ID TO ASSIGN: 131
                             )

    def path(self):
        raise NotImplementedError()

    def format(self):
        raise NotImplementedError()

    def partition(self):
        raise NotImplementedError()

    def record_count(self):
        raise NotImplementedError()

    def file_size_in_bytes(self):
        raise NotImplementedError()

    def block_size_in_bytes(self):
        raise NotImplementedError()

    def file_ordinal(self):
        raise NotImplementedError()

    def sort_columns(self):
        raise NotImplementedError()

    def column_sizes(self):
        raise NotImplementedError()

    def value_counts(self):
        raise NotImplementedError()

    def null_value_counts(self):
        raise NotImplementedError()

    def lower_bounds(self):
        raise NotImplementedError()

    def upper_bounds(self):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

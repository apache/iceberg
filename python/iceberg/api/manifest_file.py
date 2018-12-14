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

from .schema import Schema
from .types import (BinaryType,
                    BooleanType,
                    IntegerType,
                    ListType,
                    LongType,
                    NestedField,
                    StringType,
                    StructType)


class ManifestFile(object):
    SCHEMA = Schema(NestedField.required(500, "manifest_path", StringType.get()),
                    NestedField.required(501, "manifest_length", LongType.get()),
                    NestedField.required(502, "partition_spec_id", IntegerType.get()),
                    NestedField.optional(503, "added_snapshot_id", LongType.get()),
                    NestedField.optional(504, "added_data_files_count", IntegerType.get()),
                    NestedField.optional(505, "existing_data_files_count", IntegerType.get()),
                    NestedField.optional(506, "deleted_data_files_count", IntegerType.get()),
                    NestedField
                    .optional(507, "partitions",
                              ListType.of_required(508, StructType.of([NestedField.required(509,
                                                                                            "contains_null",
                                                                                            BooleanType.get()),
                                                                       NestedField.optional(510,
                                                                                            "lower_bound",
                                                                                            BinaryType.get()),
                                                                       NestedField.optional(511,
                                                                                            "upper_bound",
                                                                                            BinaryType.get())]))))

    @staticmethod
    def schema():
        return ManifestFile.SCHEMA

    def copy(self):
        raise RuntimeError("Interface Implementation")


class PartitionFieldSummary(object):
    # TYPE = ManifestFile.schema().find_type("partitions").as_list_type().element_type.as_stuct_type()

    @staticmethod
    def get_type():
        return PartitionFieldSummary.TYPE

    def contains_null(self):
        raise RuntimeError("Interface Implementation")

    def lower_bound(self):
        raise RuntimeError("Interface Implementation")

    def upper_bound(self):
        raise RuntimeError("Interface Implementation")

    def copy(self):
        raise RuntimeError("Interface Implementation")

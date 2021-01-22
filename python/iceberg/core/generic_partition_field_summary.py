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


from iceberg.api import PartitionFieldSummary, StructLike


class GenericPartitionFieldSummary(PartitionFieldSummary, StructLike):

    AVRO_SCHEMA = None  # IcebergToAvro.type_to_schema(PartitionFieldSummary.get_type())

    def __init__(self, avro_schema=None, contains_null=False, lower_bound=None, upper_bound=None, copy=None):
        if copy is not None:
            avro_schema = copy.avro_schema
            contains_null = copy.contains_null()
            lower_bound = copy.lower_bound()
            upper_bound = copy.upper_bound()

        if avro_schema is None:
            avro_schema = GenericPartitionFieldSummary.AVRO_SCHEMA

        self.avro_schema = avro_schema
        self._contains_null = contains_null
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def __str__(self):
        return ("GenericPartitionFieldSummary(contains_null={},lower_bound={}, upper_bound={})"
                .format(self.contains_null(), self.lower_bound(), self.upper_bound()))

    def contains_null(self):
        return self._contains_null

    def get(self, pos):
        raise NotImplementedError()

    def set(self, pos, value):
        raise NotImplementedError()

    def lower_bound(self):
        return self._lower_bound

    def upper_bound(self):
        return self._upper_bound

    def copy(self):
        return GenericPartitionFieldSummary(copy=self)

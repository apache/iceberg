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

from iceberg.api.types.conversions import Conversions

from .generic_partition_field_summary import GenericPartitionFieldSummary


class PartitionSummary(object):

    def __init__(self, spec):
        self.java_classes = spec.java_classes
        self.fields = [PartitionFieldStats(spec.partition_type.fields[i].type)
                       for i in range(len(self.java_classes))]

    def summaries(self):
        return [field.to_summary() for field in self.fields]

    def update(self, partition_key):
        self.update_fields(partition_key)

    def update_fields(self, key):
        for i in range(self.java_classes):
            stats = self.fields[i]
            java_class = self.java_classes[i]
            stats.update(key.get(i, java_class))


class PartitionFieldStats(object):

    def __init__(self, type_var):
        self.contains_null = False
        self.type = type_var
        self.min = None
        self.max = None

    def to_summary(self):
        lower_bound = None if self.min is None else Conversions.to_byte_buffer(self.type.type_id, self.min)
        upper_bound = None if self.max is None else Conversions.to_byte_buffer(self.type.type_id, self.max)
        return GenericPartitionFieldSummary(contains_null=self.contains_null,
                                            lower_bound=lower_bound,
                                            upper_bound=upper_bound)

    def update(self, value):
        if value is None:
            self.contains_null = True
        elif self.min is None:
            self.min = value
            self.max = value
        else:
            if value < self.min:
                self.min = value
            if value > self.max:
                self.max = value

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
import json

from iceberg.api.struct_like import StructLike


class PartitionData(StructLike):

    def __init__(self, schema=None, partition_type=None):
        if partition_type is None:
            self.partition_type = schema.as_nested_type().as_struct_type()
        else:
            for field in partition_type.fields:
                if not field.type.is_primitive_type():
                    raise RuntimeError("Partitions cannot contain nested types: %s" % field.type)
            self.partition_type = partition_type
            # schema = PartitionData.get_schema(self.partition_type)
        self._size = len(self.partition_type.fields)
        self.data = list()
        self.schema = schema
        self.string_schema = str(schema)

    def clear(self):
        self.data = list()

    def copy(self):
        return copy.deepcopy(self)

    def get_partition_type(self):
        return self.partition_type

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, PartitionData):
            return False

        return self.partition_type == other.partition_type and self.data == other.data

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return PartitionData.__class__, self.partition_type, self.data

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "PartitionData{%s}" % (",".join(["{}={}".format(self.partition_type.fields[i],
                                                               datum) for i, datum in enumerate(self.data)]))

    def __len__(self):
        return self._size

    def put(self, i, v):
        self.data.insert(i, v)

    def get(self, pos):
        try:
            return self.data[pos][1]
        except IndexError:
            return None

    @staticmethod
    def from_json(schema, json_obj):
        if isinstance(json_obj, str):
            json_obj = json.loads(json_obj)

        data = PartitionData(partition_type=schema)
        for i, kv_tuple in enumerate(json_obj.items()):
            data.put(i, kv_tuple)
        return data

    def set(self, pos, value):
        raise NotImplementedError()

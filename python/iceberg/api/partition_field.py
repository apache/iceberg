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


class PartitionField(object):

    def __init__(self, source_id, field_id, name, transform):
        self.source_id = source_id
        self.field_id = field_id
        self.name = name
        self.transform = transform

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, PartitionField):
            return False

        return self.source_id == other.source_id and self.field_id == other.field_id and \
            self.name == other.name and self.transform == other.transform

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return PartitionField.__class__, self.source_id, self.field_id, self.name, self.transform

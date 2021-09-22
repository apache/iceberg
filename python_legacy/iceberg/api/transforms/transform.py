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


class Transform(object):

    def __init__(self):
        raise NotImplementedError()

    def apply(self, value):
        raise NotImplementedError()

    def can_transform(self, type_var):
        raise NotImplementedError()

    def get_result_type(self, source_type):
        raise NotImplementedError()

    def project(self, name, predicate):
        raise NotImplementedError()

    def project_strict(self, name, predicate):
        raise NotImplementedError()

    def to_human_string(self, value):
        return str(value)

    def dedup_name(self):
        return self.__str__()

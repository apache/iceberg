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

from .transform import Transform


class VoidTransform(Transform):
    _INSTANCE = None

    @staticmethod
    def get():
        if VoidTransform._INSTANCE is None:
            VoidTransform._INSTANCE = VoidTransform()
        return VoidTransform._INSTANCE

    def __init__(self):
        pass

    def apply(self, value):
        return None

    def can_transform(self, type_var):
        return True

    def get_result_type(self, source_type):
        return source_type

    def project(self, name, predicate):
        return None

    def project_strict(self, name, predicate):
        return None

    def to_human_string(self, value):
        return "null"

    def __str__(self):
        return "void"

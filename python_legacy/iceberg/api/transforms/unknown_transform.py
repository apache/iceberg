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

from typing import Union

from iceberg.api.types import StringType, Type

from .transform import Transform


class UnknownTransform(Transform):

    def __init__(self, source_type: Type, transform: str):
        self.source_type = source_type
        self.transform = transform

    def apply(self, value):
        raise AttributeError(f"Cannot apply unsupported transform: {self.transform}")

    def can_transform(self, type_var) -> bool:
        # assume the transform function can be applied for this type because unknown transform is only used when parsing
        # a transform in an existing table. a different Iceberg version must have already validated it.
        return self.source_type == type_var

    def get_result_type(self, source_type):
        # the actual result type is not known
        return StringType.get()

    def project(self, name, predicate):
        return None

    def project_strict(self, name, predicate):
        return None

    def __str__(self):
        return self.transform

    def __eq__(self, other: Union['UnknownTransform', Transform, object]):
        if id(self) == id(other):
            return True
        elif not isinstance(other, UnknownTransform):
            return False

        return self.source_type == other.source_type and self.transform == other.transform

    def __hash__(self):
        return hash((self.source_type, self.transform))

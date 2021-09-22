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
from .transform_util import TransformUtil
from ..expressions import Expressions
from ..types import TypeID


class Identity(Transform):

    @staticmethod
    def get(type_var):
        return Identity(type_var)

    def __init__(self, type_var):
        self.type_var = type_var

    def apply(self, value):
        return value

    def can_transform(self, type_var):
        return type_var.is_primitive_type()

    def get_result_type(self, source_type):
        return source_type

    def project(self, name, predicate):
        return self.project_strict(name, predicate)

    def project_strict(self, name, predicate):
        if predicate.lit is not None:
            return Expressions.predicate(predicate.op, name, predicate.lit.value)
        else:
            return Expressions.predicate(predicate.op, name)

    def to_human_string(self, value):
        if value is None:
            return "null"

        if self.type_var.type_id == TypeID.DATE:
            return TransformUtil.human_day(value)
        elif self.type_var.type_id == TypeID.TIME:
            return TransformUtil.human_time(value)
        elif self.type_var.type_id == TypeID.TIMESTAMP:
            if self.type_var.adjust_to_utc:
                return TransformUtil.human_timestamp_with_timezone(value)
            else:
                return TransformUtil.human_timestamp_without_timezone(value)
        elif self.type_var.type_id in (TypeID.BINARY, TypeID.FIXED):
            raise NotImplementedError()
            # if isinstance(value, bytearray):
            #     return base64.b64encode(value)
            # elif isinstance(value, bytes):
            #     return base64.b64encode(bytes(value))
            # else:
            #     raise RuntimeError("Unsupported binary type: %s" % value.__class__.__name__)
        else:
            return str(value)

    def __str__(self):
        return "identity"

    def __eq__(self, other):
        if id(self) == id(other):
            return True

        if other is None or not isinstance(other, Identity):
            return False

        return self.type_var == other.type_var

    def __hash__(self):
        return hash(self.__key())

    def __key(self):
        return Identity.__class__, self.type_var

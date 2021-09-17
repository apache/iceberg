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
from .reference import BoundReference, NamedReference
from .. import StructLike
from ..transforms import Transform, Transforms
from ..types import StructType
from ...exceptions import ValidationException


class BoundTransform:

    def __init__(self, ref: BoundReference, transform: Transform):
        self._ref = ref
        self._transform = transform

    @property
    def ref(self):
        return self._ref

    @property
    def type(self):
        return self.transform.get_result_type(self.ref.type)

    @property
    def transform(self):
        return self._transform

    def eval(self, struct: StructLike):
        return self.transform.apply(self.ref.eval(struct))

    def __str__(self):
        return f"{self.transform}({self.ref})"


class UnboundTransform:

    def __init__(self, ref: NamedReference, transform: Transform):
        self._ref = ref
        self._transform = transform

    @property
    def ref(self):
        return self._ref

    @property
    def transform(self):
        return self._transform

    def bind(self, struct: StructType, case_sensitive: bool = True):
        bound_ref = self.ref.bind(struct, case_sensitive=case_sensitive)

        type_transform = Transforms.from_string(bound_ref.type, str(self.transform))
        ValidationException.check(type_transform.can_transform(bound_ref.type),
                                  "Cannot bind: %s cannot transform %s values from '%s'", (self.transform,
                                                                                           bound_ref.type,
                                                                                           self.ref.name))

        return BoundTransform(bound_ref, type_transform)

    def __str__(self):
        return f"{self.transform}({self.ref})"

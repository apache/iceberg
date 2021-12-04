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

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from iceberg.api.types import StructType

if TYPE_CHECKING:
    from iceberg.api import StructLike


class Bound(object):
    def eval(self, struct: StructLike) -> Any:
        """Evaluate current object against struct"""


class Unbound(object):
    @property
    def ref(self):
        """the underlying reference"""

    def bind(self, struct: StructType, case_sensitive: bool = True) -> Bound:
        """bind the current object based on the given struct and return the Bound object"""


class Term(object):
    @property
    def ref(self):
        """Expose the reference for this term"""

    @property
    def type(self):
        """accessor for term type """


class BoundTerm(Bound, Term):

    @property
    def ref(self):
        raise NotImplementedError("Base class does not have implementation")

    @property
    def type(self):
        raise NotImplementedError("Base class does not have implementation")

    def eval(self, struct: StructLike):
        raise NotImplementedError("Base class does not have implementation")


class UnboundTerm(Unbound, Term):

    @property
    def ref(self):
        raise NotImplementedError("Base class does not have implementation")

    def bind(self, struct: StructType, case_sensitive: bool = True):
        raise NotImplementedError("Base class does not have implementation")

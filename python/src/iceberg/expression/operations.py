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

from iceberg.expression.base import BooleanExpression
from iceberg.types import Singleton


class And(BooleanExpression):
    """AND operation expression - logical conjunction"""

    def __init__(self, left: BooleanExpression, right: BooleanExpression):
        self.left = left
        self.right = right

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (isinstance(other, And) and self.left == other.left and self.right == other.right)

    def __invert__(self) -> "Or":
        return Or(self.left.__invert__(), self.right.__invert__())

    def __repr__(self) -> str:
        return f"And({repr(self.left)}, {repr(self.right)})"

    def __str__(self) -> str:
        return f"({self.left} and {self.right})"


class Or(BooleanExpression):
    """OR operation expression - logical disjunction"""

    def __init__(self, left: BooleanExpression, right: BooleanExpression):
        self.left = left
        self.right = right

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (isinstance(other, Or) and self.left == other.left and self.right == other.right)

    def __invert__(self) -> "And":
        return And(self.left.__invert__(), self.right.__invert__())

    def __repr__(self) -> str:
        return f"Or({repr(self.left)}, {repr(self.right)})"

    def __str__(self) -> str:
        return f"({self.left} or {self.right})"


class Not(BooleanExpression):
    """NOT operation expression - logical negation"""

    def __init__(self, child):
        self.child = child

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (isinstance(other, Not) and self.child == other.child)

    def __invert__(self) -> BooleanExpression:
        return self.child

    def __repr__(self) -> str:
        return f"Not({repr(self.child)})"

    def __str__(self) -> str:
        return f"(not {self.child})"


class TrueExp(BooleanExpression, Singleton):
    """TRUE expression"""

    def __invert__(self) -> "FalseExp":
        return FalseExp()

    def __repr__(self) -> str:
        return "True"

    def __str__(self) -> str:
        return "true"


class FalseExp(BooleanExpression, Singleton):
    """FALSE expression"""

    def __invert__(self) -> "TrueExp":
        return TrueExp()

    def __repr__(self) -> str:
        return "False"

    def __str__(self) -> str:
        return "false"

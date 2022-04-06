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
from abc import ABC, abstractmethod
from functools import reduce

from iceberg.types import Singleton


class BooleanExpression(ABC):
    """base class for all boolean expressions"""

    @abstractmethod
    def __invert__(self) -> "BooleanExpression":
        ...


class And(BooleanExpression):
    """AND operation expression - logical conjunction"""

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression) -> BooleanExpression:
        if left is alwaysTrue() and right is alwaysTrue():
            return alwaysTrue()
        elif left is alwaysTrue():
            return right
        elif right is alwaysTrue():
            return left
        if not rest:
            return super().__new__(cls)
        return reduce(And, (left, right, *rest))

    def __init__(self, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression):
        if not rest:
            self.left = left
            self.right = right

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (isinstance(other, And) and self.left == other.left and self.right == other.right)

    def __invert__(self) -> "Or":
        return Or(~self.left, ~self.right)

    def __repr__(self) -> str:
        return f"And({repr(self.left)}, {repr(self.right)})"

    def __str__(self) -> str:
        return f"({self.left} and {self.right})"


class Or(BooleanExpression):
    """OR operation expression - logical disjunction"""

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression) -> BooleanExpression:
        if left is alwaysTrue() or right is alwaysTrue():
            return alwaysTrue()
        elif left is alwaysFalse():
            return right
        elif right is alwaysFalse():
            return left
        if not rest:
            return super().__new__(cls)
        return reduce(Or, (left, right, *rest))

    def __init__(self, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression):
        if not rest:
            self.left = left
            self.right = right

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (isinstance(other, Or) and self.left == other.left and self.right == other.right)

    def __invert__(self) -> "And":
        return And(~self.left, ~self.right)

    def __repr__(self) -> str:
        return f"Or({repr(self.left)}, {repr(self.right)})"

    def __str__(self) -> str:
        return f"({self.left} or {self.right})"


class Not(BooleanExpression):
    """NOT operation expression - logical negation"""

    def __new__(cls, child: BooleanExpression) -> BooleanExpression:
        if child is alwaysTrue():
            return alwaysFalse()
        elif child is alwaysFalse():
            return alwaysTrue()
        elif isinstance(child, Not):
            return child.child
        return super().__new__(cls)

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


class alwaysTrue(BooleanExpression, Singleton):
    """TRUE expression"""

    def __invert__(self) -> "alwaysFalse":
        return alwaysFalse()

    def __repr__(self) -> str:
        return "alwaysTrue()"

    def __str__(self) -> str:
        return "true"


class alwaysFalse(BooleanExpression, Singleton):
    """FALSE expression"""

    def __invert__(self) -> "alwaysTrue":
        return alwaysTrue()

    def __repr__(self) -> str:
        return "alwaysTrue()"

    def __str__(self) -> str:
        return "false"

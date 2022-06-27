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
from abc import ABCMeta, abstractmethod
from enum import Enum, auto
from functools import reduce, singledispatch
from typing import Any, Generic, TypeVar

from iceberg.files import StructProtocol
from iceberg.schema import Accessor, Schema
from iceberg.types import NestedField
from iceberg.utils.singleton import Singleton

T = TypeVar("T")


class Operation(Enum):
    """Operations to be used as components in expressions

    Operations can be negated by calling the negate method.
    >>> Operation.TRUE.negate()
    <Operation.FALSE: 2>
    >>> Operation.IS_NULL.negate()
    <Operation.NOT_NULL: 4>

    The above example uses the OPERATION_NEGATIONS map which maps each enum
    to it's opposite enum.

    Raises:
        ValueError: This is raised when attempting to negate an operation
            that cannot be negated.
    """

    TRUE = auto()
    FALSE = auto()
    IS_NULL = auto()
    NOT_NULL = auto()
    IS_NAN = auto()
    NOT_NAN = auto()
    LT = auto()
    LT_EQ = auto()
    GT = auto()
    GT_EQ = auto()
    EQ = auto()
    NOT_EQ = auto()
    IN = auto()
    NOT_IN = auto()
    NOT = auto()
    AND = auto()
    OR = auto()

    def negate(self) -> "Operation":
        """Returns the operation used when this is negated."""

        try:
            return OPERATION_NEGATIONS[self]
        except KeyError as e:
            raise ValueError(f"No negation defined for operation {self}") from e


OPERATION_NEGATIONS = {
    Operation.TRUE: Operation.FALSE,
    Operation.FALSE: Operation.TRUE,
    Operation.IS_NULL: Operation.NOT_NULL,
    Operation.NOT_NULL: Operation.IS_NULL,
    Operation.IS_NAN: Operation.NOT_NAN,
    Operation.NOT_NAN: Operation.IS_NAN,
    Operation.LT: Operation.GT_EQ,
    Operation.LT_EQ: Operation.GT,
    Operation.GT: Operation.LT_EQ,
    Operation.GT_EQ: Operation.LT,
    Operation.EQ: Operation.NOT_EQ,
    Operation.NOT_EQ: Operation.EQ,
    Operation.IN: Operation.NOT_IN,
    Operation.NOT_IN: Operation.IN,
}


class Literal(Generic[T], metaclass=ABCMeta):
    """Literal which has a value and can be converted between types"""

    def __init__(self, value: T, value_type: type):
        if value is None or not isinstance(value, value_type):
            raise TypeError(f"Invalid literal value: {value} (not a {value_type})")
        self._value = value

    @property
    def value(self) -> T:
        return self._value  # type: ignore

    @abstractmethod
    def to(self, type_var):
        ...  # pragma: no cover

    def __repr__(self):
        return f"{type(self).__name__}({self.value})"

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return self.value > other.value

    def __le__(self, other):
        return self.value <= other.value

    def __ge__(self, other):
        return self.value >= other.value


class BooleanExpression(metaclass=ABCMeta):
    """base class for all boolean expressions"""

    @abstractmethod
    def __invert__(self) -> "BooleanExpression":
        ...


class And(BooleanExpression):
    """AND operation expression - logical conjunction"""

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression):
        if rest:
            return reduce(And, (left, right, *rest))
        if left is AlwaysFalse() or right is AlwaysFalse():
            return AlwaysFalse()
        elif left is AlwaysTrue():
            return right
        elif right is AlwaysTrue():
            return left
        self = super().__new__(cls)
        self._left = left  # type: ignore
        self._right = right  # type: ignore
        return self

    @property
    def left(self) -> BooleanExpression:
        return self._left  # type: ignore

    @property
    def right(self) -> BooleanExpression:
        return self._right  # type: ignore

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

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression):
        if rest:
            return reduce(Or, (left, right, *rest))
        if left is AlwaysTrue() or right is AlwaysTrue():
            return AlwaysTrue()
        elif left is AlwaysFalse():
            return right
        elif right is AlwaysFalse():
            return left
        self = super().__new__(cls)
        self._left = left  # type: ignore
        self._right = right  # type: ignore
        return self

    @property
    def left(self) -> BooleanExpression:
        return self._left  # type: ignore

    @property
    def right(self) -> BooleanExpression:
        return self._right  # type: ignore

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

    def __new__(cls, child: BooleanExpression):
        if child is AlwaysTrue():
            return AlwaysFalse()
        elif child is AlwaysFalse():
            return AlwaysTrue()
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


class AlwaysTrue(BooleanExpression, metaclass=Singleton):
    """TRUE expression"""

    def __invert__(self) -> "AlwaysFalse":
        return AlwaysFalse()

    def __repr__(self) -> str:
        return "AlwaysTrue()"

    def __str__(self) -> str:
        return "true"


class AlwaysFalse(BooleanExpression, metaclass=Singleton):
    """FALSE expression"""

    def __invert__(self) -> "AlwaysTrue":
        return AlwaysTrue()

    def __repr__(self) -> str:
        return "AlwaysFalse()"

    def __str__(self) -> str:
        return "false"


class BoundReference:
    """A reference bound to a field in a schema

    Args:
        field (NestedField): A referenced field in an Iceberg schema
        accessor (Accessor): An Accessor object to access the value at the field's position
    """

    def __init__(self, field: NestedField, accessor: Accessor):
        self._field = field
        self._accessor = accessor

    def __str__(self):
        return f"BoundReference(field={repr(self.field)}, accessor={repr(self._accessor)})"

    def __repr__(self):
        return f"BoundReference(field={repr(self.field)}, accessor={repr(self._accessor)})"

    @property
    def field(self) -> NestedField:
        """The referenced field"""
        return self._field

    def eval(self, struct: StructProtocol) -> Any:
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol

        Args:
            struct (StructProtocol): A row object that abides by the StructProtocol and returns values given a position

        Returns:
            Any: The value at the referenced field's position in `struct`
        """
        return self._accessor.get(struct)


class UnboundReference:
    """A reference not yet bound to a field in a schema

    Args:
        name (str): The name of the field

    Note:
        An unbound reference is sometimes referred to as a "named" reference
    """

    def __init__(self, name: str):
        if not name:
            raise ValueError(f"Name cannot be null: {name}")
        self._name = name

    def __str__(self) -> str:
        return f"UnboundReference(name={repr(self.name)})"

    def __repr__(self) -> str:
        return f"UnboundReference(name={repr(self.name)})"

    @property
    def name(self) -> str:
        return self._name

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundReference:
        """Bind the reference to an Iceberg schema

        Args:
            schema (Schema): An Iceberg schema
            case_sensitive (bool): Whether to consider case when binding the reference to the field

        Raises:
            ValueError: If an empty name is provided

        Returns:
            BoundReference: A reference bound to the specific field in the Iceberg schema
        """
        field = schema.find_field(name_or_id=self.name, case_sensitive=case_sensitive)

        if not field:
            raise ValueError(f"Cannot find field '{self.name}' in schema: {schema}")

        return BoundReference(field=field, accessor=schema.accessor_for_field(field.field_id))


class BooleanExpressionVisitor(Generic[T], metaclass=ABCMeta):
    @abstractmethod
    def visit_true(self) -> T:
        """Visit method for an AlwaysTrue boolean expression

        Note: This visit method has no arguments since AlwaysTrue instances have no context.
        """

    @abstractmethod
    def visit_false(self) -> T:
        """Visit method for an AlwaysFalse boolean expression

        Note: This visit method has no arguments since AlwaysFalse instances have no context.
        """

    @abstractmethod
    def visit_not(self, child_result: T) -> T:
        """Visit method for a Not boolean expression

        Args:
            result (T): The result of visiting the child of the Not boolean expression
        """

    @abstractmethod
    def visit_and(self, left_result: T, right_result: T) -> T:
        """Visit method for an And boolean expression

        Args:
            left_result (T): The result of visiting the left side of the expression
            right_result (T): The result of visiting the right side of the expression
        """

    @abstractmethod
    def visit_or(self, left_result: T, right_result: T) -> T:
        """Visit method for an Or boolean expression

        Args:
            left_result (T): The result of visiting the left side of the expression
            right_result (T): The result of visiting the right side of the expression
        """

    @abstractmethod
    def visit_unbound_predicate(self, predicate) -> T:
        """Visit method for an unbound predicate in an expression tree

        Args:
            predicate (UnboundPredicate): An instance of an UnboundPredicate
        """

    @abstractmethod
    def visit_bound_predicate(self, predicate) -> T:
        """Visit method for a bound predicate in an expression tree

        Args:
            predicate (BoundPredicate): An instance of a BoundPredicate
        """


@singledispatch
def visit(obj, visitor: BooleanExpressionVisitor[T]) -> T:
    """A generic function for applying a boolean expression visitor to any point within an expression

    The function traverses the expression in post-order fashion

    Args:
        obj(BooleanExpression): An instance of a BooleanExpression
        visitor(BooleanExpressionVisitor[T]): An instance of an implementation of the generic BooleanExpressionVisitor base class

    Raises:
        NotImplementedError: If attempting to visit an unsupported expression
    """
    raise NotImplementedError(f"Cannot visit unsupported expression: {obj}")


@visit.register(AlwaysTrue)
def _(obj: AlwaysTrue, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an AlwaysTrue boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_true()


@visit.register(AlwaysFalse)
def _(obj: AlwaysFalse, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an AlwaysFalse boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_false()


@visit.register(Not)
def _(obj: Not, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit a Not boolean expression with a concrete BooleanExpressionVisitor"""
    child_result: T = visit(obj.child, visitor=visitor)
    return visitor.visit_not(child_result=child_result)


@visit.register(And)
def _(obj: And, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an And boolean expression with a concrete BooleanExpressionVisitor"""
    left_result: T = visit(obj.left, visitor=visitor)
    right_result: T = visit(obj.right, visitor=visitor)
    return visitor.visit_and(left_result=left_result, right_result=right_result)


@visit.register(Or)
def _(obj: Or, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an Or boolean expression with a concrete BooleanExpressionVisitor"""
    left_result: T = visit(obj.left, visitor=visitor)
    right_result: T = visit(obj.right, visitor=visitor)
    return visitor.visit_or(left_result=left_result, right_result=right_result)

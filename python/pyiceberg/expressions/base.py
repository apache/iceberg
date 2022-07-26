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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce, singledispatch
from typing import Generic, TypeVar

from pyiceberg.files import StructProtocol
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import NestedField
from pyiceberg.utils.singleton import Singleton

T = TypeVar("T")
B = TypeVar("B")


class Literal(Generic[T], ABC):
    """Literal which has a value and can be converted between types"""

    def __init__(self, value: T, value_type: type):
        if value is None or not isinstance(value, value_type):
            raise TypeError(f"Invalid literal value: {value} (not a {value_type})")
        self._value = value

    @property
    def value(self) -> T:
        return self._value  # type: ignore

    @abstractmethod
    def to(self, type_var) -> Literal:
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


class BooleanExpression(ABC):
    """Represents a boolean expression tree."""

    @abstractmethod
    def __invert__(self) -> BooleanExpression:
        """Transform the Expression into its negated version."""


class Bound(Generic[T], ABC):
    """Represents a bound value expression."""

    def eval(self, struct: StructProtocol):  # pylint: disable=W0613
        ...  # pragma: no cover


class Unbound(Generic[T, B], ABC):
    """Represents an unbound expression node."""

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool) -> B:
        ...  # pragma: no cover


class Term(ABC):
    """An expression that evaluates to a value."""


class BaseReference(Generic[T], Term, ABC):
    """Represents a variable reference in an expression."""


class BoundTerm(Bound[T], Term):
    """Represents a bound term."""


class UnboundTerm(Unbound[T, BoundTerm[T]], Term):
    """Represents an unbound term."""


@dataclass(frozen=True)
class BoundReference(BoundTerm[T], BaseReference[T]):
    """A reference bound to a field in a schema

    Args:
        field (NestedField): A referenced field in an Iceberg schema
        accessor (Accessor): An Accessor object to access the value at the field's position
    """

    field: NestedField
    accessor: Accessor

    def eval(self, struct: StructProtocol) -> T:
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol
        Args:
            struct (StructProtocol): A row object that abides by the StructProtocol and returns values given a position
        Returns:
            Any: The value at the referenced field's position in `struct`
        """
        return self.accessor.get(struct)


@dataclass(frozen=True)
class Reference(UnboundTerm[T], BaseReference[T]):
    """A reference not yet bound to a field in a schema

    Args:
        name (str): The name of the field

    Note:
        An unbound reference is sometimes referred to as a "named" reference
    """

    name: str

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundReference[T]:
        """Bind the reference to an Iceberg schema

        Args:
            schema (Schema): An Iceberg schema
            case_sensitive (bool): Whether to consider case when binding the reference to the field

        Raises:
            ValueError: If an empty name is provided

        Returns:
            BoundReference: A reference bound to the specific field in the Iceberg schema
        """
        field = schema.find_field(name_or_id=self.name, case_sensitive=case_sensitive)  # pylint: disable=redefined-outer-name

        if not field:
            raise ValueError(f"Cannot find field '{self.name}' in schema: {schema}")

        accessor = schema.accessor_for_field(field.field_id)

        if not accessor:
            raise ValueError(f"Cannot find accessor for field '{self.name}' in schema: {schema}")

        return BoundReference(field=field, accessor=accessor)


class BoundPredicate(Bound[T], BooleanExpression):
    _term: BoundTerm[T]
    _literals: tuple[Literal[T], ...]

    def __init__(self, term: BoundTerm[T], *literals: Literal[T]):
        self._term = term
        self._literals = literals
        self._validate_literals()

    def _validate_literals(self):
        if len(self.literals) != 1:
            raise AttributeError(f"{self.__class__.__name__} must have exactly 1 literal.")

    @property
    def term(self) -> BoundTerm[T]:
        return self._term

    @property
    def literals(self) -> tuple[Literal[T], ...]:
        return self._literals

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({str(self.term)}{self.literals and ', '+str(self.literals)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.term)}{self.literals and ', '+repr(self.literals)})"

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (
            type(self) == type(other) and self.term == other.term and self.literals == other.literals
        )


class UnboundPredicate(Unbound[T, BooleanExpression], BooleanExpression, ABC):
    _term: UnboundTerm[T]
    _literals: tuple[Literal[T], ...]

    def __init__(self, term: UnboundTerm[T], *literals: Literal[T]):
        self._term = term
        self._literals = literals
        self._validate_literals()

    def _validate_literals(self):
        if len(self.literals) != 1:
            raise AttributeError(f"{self.__class__.__name__} must have exactly 1 literal.")

    @property
    def term(self) -> UnboundTerm[T]:
        return self._term

    @property
    def literals(self) -> tuple[Literal[T], ...]:
        return self._literals

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({str(self.term)}{self.literals and ', '+str(self.literals)})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.term)}{self.literals and ', '+repr(self.literals)})"

    def __eq__(self, other) -> bool:
        return id(self) == id(other) or (
            type(self) == type(other) and self.term == other.term and self.literals == other.literals
        )


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

    def __invert__(self) -> Or:
        return Or(~self.left, ~self.right)

    def __repr__(self) -> str:
        return f"And({repr(self.left)}, {repr(self.right)})"

    def __str__(self) -> str:
        return f"And({str(self.left)}, {str(self.right)})"


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

    def __invert__(self) -> And:
        return And(~self.left, ~self.right)

    def __repr__(self) -> str:
        return f"Or({repr(self.left)}, {repr(self.right)})"

    def __str__(self) -> str:
        return f"Or({str(self.left)}, {str(self.right)})"


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
        return f"Not({str(self.child)})"


@dataclass(frozen=True)
class AlwaysTrue(BooleanExpression, ABC, Singleton):
    """TRUE expression"""

    def __invert__(self) -> AlwaysFalse:
        return AlwaysFalse()


@dataclass(frozen=True)
class AlwaysFalse(BooleanExpression, ABC, Singleton):
    """FALSE expression"""

    def __invert__(self) -> AlwaysTrue:
        return AlwaysTrue()


class IsNull(UnboundPredicate[T]):
    def __invert__(self) -> NotNull:
        return NotNull(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals is not None:
            raise AttributeError("Null is a unary predicate and takes no Literals.")

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundIsNull[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundIsNull(bound_ref)


class BoundIsNull(BoundPredicate[T]):
    def __invert__(self) -> BoundNotNull:
        return BoundNotNull(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("Null is a unary predicate and takes no Literals.")


class NotNull(UnboundPredicate[T]):
    def __invert__(self) -> IsNull:
        return IsNull(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("NotNull is a unary predicate and takes no Literals.")

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundNotNull[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundNotNull(bound_ref)


class BoundNotNull(BoundPredicate[T]):
    def __invert__(self) -> BoundIsNull:
        return BoundIsNull(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("NotNull is a unary predicate and takes no Literals.")


class IsNaN(UnboundPredicate[T]):
    def __invert__(self) -> NotNaN:
        return NotNaN(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("IsNaN is a unary predicate and takes no Literals.")

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundIsNaN[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundIsNaN(bound_ref)


class BoundIsNaN(BoundPredicate[T]):
    def __invert__(self) -> BoundNotNaN:
        return BoundNotNaN(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("IsNaN is a unary predicate and takes no Literals.")


class NotNaN(UnboundPredicate[T]):
    def __invert__(self) -> IsNaN:
        return IsNaN(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("NotNaN is a unary predicate and takes no Literals.")

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundNotNaN[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundNotNaN(bound_ref)


class BoundNotNaN(BoundPredicate[T]):
    def __invert__(self) -> BoundIsNaN:
        return BoundIsNaN(self.term)

    def _validate_literals(self):  # pylint: disable=W0238
        if self.literals:
            raise AttributeError("NotNaN is a unary predicate and takes no Literals.")


class BoundIn(BoundPredicate[T]):
    def _validate_literals(self):  # pylint: disable=W0238
        if not self.literals:
            raise AttributeError("BoundIn must contain at least 1 literal.")

    def __invert__(self) -> BoundNotIn[T]:
        return BoundNotIn(self.term, *self.literals)


class In(UnboundPredicate[T]):
    def _validate_literals(self):  # pylint: disable=W0238
        if not self.literals:
            raise AttributeError("In must contain at least 1 literal.")

    def __invert__(self) -> NotIn[T]:
        return NotIn(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundIn[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundIn(bound_ref, *tuple(lit.to(bound_ref.field.field_type) for lit in self.literals))  # type: ignore


class BoundNotIn(BoundPredicate[T]):
    def _validate_literals(self):  # pylint: disable=W0238
        if not self.literals:
            raise AttributeError("BoundNotIn must contain at least 1 literal.")

    def __invert__(self) -> BoundIn[T]:
        return BoundIn(self.term, *self.literals)


class NotIn(UnboundPredicate[T]):
    def _validate_literals(self):  # pylint: disable=W0238
        if not self.literals:
            raise AttributeError("NotIn must contain at least 1 literal.")

    def __invert__(self) -> In[T]:
        return In(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundNotIn[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundNotIn(bound_ref, *tuple(lit.to(bound_ref.field.field_type) for lit in self.literals))  # type: ignore


class BoundEq(BoundPredicate[T]):
    def __invert__(self) -> BoundNotEq[T]:
        return BoundNotEq(self.term, *self.literals)


class Eq(UnboundPredicate[T]):
    def __invert__(self) -> NotEq[T]:
        return NotEq(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundEq[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundEq(bound_ref, self.literals[0].to(bound_ref.field.field_type))  # type: ignore


class BoundNotEq(BoundPredicate[T]):
    def __invert__(self) -> BoundEq[T]:
        return BoundEq(self.term, *self.literals)


class NotEq(UnboundPredicate[T]):
    def __invert__(self) -> Eq[T]:
        return Eq(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundNotEq[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundNotEq(bound_ref, self.literals[0].to(bound_ref.field.field_type))  # type: ignore


class BoundLt(BoundPredicate[T]):
    def __invert__(self) -> BoundGtEq[T]:
        return BoundGtEq(self.term, *self.literals)


class Lt(UnboundPredicate[T]):
    def __invert__(self) -> GtEq[T]:
        return GtEq(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundEq[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundLt(bound_ref, self.literals[0].to(bound_ref.field.field_type))  # type: ignore


class BoundGtEq(BoundPredicate[T]):
    def __invert__(self) -> BoundLt[T]:
        return BoundLt(self.term, *self.literals)


class GtEq(UnboundPredicate[T]):
    def __invert__(self) -> Lt[T]:
        return Lt(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundEq[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundGtEq(bound_ref, self.literals[0].to(bound_ref.field.field_type))  # type: ignore


class BoundGt(BoundPredicate[T]):
    def __invert__(self) -> BoundLtEq[T]:
        return BoundLtEq(self.term, *self.literals)


class Gt(UnboundPredicate[T]):
    def __invert__(self) -> LtEq[T]:
        return LtEq(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundEq[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundGt(bound_ref, self.literals[0].to(bound_ref.field.field_type))  # type: ignore


class BoundLtEq(BoundPredicate[T]):
    def __invert__(self) -> BoundGt[T]:
        return BoundGt(self.term, *self.literals)


class LtEq(UnboundPredicate[T]):
    def __invert__(self) -> Gt[T]:
        return Gt(self.term, *self.literals)

    def bind(self, schema: Schema, case_sensitive: bool) -> BoundEq[T]:
        bound_ref = self.term.bind(schema, case_sensitive)
        return BoundLtEq(bound_ref, self.literals[0].to(bound_ref.field.field_type))  # type: ignore


class BooleanExpressionVisitor(Generic[T], ABC):
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


@visit.register(In)
def _(obj: In, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an In boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_unbound_predicate(predicate=obj)


@visit.register(Or)
def _(obj: Or, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an Or boolean expression with a concrete BooleanExpressionVisitor"""
    left_result: T = visit(obj.left, visitor=visitor)
    right_result: T = visit(obj.right, visitor=visitor)
    return visitor.visit_or(left_result=left_result, right_result=right_result)


class BindVisitor(BooleanExpressionVisitor[BooleanExpression]):
    """Rewrites a boolean expression by replacing unbound references with references to fields in a struct schema

    Args:
      schema (Schema): A schema to use when binding the expression
      case_sensitive (bool): Whether to consider case when binding a reference to a field in a schema, defaults to True
    """

    def __init__(self, schema: Schema, case_sensitive: bool = True) -> None:
        self._schema = schema
        self._case_sensitive = case_sensitive

    def visit_true(self) -> BooleanExpression:
        return AlwaysTrue()

    def visit_false(self) -> BooleanExpression:
        return AlwaysFalse()

    def visit_not(self, child_result: BooleanExpression) -> BooleanExpression:
        return Not(child=child_result)

    def visit_and(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return And(left=left_result, right=right_result)

    def visit_or(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return Or(left=left_result, right=right_result)

    def visit_unbound_predicate(self, predicate) -> BooleanExpression:
        return predicate.bind(self._schema, case_sensitive=self._case_sensitive)

    def visit_bound_predicate(self, predicate) -> BooleanExpression:
        raise TypeError(f"Found already bound predicate: {predicate}")

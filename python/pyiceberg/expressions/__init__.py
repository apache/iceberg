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
from functools import reduce
from typing import ClassVar, Generic, TypeVar

from pyiceberg.expressions.literals import Literal
from pyiceberg.files import StructProtocol
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import DoubleType, FloatType, NestedField
from pyiceberg.utils.singleton import Singleton

T = TypeVar("T")
B = TypeVar("B")


class BooleanExpression(ABC):
    """An expression that evaluates to a boolean"""

    @abstractmethod
    def __invert__(self) -> BooleanExpression:
        """Transform the Expression into its negated version."""


class Term(Generic[T], ABC):
    """A simple expression that evaluates to a value"""


class Bound(ABC):
    """Represents a bound value expression"""


class Unbound(Generic[B], ABC):
    """Represents an unbound value expression"""

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool = True) -> B:
        ...  # pragma: no cover


class BoundTerm(Term[T], Bound, ABC):
    """Represents a bound term"""

    @abstractmethod
    def ref(self) -> BoundReference[T]:
        """Returns the bound reference"""

    @abstractmethod
    def eval(self, struct: StructProtocol) -> T:  # pylint: disable=W0613
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol"""


@dataclass(frozen=True)
class BoundReference(BoundTerm[T]):
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

    def ref(self) -> BoundReference[T]:
        return self


class UnboundTerm(Term[T], Unbound[BoundTerm[T]], ABC):
    """Represents an unbound term."""


@dataclass(frozen=True)
class Reference(UnboundTerm[T]):
    """A reference not yet bound to a field in a schema

    Args:
        name (str): The name of the field

    Note:
        An unbound reference is sometimes referred to as a "named" reference
    """

    name: str

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundReference[T]:
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
        accessor = schema.accessor_for_field(field.field_id)
        return BoundReference(field=field, accessor=accessor)


@dataclass(frozen=True, init=False)
class And(BooleanExpression):
    """AND operation expression - logical conjunction"""

    left: BooleanExpression
    right: BooleanExpression

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression):
        if rest:
            return reduce(And, (left, right, *rest))
        if left is AlwaysFalse() or right is AlwaysFalse():
            return AlwaysFalse()
        elif left is AlwaysTrue():
            return right
        elif right is AlwaysTrue():
            return left
        else:
            result = super().__new__(cls)
            object.__setattr__(result, "left", left)
            object.__setattr__(result, "right", right)
            return result

    def __invert__(self) -> Or:
        return Or(~self.left, ~self.right)


@dataclass(frozen=True, init=False)
class Or(BooleanExpression):
    """OR operation expression - logical disjunction"""

    left: BooleanExpression
    right: BooleanExpression

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression):
        if rest:
            return reduce(Or, (left, right, *rest))
        if left is AlwaysTrue() or right is AlwaysTrue():
            return AlwaysTrue()
        elif left is AlwaysFalse():
            return right
        elif right is AlwaysFalse():
            return left
        else:
            result = super().__new__(cls)
            object.__setattr__(result, "left", left)
            object.__setattr__(result, "right", right)
            return result

    def __invert__(self) -> And:
        return And(~self.left, ~self.right)


@dataclass(frozen=True, init=False)
class Not(BooleanExpression):
    """NOT operation expression - logical negation"""

    child: BooleanExpression

    def __new__(cls, child: BooleanExpression):
        if child is AlwaysTrue():
            return AlwaysFalse()
        elif child is AlwaysFalse():
            return AlwaysTrue()
        elif isinstance(child, Not):
            return child.child
        result = super().__new__(cls)
        object.__setattr__(result, "child", child)
        return result

    def __invert__(self) -> BooleanExpression:
        return self.child


@dataclass(frozen=True)
class AlwaysTrue(BooleanExpression, Singleton):
    """TRUE expression"""

    def __invert__(self) -> AlwaysFalse:
        return AlwaysFalse()


@dataclass(frozen=True)
class AlwaysFalse(BooleanExpression, Singleton):
    """FALSE expression"""

    def __invert__(self) -> AlwaysTrue:
        return AlwaysTrue()


@dataclass(frozen=True)
class BoundPredicate(Generic[T], Bound, BooleanExpression):
    term: BoundTerm[T]

    def __invert__(self) -> BoundPredicate[T]:
        """Inverts the predicate"""
        raise NotImplementedError


@dataclass(frozen=True)
class UnboundPredicate(Generic[T], Unbound[BooleanExpression], BooleanExpression):
    as_bound: ClassVar[type]
    term: UnboundTerm[T]

    def __invert__(self) -> UnboundPredicate[T]:
        """Inverts the predicate"""
        raise NotImplementedError

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BooleanExpression:
        """Binds the predicate to a schema"""
        raise NotImplementedError


@dataclass(frozen=True)
class UnaryPredicate(UnboundPredicate[T]):
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BooleanExpression:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term)

    def __invert__(self) -> UnaryPredicate[T]:
        """Inverts the unary predicate"""
        raise NotImplementedError


@dataclass(frozen=True)
class BoundUnaryPredicate(BoundPredicate[T]):
    def __invert__(self) -> BoundUnaryPredicate[T]:
        """Inverts the unary predicate"""
        raise NotImplementedError


@dataclass(frozen=True)
class BoundIsNull(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        if term.ref().field.required:
            return AlwaysFalse()
        return super().__new__(cls)

    def __invert__(self) -> BoundNotNull[T]:
        return BoundNotNull(self.term)


@dataclass(frozen=True)
class BoundNotNull(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        if term.ref().field.required:
            return AlwaysTrue()
        return super().__new__(cls)

    def __invert__(self) -> BoundIsNull:
        return BoundIsNull(self.term)


@dataclass(frozen=True)
class IsNull(UnaryPredicate[T]):
    as_bound = BoundIsNull

    def __invert__(self) -> NotNull[T]:
        return NotNull(self.term)


@dataclass(frozen=True)
class NotNull(UnaryPredicate[T]):
    as_bound = BoundNotNull

    def __invert__(self) -> IsNull[T]:
        return IsNull(self.term)


@dataclass(frozen=True)
class BoundIsNaN(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        bound_type = term.ref().field.field_type
        if type(bound_type) in {FloatType, DoubleType}:
            return super().__new__(cls)
        return AlwaysFalse()

    def __invert__(self) -> BoundNotNaN[T]:
        return BoundNotNaN(self.term)


@dataclass(frozen=True)
class BoundNotNaN(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        bound_type = term.ref().field.field_type
        if type(bound_type) in {FloatType, DoubleType}:
            return super().__new__(cls)
        return AlwaysTrue()

    def __invert__(self) -> BoundIsNaN[T]:
        return BoundIsNaN(self.term)


@dataclass(frozen=True)
class IsNaN(UnaryPredicate[T]):
    as_bound = BoundIsNaN

    def __invert__(self) -> NotNaN[T]:
        return NotNaN(self.term)


@dataclass(frozen=True)
class NotNaN(UnaryPredicate[T]):
    as_bound = BoundNotNaN

    def __invert__(self) -> IsNaN[T]:
        return IsNaN(self.term)


@dataclass(frozen=True)
class SetPredicate(UnboundPredicate[T]):
    literals: tuple[Literal[T], ...]

    def __invert__(self) -> SetPredicate[T]:
        """Inverted expression of the SetPredicate"""
        raise NotImplementedError

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BooleanExpression:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term, {lit.to(bound_term.ref().field.field_type) for lit in self.literals})


@dataclass(frozen=True)
class BoundSetPredicate(BoundPredicate[T]):
    literals: set[Literal[T]]

    def __invert__(self) -> BoundSetPredicate[T]:
        """Inverted expression of the SetPredicate"""
        raise NotImplementedError


@dataclass(frozen=True)
class BoundIn(BoundSetPredicate[T]):
    def __new__(cls, term: BoundTerm[T], literals: set[Literal[T]]):  # pylint: disable=W0221
        count = len(literals)
        if count == 0:
            return AlwaysFalse()
        elif count == 1:
            return BoundEqualTo(term, next(iter(literals)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> BoundNotIn[T]:
        return BoundNotIn(self.term, self.literals)


@dataclass(frozen=True)
class BoundNotIn(BoundSetPredicate[T]):
    def __new__(cls, term: BoundTerm[T], literals: set[Literal[T]]):  # pylint: disable=W0221
        count = len(literals)
        if count == 0:
            return AlwaysTrue()
        elif count == 1:
            return BoundNotEqualTo(term, next(iter(literals)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> BoundIn[T]:
        return BoundIn(self.term, self.literals)


@dataclass(frozen=True)
class In(SetPredicate[T]):
    as_bound = BoundIn

    def __new__(cls, term: UnboundTerm[T], literals: tuple[Literal[T], ...]):  # pylint: disable=W0221
        count = len(literals)
        if count == 0:
            return AlwaysFalse()
        elif count == 1:
            return EqualTo(term, literals[0])
        else:
            return super().__new__(cls)

    def __invert__(self) -> NotIn[T]:
        return NotIn(self.term, self.literals)


@dataclass(frozen=True)
class NotIn(SetPredicate[T]):
    as_bound = BoundNotIn

    def __new__(cls, term: UnboundTerm[T], literals: tuple[Literal[T], ...]):  # pylint: disable=W0221
        count = len(literals)
        if count == 0:
            return AlwaysTrue()
        elif count == 1:
            return NotEqualTo(term, literals[0])
        else:
            return super().__new__(cls)

    def __invert__(self) -> In[T]:
        return In(self.term, self.literals)


@dataclass(frozen=True)
class LiteralPredicate(UnboundPredicate[T]):
    literal: Literal[T]

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BooleanExpression:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term, self.literal.to(bound_term.ref().field.field_type))

    def __invert__(self) -> LiteralPredicate[T]:
        """Inverts the literal predicate"""
        raise NotImplementedError


@dataclass(frozen=True)
class BoundLiteralPredicate(BoundPredicate[T]):
    literal: Literal[T]

    def __invert__(self) -> BoundLiteralPredicate[T]:
        """Inverts the bound literal predicate"""
        raise NotImplementedError


@dataclass(frozen=True)
class BoundEqualTo(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundNotEqualTo[T]:
        return BoundNotEqualTo(self.term, self.literal)


@dataclass(frozen=True)
class BoundNotEqualTo(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundEqualTo[T]:
        return BoundEqualTo(self.term, self.literal)


@dataclass(frozen=True)
class BoundGreaterThanOrEqual(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundLessThan[T]:
        return BoundLessThan(self.term, self.literal)


@dataclass(frozen=True)
class BoundGreaterThan(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundLessThanOrEqual[T]:
        return BoundLessThanOrEqual(self.term, self.literal)


@dataclass(frozen=True)
class BoundLessThan(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundGreaterThanOrEqual[T]:
        return BoundGreaterThanOrEqual(self.term, self.literal)


@dataclass(frozen=True)
class BoundLessThanOrEqual(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundGreaterThan[T]:
        return BoundGreaterThan(self.term, self.literal)


@dataclass(frozen=True)
class EqualTo(LiteralPredicate[T]):
    as_bound = BoundEqualTo

    def __invert__(self) -> NotEqualTo[T]:
        return NotEqualTo(self.term, self.literal)


@dataclass(frozen=True)
class NotEqualTo(LiteralPredicate[T]):
    as_bound = BoundNotEqualTo

    def __invert__(self) -> EqualTo[T]:
        return EqualTo(self.term, self.literal)


@dataclass(frozen=True)
class LessThan(LiteralPredicate[T]):
    as_bound = BoundLessThan

    def __invert__(self) -> GreaterThanOrEqual[T]:
        return GreaterThanOrEqual(self.term, self.literal)


@dataclass(frozen=True)
class GreaterThanOrEqual(LiteralPredicate[T]):
    as_bound = BoundGreaterThanOrEqual

    def __invert__(self) -> LessThan[T]:
        return LessThan(self.term, self.literal)


@dataclass(frozen=True)
class GreaterThan(LiteralPredicate[T]):
    as_bound = BoundGreaterThan

    def __invert__(self) -> LessThanOrEqual[T]:
        return LessThanOrEqual(self.term, self.literal)


@dataclass(frozen=True)
class LessThanOrEqual(LiteralPredicate[T]):
    as_bound = BoundLessThanOrEqual

    def __invert__(self) -> GreaterThan[T]:
        return GreaterThan(self.term, self.literal)

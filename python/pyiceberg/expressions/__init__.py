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
from functools import cached_property, reduce
from typing import (
    Any,
    Generic,
    Iterable,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pyiceberg.expressions.literals import (
    AboveMax,
    BelowMin,
    Literal,
    literal,
)
from pyiceberg.schema import Accessor, Schema
from pyiceberg.typedef import L, StructProtocol
from pyiceberg.types import DoubleType, FloatType, NestedField
from pyiceberg.utils.singleton import Singleton


def _to_unbound_term(term: Union[str, UnboundTerm[Any]]) -> UnboundTerm[Any]:
    return Reference(term) if isinstance(term, str) else term


def _to_literal_set(values: Union[Iterable[L], Iterable[Literal[L]]]) -> Set[Literal[L]]:
    return {_to_literal(v) for v in values}


def _to_literal(value: Union[L, Literal[L]]) -> Literal[L]:
    if isinstance(value, Literal):
        return value
    else:
        return literal(value)


class BooleanExpression(ABC):
    """An expression that evaluates to a boolean."""

    @abstractmethod
    def __invert__(self) -> BooleanExpression:
        """Transform the Expression into its negated version."""


class Term(Generic[L], ABC):
    """A simple expression that evaluates to a value."""


class Bound(ABC):
    """Represents a bound value expression."""


B = TypeVar("B")


class Unbound(Generic[B], ABC):
    """Represents an unbound value expression."""

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool = True) -> B:
        ...

    @property
    @abstractmethod
    def as_bound(self) -> Type[Bound]:
        ...


class BoundTerm(Term[L], Bound, ABC):
    """Represents a bound term."""

    @abstractmethod
    def ref(self) -> BoundReference[L]:
        """Returns the bound reference."""

    @abstractmethod
    def eval(self, struct: StructProtocol) -> L:  # pylint: disable=W0613
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol."""


class BoundReference(BoundTerm[L]):
    """A reference bound to a field in a schema.

    Args:
        field (NestedField): A referenced field in an Iceberg schema.
        accessor (Accessor): An Accessor object to access the value at the field's position.
    """

    field: NestedField
    accessor: Accessor

    def __init__(self, field: NestedField, accessor: Accessor):
        self.field = field
        self.accessor = accessor

    def eval(self, struct: StructProtocol) -> L:
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol.

        Args:
            struct (StructProtocol): A row object that abides by the StructProtocol and returns values given a position.
        Returns:
            Any: The value at the referenced field's position in `struct`.
        """
        return self.accessor.get(struct)

    def __eq__(self, other: Any) -> bool:
        return self.field == other.field if isinstance(other, BoundReference) else False

    def __repr__(self) -> str:
        return f"BoundReference(field={repr(self.field)}, accessor={repr(self.accessor)})"

    def ref(self) -> BoundReference[L]:
        return self


class UnboundTerm(Term[Any], Unbound[BoundTerm[L]], ABC):
    """Represents an unbound term."""

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundTerm[L]:
        ...


class Reference(UnboundTerm[Any]):
    """A reference not yet bound to a field in a schema.

    Args:
        name (str): The name of the field.

    Note:
        An unbound reference is sometimes referred to as a "named" reference.
    """

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"Reference(name={repr(self.name)})"

    def __eq__(self, other: Any) -> bool:
        return self.name == other.name if isinstance(other, Reference) else False

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundReference[L]:
        """Bind the reference to an Iceberg schema.

        Args:
            schema (Schema): An Iceberg schema.
            case_sensitive (bool): Whether to consider case when binding the reference to the field.

        Raises:
            ValueError: If an empty name is provided.

        Returns:
            BoundReference: A reference bound to the specific field in the Iceberg schema.
        """
        field = schema.find_field(name_or_id=self.name, case_sensitive=case_sensitive)
        accessor = schema.accessor_for_field(field.field_id)
        return self.as_bound(field=field, accessor=accessor)  # type: ignore

    @property
    def as_bound(self) -> Type[BoundReference[L]]:
        return BoundReference[L]


class And(BooleanExpression):
    """AND operation expression - logical conjunction."""

    left: BooleanExpression
    right: BooleanExpression

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression) -> BooleanExpression:  # type: ignore
        if rest:
            return reduce(And, (left, right, *rest))
        if left is AlwaysFalse() or right is AlwaysFalse():
            return AlwaysFalse()
        elif left is AlwaysTrue():
            return right
        elif right is AlwaysTrue():
            return left
        else:
            obj = super().__new__(cls)
            obj.left = left
            obj.right = right
            return obj

    def __eq__(self, other: Any) -> bool:
        return self.left == other.left and self.right == other.right if isinstance(other, And) else False

    def __str__(self) -> str:
        return f"And(left={str(self.left)}, right={str(self.right)})"

    def __repr__(self) -> str:
        return f"And(left={repr(self.left)}, right={repr(self.right)})"

    def __invert__(self) -> BooleanExpression:
        # De Morgan's law: not (A and B) = (not A) or (not B)
        return Or(~self.left, ~self.right)

    def __getnewargs__(self) -> Tuple[BooleanExpression, BooleanExpression]:
        return (self.left, self.right)


class Or(BooleanExpression):
    """OR operation expression - logical disjunction."""

    left: BooleanExpression
    right: BooleanExpression

    def __new__(cls, left: BooleanExpression, right: BooleanExpression, *rest: BooleanExpression) -> BooleanExpression:  # type: ignore
        if rest:
            return reduce(Or, (left, right, *rest))
        if left is AlwaysTrue() or right is AlwaysTrue():
            return AlwaysTrue()
        elif left is AlwaysFalse():
            return right
        elif right is AlwaysFalse():
            return left
        else:
            obj = super().__new__(cls)
            obj.left = left
            obj.right = right
            return obj

    def __eq__(self, other: Any) -> bool:
        return self.left == other.left and self.right == other.right if isinstance(other, Or) else False

    def __repr__(self) -> str:
        return f"Or(left={repr(self.left)}, right={repr(self.right)})"

    def __invert__(self) -> BooleanExpression:
        # De Morgan's law: not (A or B) = (not A) and (not B)
        return And(~self.left, ~self.right)

    def __getnewargs__(self) -> Tuple[BooleanExpression, BooleanExpression]:
        return (self.left, self.right)


class Not(BooleanExpression):
    """NOT operation expression - logical negation."""

    child: BooleanExpression

    def __new__(cls, child: BooleanExpression) -> BooleanExpression:  # type: ignore
        if child is AlwaysTrue():
            return AlwaysFalse()
        elif child is AlwaysFalse():
            return AlwaysTrue()
        elif isinstance(child, Not):
            return child.child
        obj = super().__new__(cls)
        obj.child = child
        return obj

    def __repr__(self) -> str:
        return f"Not(child={repr(self.child)})"

    def __eq__(self, other: Any) -> bool:
        return self.child == other.child if isinstance(other, Not) else False

    def __invert__(self) -> BooleanExpression:
        return self.child

    def __getnewargs__(self) -> Tuple[BooleanExpression]:
        return (self.child,)


class AlwaysTrue(BooleanExpression, Singleton):
    """TRUE expression."""

    def __invert__(self) -> AlwaysFalse:
        return AlwaysFalse()

    def __str__(self) -> str:
        return "AlwaysTrue()"

    def __repr__(self) -> str:
        return "AlwaysTrue()"


class AlwaysFalse(BooleanExpression, Singleton):
    """FALSE expression."""

    def __invert__(self) -> AlwaysTrue:
        return AlwaysTrue()

    def __str__(self) -> str:
        return "AlwaysFalse()"

    def __repr__(self) -> str:
        return "AlwaysFalse()"


class BoundPredicate(Generic[L], Bound, BooleanExpression, ABC):
    term: BoundTerm[L]

    def __init__(self, term: BoundTerm[L]):
        self.term = term

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BoundPredicate):
            return self.term == other.term
        return False

    @property
    @abstractmethod
    def as_unbound(self) -> Type[UnboundPredicate[Any]]:
        ...


class UnboundPredicate(Generic[L], Unbound[BooleanExpression], BooleanExpression, ABC):
    term: UnboundTerm[Any]

    def __init__(self, term: Union[str, UnboundTerm[Any]]):
        self.term = _to_unbound_term(term)

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term if isinstance(other, UnboundPredicate) else False

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BooleanExpression:
        ...

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundPredicate[L]]:
        ...


class UnaryPredicate(UnboundPredicate[Any], ABC):
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundUnaryPredicate[Any]:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term)

    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)})"

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundUnaryPredicate[Any]]:
        ...


class BoundUnaryPredicate(BoundPredicate[L], ABC):
    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)})"

    @property
    @abstractmethod
    def as_unbound(self) -> Type[UnaryPredicate]:
        ...

    def __getnewargs__(self) -> Tuple[BoundTerm[L]]:
        return (self.term,)


class BoundIsNull(BoundUnaryPredicate[L]):
    def __new__(cls, term: BoundTerm[L]) -> BooleanExpression:  # type: ignore  # pylint: disable=W0221
        if term.ref().field.required:
            return AlwaysFalse()
        return super().__new__(cls)

    def __invert__(self) -> BoundNotNull[L]:
        return BoundNotNull(self.term)

    @property
    def as_unbound(self) -> Type[IsNull]:
        return IsNull


class BoundNotNull(BoundUnaryPredicate[L]):
    def __new__(cls, term: BoundTerm[L]):  # type: ignore  # pylint: disable=W0221
        if term.ref().field.required:
            return AlwaysTrue()
        return super().__new__(cls)

    def __invert__(self) -> BoundIsNull[L]:
        return BoundIsNull(self.term)

    @property
    def as_unbound(self) -> Type[NotNull]:
        return NotNull


class IsNull(UnaryPredicate):
    def __invert__(self) -> NotNull:
        return NotNull(self.term)

    @property
    def as_bound(self) -> Type[BoundIsNull[L]]:
        return BoundIsNull[L]


class NotNull(UnaryPredicate):
    def __invert__(self) -> IsNull:
        return IsNull(self.term)

    @property
    def as_bound(self) -> Type[BoundNotNull[L]]:
        return BoundNotNull[L]


class BoundIsNaN(BoundUnaryPredicate[L]):
    def __new__(cls, term: BoundTerm[L]) -> BooleanExpression:  # type: ignore  # pylint: disable=W0221
        bound_type = term.ref().field.field_type
        if type(bound_type) in {FloatType, DoubleType}:
            return super().__new__(cls)
        return AlwaysFalse()

    def __invert__(self) -> BoundNotNaN[L]:
        return BoundNotNaN(self.term)

    @property
    def as_unbound(self) -> Type[IsNaN]:
        return IsNaN


class BoundNotNaN(BoundUnaryPredicate[L]):
    def __new__(cls, term: BoundTerm[L]) -> BooleanExpression:  # type: ignore  # pylint: disable=W0221
        bound_type = term.ref().field.field_type
        if type(bound_type) in {FloatType, DoubleType}:
            return super().__new__(cls)
        return AlwaysTrue()

    def __invert__(self) -> BoundIsNaN[L]:
        return BoundIsNaN(self.term)

    @property
    def as_unbound(self) -> Type[NotNaN]:
        return NotNaN


class IsNaN(UnaryPredicate):
    def __invert__(self) -> NotNaN:
        return NotNaN(self.term)

    @property
    def as_bound(self) -> Type[BoundIsNaN[L]]:
        return BoundIsNaN[L]


class NotNaN(UnaryPredicate):
    def __invert__(self) -> IsNaN:
        return IsNaN(self.term)

    @property
    def as_bound(self) -> Type[BoundNotNaN[L]]:
        return BoundNotNaN[L]


class SetPredicate(UnboundPredicate[L], ABC):
    literals: Set[Literal[L]]

    def __init__(self, term: Union[str, UnboundTerm[Any]], literals: Union[Iterable[L], Iterable[Literal[L]]]):
        super().__init__(term)
        self.literals = _to_literal_set(literals)

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundSetPredicate[L]:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term, {lit.to(bound_term.ref().field.field_type) for lit in self.literals})

    def __str__(self) -> str:
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({str(self.term)}, {{{', '.join(sorted([str(literal) for literal in self.literals]))}}})"

    def __repr__(self) -> str:
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({repr(self.term)}, {{{', '.join(sorted([repr(literal) for literal in self.literals]))}}})"

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term and self.literals == other.literals if isinstance(other, SetPredicate) else False

    def __getnewargs__(self) -> Tuple[UnboundTerm[L], Set[Literal[L]]]:
        return (self.term, self.literals)

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundSetPredicate[L]]:
        return BoundSetPredicate[L]


class BoundSetPredicate(BoundPredicate[L], ABC):
    literals: Set[Literal[L]]

    def __init__(self, term: BoundTerm[L], literals: Set[Literal[L]]):
        # Since we don't know the type of BoundPredicate[L], we have to ignore this one
        super().__init__(term)  # type: ignore
        self.literals = _to_literal_set(literals)  # pylint: disable=W0621

    @cached_property
    def value_set(self) -> Set[L]:
        return {lit.value for lit in self.literals}

    def __str__(self) -> str:
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({str(self.term)}, {{{', '.join(sorted([str(literal) for literal in self.literals]))}}})"

    def __repr__(self) -> str:
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({repr(self.term)}, {{{', '.join(sorted([repr(literal) for literal in self.literals]))}}})"

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term and self.literals == other.literals if isinstance(other, BoundSetPredicate) else False

    def __getnewargs__(self) -> Tuple[BoundTerm[L], Set[Literal[L]]]:
        return (self.term, self.literals)

    @property
    @abstractmethod
    def as_unbound(self) -> Type[SetPredicate[L]]:
        ...


class BoundIn(BoundSetPredicate[L]):
    def __new__(cls, term: BoundTerm[L], literals: Set[Literal[L]]) -> BooleanExpression:  # type: ignore  # pylint: disable=W0221
        count = len(literals)
        if count == 0:
            return AlwaysFalse()
        elif count == 1:
            return BoundEqualTo(term, next(iter(literals)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> BoundNotIn[L]:
        return BoundNotIn(self.term, self.literals)

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term and self.literals == other.literals if isinstance(other, BoundIn) else False

    @property
    def as_unbound(self) -> Type[In[L]]:
        return In


class BoundNotIn(BoundSetPredicate[L]):
    def __new__(  # type: ignore  # pylint: disable=W0221
        cls,
        term: BoundTerm[L],
        literals: Set[Literal[L]],
    ) -> BooleanExpression:
        count = len(literals)
        if count == 0:
            return AlwaysTrue()
        elif count == 1:
            return BoundNotEqualTo(term, next(iter(literals)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> BoundIn[L]:
        return BoundIn(self.term, self.literals)

    @property
    def as_unbound(self) -> Type[NotIn[L]]:
        return NotIn


class In(SetPredicate[L]):
    def __new__(  # type: ignore  # pylint: disable=W0221
        cls, term: Union[str, UnboundTerm[Any]], literals: Union[Iterable[L], Iterable[Literal[L]]]
    ) -> BooleanExpression:
        literals_set: Set[Literal[L]] = _to_literal_set(literals)
        count = len(literals_set)
        if count == 0:
            return AlwaysFalse()
        elif count == 1:
            return EqualTo(term, next(iter(literals)))  # type: ignore
        else:
            return super().__new__(cls)

    def __invert__(self) -> NotIn[L]:
        return NotIn[L](self.term, self.literals)

    @property
    def as_bound(self) -> Type[BoundIn[L]]:
        return BoundIn[L]


class NotIn(SetPredicate[L], ABC):
    def __new__(  # type: ignore  # pylint: disable=W0221
        cls, term: Union[str, UnboundTerm[Any]], literals: Union[Iterable[L], Iterable[Literal[L]]]
    ) -> BooleanExpression:
        literals_set: Set[Literal[L]] = _to_literal_set(literals)
        count = len(literals_set)
        if count == 0:
            return AlwaysTrue()
        elif count == 1:
            return NotEqualTo(term, next(iter(literals_set)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> In[L]:
        return In[L](self.term, self.literals)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, NotIn):
            return self.term == other.term and self.literals == other.literals
        return False

    @property
    def as_bound(self) -> Type[BoundNotIn[L]]:
        return BoundNotIn[L]


class LiteralPredicate(UnboundPredicate[L], ABC):
    literal: Literal[L]

    def __init__(self, term: Union[str, UnboundTerm[Any]], literal: Union[L, Literal[L]]):  # pylint: disable=W0621
        super().__init__(term)
        self.literal = _to_literal(literal)  # pylint: disable=W0621

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundLiteralPredicate[L]:
        bound_term = self.term.bind(schema, case_sensitive)
        lit = self.literal.to(bound_term.ref().field.field_type)

        if isinstance(lit, AboveMax):
            if isinstance(self, (LessThan, LessThanOrEqual, NotEqualTo)):
                return AlwaysTrue()  # type: ignore
            elif isinstance(self, (GreaterThan, GreaterThanOrEqual, EqualTo)):
                return AlwaysFalse()  # type: ignore
        elif isinstance(lit, BelowMin):
            if isinstance(self, (GreaterThan, GreaterThanOrEqual, NotEqualTo)):
                return AlwaysTrue()  # type: ignore
            elif isinstance(self, (LessThan, LessThanOrEqual, EqualTo)):
                return AlwaysFalse()  # type: ignore

        return self.as_bound(bound_term, lit)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, LiteralPredicate):
            return self.term == other.term and self.literal == other.literal
        return False

    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)}, literal={repr(self.literal)})"

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundLiteralPredicate[L]]:
        ...


class BoundLiteralPredicate(BoundPredicate[L], ABC):
    literal: Literal[L]

    def __init__(self, term: BoundTerm[L], literal: Literal[L]):  # pylint: disable=W0621
        # Since we don't know the type of BoundPredicate[L], we have to ignore this one
        super().__init__(term)  # type: ignore
        self.literal = literal  # pylint: disable=W0621

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BoundLiteralPredicate):
            return self.term == other.term and self.literal == other.literal
        return False

    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)}, literal={repr(self.literal)})"

    @property
    @abstractmethod
    def as_unbound(self) -> Type[LiteralPredicate[L]]:
        ...


class BoundEqualTo(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundNotEqualTo[L]:
        return BoundNotEqualTo[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[EqualTo[L]]:
        return EqualTo


class BoundNotEqualTo(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundEqualTo[L]:
        return BoundEqualTo[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[NotEqualTo[L]]:
        return NotEqualTo


class BoundGreaterThanOrEqual(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundLessThan[L]:
        return BoundLessThan[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[GreaterThanOrEqual[L]]:
        return GreaterThanOrEqual[L]


class BoundGreaterThan(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundLessThanOrEqual[L]:
        return BoundLessThanOrEqual(self.term, self.literal)

    @property
    def as_unbound(self) -> Type[GreaterThan[L]]:
        return GreaterThan[L]


class BoundLessThan(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundGreaterThanOrEqual[L]:
        return BoundGreaterThanOrEqual[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[LessThan[L]]:
        return LessThan[L]


class BoundLessThanOrEqual(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundGreaterThan[L]:
        return BoundGreaterThan[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[LessThanOrEqual[L]]:
        return LessThanOrEqual[L]


class BoundStartsWith(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundNotStartsWith[L]:
        return BoundNotStartsWith[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[StartsWith[L]]:
        return StartsWith[L]


class BoundNotStartsWith(BoundLiteralPredicate[L]):
    def __invert__(self) -> BoundStartsWith[L]:
        return BoundStartsWith[L](self.term, self.literal)

    @property
    def as_unbound(self) -> Type[NotStartsWith[L]]:
        return NotStartsWith[L]


class EqualTo(LiteralPredicate[L]):
    def __invert__(self) -> NotEqualTo[L]:
        return NotEqualTo[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundEqualTo[L]]:
        return BoundEqualTo[L]


class NotEqualTo(LiteralPredicate[L]):
    def __invert__(self) -> EqualTo[L]:
        return EqualTo[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundNotEqualTo[L]]:
        return BoundNotEqualTo[L]


class LessThan(LiteralPredicate[L]):
    def __invert__(self) -> GreaterThanOrEqual[L]:
        return GreaterThanOrEqual[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundLessThan[L]]:
        return BoundLessThan[L]


class GreaterThanOrEqual(LiteralPredicate[L]):
    def __invert__(self) -> LessThan[L]:
        return LessThan[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundGreaterThanOrEqual[L]]:
        return BoundGreaterThanOrEqual[L]


class GreaterThan(LiteralPredicate[L]):
    def __invert__(self) -> LessThanOrEqual[L]:
        return LessThanOrEqual[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundGreaterThan[L]]:
        return BoundGreaterThan[L]


class LessThanOrEqual(LiteralPredicate[L]):
    def __invert__(self) -> GreaterThan[L]:
        return GreaterThan[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundLessThanOrEqual[L]]:
        return BoundLessThanOrEqual[L]


class StartsWith(LiteralPredicate[L]):
    def __invert__(self) -> NotStartsWith[L]:
        return NotStartsWith[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundStartsWith[L]]:
        return BoundStartsWith[L]


class NotStartsWith(LiteralPredicate[L]):
    def __invert__(self) -> NotStartsWith[L]:
        return NotStartsWith[L](self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundNotStartsWith[L]]:
        return BoundNotStartsWith[L]

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
from functools import reduce
from typing import (
    Any,
    Generic,
    Iterable,
    Literal,
    Set,
    Type,
    TypeVar,
    Union,
)

from pyiceberg.files import StructProtocol
from pyiceberg.schema import Accessor, Schema
from pyiceberg.types import DoubleType, FloatType, NestedField
from pyiceberg.utils.singleton import Singleton

T = TypeVar("T")


def _to_unbound_term(term: Union[str, UnboundTerm]) -> UnboundTerm:
    return Reference(term) if isinstance(term, str) else term



class BooleanExpression(ABC):
    """An expression that evaluates to a boolean"""

    @abstractmethod
    def __invert__(self) -> BooleanExpression:
        """Transform the Expression into its negated version."""


class Term(Generic[T], ABC):
    """A simple expression that evaluates to a value"""


class Bound(ABC):
    """Represents a bound value expression"""


class Unbound(ABC):
    """Represents an unbound value expression"""

    @property
    @abstractmethod
    def as_bound(self) -> Type[Bound]:
        ...


class BoundTerm(Term[T], Bound, ABC):
    """Represents a bound term"""

    @abstractmethod
    def ref(self) -> BoundReference[T]:
        """Returns the bound reference"""

    @abstractmethod
    def eval(self, struct: StructProtocol) -> T:  # pylint: disable=W0613
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol"""


class BoundReference(BoundTerm[T]):
    """A reference bound to a field in a schema

    Args:
        field (NestedField): A referenced field in an Iceberg schema
        accessor (Accessor): An Accessor object to access the value at the field's position
    """

    field: NestedField
    accessor: Accessor

    def __init__(self, field: NestedField, accessor: Accessor):
        self.field = field
        self.accessor = accessor

    def eval(self, struct: StructProtocol) -> T:
        """Returns the value at the referenced field's position in an object that abides by the StructProtocol

        Args:
            struct (StructProtocol): A row object that abides by the StructProtocol and returns values given a position
        Returns:
            Any: The value at the referenced field's position in `struct`
        """
        return self.accessor.get(struct)

    def __eq__(self, other):
        return self.field == other.field if isinstance(other, BoundReference) else False

    def __repr__(self) -> str:
        return f"BoundReference(field={repr(self.field)}, accessor={repr(self.accessor)})"

    def ref(self) -> BoundReference[T]:
        return self


class UnboundTerm(Term, Unbound, ABC):
    """Represents an unbound term."""

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundTerm:
        ...  # pragma: no cover


class Reference(UnboundTerm):
    """A reference not yet bound to a field in a schema

    Args:
        name (str): The name of the field

    Note:
        An unbound reference is sometimes referred to as a "named" reference
    """

    name: str

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"Reference(name={repr(self.name)})"

    def __eq__(self, other):
        return self.name == other.name if isinstance(other, Reference) else False

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
        return self.as_bound(field=field, accessor=accessor)

    @property
    def as_bound(self) -> Type[BoundReference]:
        return BoundReference[T]


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

    def __invert__(self) -> Or:
        return Or(~self.left, ~self.right)


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
            obj = super().__new__(cls)
            obj.left = left
            obj.right = right
            return obj

    def __eq__(self, other: Any) -> bool:
        return self.left == other.left and self.right == other.right if isinstance(other, Or) else False

    def __repr__(self) -> str:
        return f"Or(left={repr(self.left)}, right={repr(self.right)})"

    def __invert__(self) -> And:
        return And(~self.left, ~self.right)


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
        obj = super().__new__(cls)
        obj.child = child
        return obj

    def __repr__(self) -> str:
        return f"Not(child={repr(self.child)})"

    def __eq__(self, other: Any) -> bool:
        return self.child == other.child if isinstance(other, Not) else False

    def __invert__(self) -> BooleanExpression:
        return self.child


class AlwaysTrue(BooleanExpression, Singleton):
    """TRUE expression"""

    def __invert__(self) -> AlwaysFalse:
        return AlwaysFalse()

    def __str__(self):
        return "AlwaysTrue()"

    def __repr__(self):
        return "AlwaysTrue()"


class AlwaysFalse(BooleanExpression, Singleton):
    """FALSE expression"""

    def __invert__(self) -> AlwaysTrue:
        return AlwaysTrue()

    def __str__(self):
        return "AlwaysFalse()"

    def __repr__(self):
        return "AlwaysFalse()"


class BoundPredicate(Generic[T], Bound, BooleanExpression, ABC):
    term: BoundTerm[T]

    def __init__(self, term: BoundTerm[T]):
        self.term = term

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BoundPredicate):
            return self.term == other.term
        return False


class UnboundPredicate(Unbound, BooleanExpression, ABC):
    term: UnboundTerm

    def __init__(self, term: Union[str, UnboundTerm]):
        self.term = _to_unbound_term(term)

    def __eq__(self, other):
        return self.term == other.term if isinstance(other, UnboundPredicate) else False

    @abstractmethod
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundPredicate:
        ...

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundPredicate]:
        ...


class UnaryPredicate(UnboundPredicate, ABC):
    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundUnaryPredicate:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term)

    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)})"

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundUnaryPredicate[T]]:
        ...


class BoundUnaryPredicate(BoundPredicate[T], ABC):
    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)})"


class BoundIsNull(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        if term.ref().field.required:
            return AlwaysFalse()
        return super().__new__(cls)

    def __invert__(self) -> BoundNotNull[T]:
        return BoundNotNull(self.term)


class BoundNotNull(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        if term.ref().field.required:
            return AlwaysTrue()
        return super().__new__(cls)

    def __invert__(self) -> BoundIsNull:
        return BoundIsNull(self.term)


class IsNull(UnaryPredicate):
    def __invert__(self) -> NotNull:
        return NotNull(self.term)

    @property
    def as_bound(self) -> Type[BoundIsNull[T]]:
        return BoundIsNull[T]


class NotNull(UnaryPredicate):
    def __invert__(self) -> IsNull:
        return IsNull(self.term)

    @property
    def as_bound(self) -> Type[BoundNotNull[T]]:
        return BoundNotNull[T]


class BoundIsNaN(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        bound_type = term.ref().field.field_type
        if type(bound_type) in {FloatType, DoubleType}:
            return super().__new__(cls)
        return AlwaysFalse()

    def __invert__(self) -> BoundNotNaN[T]:
        return BoundNotNaN(self.term)


class BoundNotNaN(BoundUnaryPredicate[T]):
    def __new__(cls, term: BoundTerm[T]):  # pylint: disable=W0221
        bound_type = term.ref().field.field_type
        if type(bound_type) in {FloatType, DoubleType}:
            return super().__new__(cls)
        return AlwaysTrue()

    def __invert__(self) -> BoundIsNaN[T]:
        return BoundIsNaN(self.term)


class IsNaN(UnaryPredicate):
    def __invert__(self) -> NotNaN:
        return NotNaN(self.term)

    @property
    def as_bound(self) -> Type[BoundIsNaN[T]]:
        return BoundIsNaN[T]


class NotNaN(UnaryPredicate):
    def __invert__(self) -> IsNaN:
        return IsNaN(self.term)

    @property
    def as_bound(self) -> Type[BoundNotNaN[T]]:
        return BoundNotNaN[T]


class SetPredicate(Generic[T], UnboundPredicate, ABC):
    literals: Set[Literal[T]]

    def __init__(self, term: Union[str, UnboundTerm], literals: Union[Iterable, Iterable[Literal[T]]]):
        super().__init__(term)
        self.literals = _convert_into_set(literals)

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundSetPredicate[T]:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term, {lit.to(bound_term.ref().field.field_type) for lit in self.literals})

    def __str__(self):
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({str(self.term)}, {{{', '.join(sorted([str(literal) for literal in self.literals]))}}})"

    def __repr__(self) -> str:
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({repr(self.term)}, {{{', '.join(sorted([repr(literal) for literal in self.literals]))}}})"

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term and self.literals == other.literals if isinstance(other, SetPredicate) else False

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundSetPredicate[T]]:
        return BoundSetPredicate[T]


class BoundSetPredicate(BoundPredicate[T], ABC):
    literals: Set[Literal[T]]

    def __init__(self, term: BoundTerm[T], literals: Union[Iterable, Iterable[Literal[T]]]):
        super().__init__(term)
        self.literals = _convert_into_set(literals)  # pylint: disable=W0621

    def __str__(self):
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({str(self.term)}, {{{', '.join(sorted([str(literal) for literal in self.literals]))}}})"

    def __repr__(self) -> str:
        # Sort to make it deterministic
        return f"{str(self.__class__.__name__)}({repr(self.term)}, {{{', '.join(sorted([repr(literal) for literal in self.literals]))}}})"

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term and self.literals == other.literals if isinstance(other, BoundSetPredicate) else False


class BoundIn(BoundSetPredicate[T]):
    def __new__(cls, term: BoundTerm[T], literals: Union[Iterable, Iterable[Literal[T]]]):  # pylint: disable=W0221
        literals_set: Set[Literal[T]] = _convert_into_set(literals)
        count = len(literals_set)
        if count == 0:
            return AlwaysFalse()
        elif count == 1:
            return BoundEqualTo(term, next(iter(literals_set)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> BoundNotIn[T]:
        return BoundNotIn(self.term, self.literals)

    def __eq__(self, other: Any) -> bool:
        return self.term == other.term and self.literals == other.literals if isinstance(other, BoundIn) else False


class BoundNotIn(BoundSetPredicate[T]):
    def __new__(  # pylint: disable=W0221
        cls,
        term: BoundTerm[T],
        literals: Union[Iterable, Iterable[Literal[T]]],
    ):
        literals_set: Set[Literal[T]] = _convert_into_set(literals)
        count = len(literals_set)
        if count == 0:
            return AlwaysTrue()
        elif count == 1:
            return BoundNotEqualTo(term, next(iter(literals_set)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> BoundIn[T]:
        return BoundIn(self.term, self.literals)


class In(SetPredicate[T]):
    def __new__(cls, term: Union[str, UnboundTerm], literals: Union[Iterable, Set[Literal[T]]]):  # pylint: disable=W0221
        literals_set: Set[Literal[T]] = _convert_into_set(literals)
        count = len(literals_set)
        if count == 0:
            return AlwaysFalse()
        elif count == 1:
            return EqualTo(term, next(iter(literals)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> NotIn:
        return NotIn[T](self.term, self.literals)

    @property
    def as_bound(self) -> Type[BoundIn[T]]:
        return BoundIn[T]


class NotIn(SetPredicate[T], ABC):
    def __new__(cls, term: Union[str, UnboundTerm], literals: Union[Iterable, Iterable[Literal[T]]]):  # pylint: disable=W0221
        literals_set: Set[Literal[T]] = _convert_into_set(literals)
        count = len(literals_set)
        if count == 0:
            return AlwaysTrue()
        elif count == 1:
            return NotEqualTo(term, next(iter(literals_set)))
        else:
            return super().__new__(cls)

    def __invert__(self) -> In:
        return In[T](self.term, self.literals)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, NotIn):
            return self.term == other.term and self.literals == other.literals
        return False

    @property
    def as_bound(self) -> Type[BoundNotIn[T]]:
        return BoundNotIn[T]


class LiteralPredicate(Generic[T], UnboundPredicate, ABC):
    literal: Literal[T]

    def __init__(self, term: Union[str, UnboundTerm], literal: Union[T, Literal[T]]):  # pylint: disable=W0621
        super().__init__(term)
        self.literal = _to_literal(literal)  # pylint: disable=W0621

    def bind(self, schema: Schema, case_sensitive: bool = True) -> BoundLiteralPredicate[T]:
        bound_term = self.term.bind(schema, case_sensitive)
        return self.as_bound(bound_term, self.literal.to(bound_term.ref().field.field_type))

    def __eq__(self, other):
        if isinstance(other, LiteralPredicate):
            return self.term == other.term and self.literal == other.literal
        return False

    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)}, literal={repr(self.literal)})"

    @property
    @abstractmethod
    def as_bound(self) -> Type[BoundLiteralPredicate[T]]:
        ...


class BoundLiteralPredicate(BoundPredicate[T], ABC):
    literal: Literal[T]

    def __init__(self, term: BoundTerm[T], literal: Union[Any, Literal[T]]):  # pylint: disable=W0621
        super().__init__(term)
        self.literal = _to_literal(literal)  # pylint: disable=W0621

    def __eq__(self, other):
        if isinstance(other, BoundLiteralPredicate):
            return self.term == other.term and self.literal == other.literal
        return False

    def __repr__(self) -> str:
        return f"{str(self.__class__.__name__)}(term={repr(self.term)}, literal={repr(self.literal)})"


class BoundEqualTo(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundNotEqualTo[T]:
        return BoundNotEqualTo(self.term, self.literal)


class BoundNotEqualTo(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundEqualTo[T]:
        return BoundEqualTo(self.term, self.literal)


class BoundGreaterThanOrEqual(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundLessThan[T]:
        return BoundLessThan(self.term, self.literal)


class BoundGreaterThan(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundLessThanOrEqual[T]:
        return BoundLessThanOrEqual(self.term, self.literal)


class BoundLessThan(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundGreaterThanOrEqual[T]:
        return BoundGreaterThanOrEqual(self.term, self.literal)


class BoundLessThanOrEqual(BoundLiteralPredicate[T]):
    def __invert__(self) -> BoundGreaterThan[T]:
        return BoundGreaterThan(self.term, self.literal)


class EqualTo(LiteralPredicate[T]):
    def __invert__(self) -> NotEqualTo:
        return NotEqualTo(self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundEqualTo[T]]:
        return BoundEqualTo[T]


class NotEqualTo(LiteralPredicate[T]):
    def __invert__(self) -> EqualTo:
        return EqualTo(self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundNotEqualTo[T]]:
        return BoundNotEqualTo[T]


class LessThan(LiteralPredicate):
    def __invert__(self) -> GreaterThanOrEqual:
        return GreaterThanOrEqual(self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundLessThan[T]]:
        return BoundLessThan[T]


class GreaterThanOrEqual(LiteralPredicate[T]):
    def __invert__(self) -> LessThan:
        return LessThan(self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundGreaterThanOrEqual[T]]:
        return BoundGreaterThanOrEqual[T]


class GreaterThan(LiteralPredicate[T]):
    def __invert__(self) -> LessThanOrEqual:
        return LessThanOrEqual(self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundGreaterThan[T]]:
        return BoundGreaterThan[T]


class LessThanOrEqual(LiteralPredicate[T]):
    def __invert__(self) -> GreaterThan:
        return GreaterThan(self.term, self.literal)

    @property
    def as_bound(self) -> Type[BoundLessThanOrEqual[T]]:
        return BoundLessThanOrEqual[T]

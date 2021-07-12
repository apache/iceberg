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

from math import isnan
from typing import Any, List, Optional, TYPE_CHECKING, Union

from iceberg.exceptions import ValidationException

from .expression import (Expression,
                         Operation)
from .literals import (BaseLiteral,
                       Literals)
from .term import BoundTerm, UnboundTerm
from ..types import TypeID

if TYPE_CHECKING:
    from iceberg.api import StructLike


class Predicate(Expression):

    def __init__(self, op: Operation, term: Union[BoundTerm, UnboundTerm]):
        if term is None:
            raise ValueError("Term cannot be None")

        self.op: Operation = op
        self.term: Union[BoundTerm, UnboundTerm] = term

    @property
    def ref(self):
        return self.term.ref

    @property
    def lit(self):
        raise NotImplementedError("Not Implemented for base class")

    def __eq__(self, other):
        if id(self) == id(other):
            return True
        elif other is None or not isinstance(other, Predicate):
            return False

        return self.op == other.op and self.ref == other.ref and self.lit == other.lit

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "Predicate({},{},{})".format(self.op, self.ref, self.lit)

    def __str__(self):
        if self.op == Operation.IS_NULL:
            return "is_null({})".format(self.ref)
        elif self.op == Operation.NOT_NULL:
            return "not_null({})".format(self.ref)
        elif self.op == Operation.LT:
            return "less_than({})".format(self.ref)
        elif self.op == Operation.LT_EQ:
            return "less_than_equal({})".format(self.ref)
        elif self.op == Operation.GT:
            return "greater_than({})".format(self.ref)
        elif self.op == Operation.GT_EQ:
            return "greater_than_equal({})".format(self.ref)
        elif self.op == Operation.EQ:
            return "equal({})".format(self.ref)
        elif self.op == Operation.NOT_EQ:
            return "not_equal({})".format(self.ref)
        else:
            return "invalid predicate: operation = {}".format(self.op)


class BoundPredicate(Predicate):

    def __init__(self, op: Operation, term: BoundTerm, lit: BaseLiteral = None, literals: List[BaseLiteral] = None,
                 is_unary_predicate: bool = False, is_literal_predicate: bool = False,
                 is_set_predicate: bool = False):
        self.is_unary_predicate = is_unary_predicate
        self.is_literal_predicate = is_literal_predicate
        self.is_set_predicate = is_set_predicate

        super(BoundPredicate, self).__init__(op, term)
        ValidationException.check(sum([is_unary_predicate, is_literal_predicate, is_set_predicate]) == 1,
                                  "Only a single predicate type may be set: %s=%s, %s=%s, %s=%s",
                                  ("is_unary_predicate", is_unary_predicate,
                                   "is_literal_predicate", is_literal_predicate,
                                   "is_set_predicate", is_set_predicate))

        self._literals: Optional[List[BaseLiteral]] = None
        if self.is_unary_predicate:
            ValidationException.check(lit is None, "Unary Predicates may not have a literal", ())

        elif self.is_literal_predicate:
            ValidationException.check(lit is not None, "Literal Predicates must have a literal set", ())
            self._literals = [lit]  # type: ignore

        elif self.is_set_predicate:
            ValidationException.check(literals is not None, "Set Predicates must have literals set", ())
            self._literals = literals
        else:
            raise ValueError(f"Unable to instantiate {op} -> (lit={lit}, literal={literals}")

    @property
    def lit(self) -> Optional[BaseLiteral]:
        if self._literals is None or len(self._literals) == 0:
            return None
        return self._literals[0]

    def eval(self, struct: StructLike) -> bool:
        ValidationException.check(isinstance(self.term, BoundTerm), "Term must be bound to eval: %s", (self.term))
        return self.test(self.term.eval(struct))  # type: ignore

    def test(self, struct: StructLike = None, value: Any = None) -> bool:
        ValidationException.check(struct is None or value is None, "Either struct or value must be none", ())
        if struct is not None:
            ValidationException.check(isinstance(self.term, BoundTerm), "Term must be bound to eval: %s", (self.term))
            return self.test(value=self.term.eval(struct))  # type: ignore
        else:
            if self.is_unary_predicate:
                return self.test_unary_predicate(value)
            elif self.is_literal_predicate:
                return self.test_literal_predicate(value)
            else:
                return self.test_set_predicate(value)

    def test_unary_predicate(self, value: Any) -> bool:

        if self.op == Operation.IS_NULL:
            return value is None
        elif self.op == Operation.NOT_NULL:
            return value is not None
        elif self.op == Operation.IS_NAN:
            return isnan(value)
        elif self.op == Operation.NOT_NAN:
            return not isnan(value)
        else:
            raise ValueError(f"{self.op} is not a valid unary predicate")

    def test_literal_predicate(self, value: Any) -> bool:
        if self.lit is None:
            raise ValidationException("Literal must not be none", ())

        if self.op == Operation.LT:
            return value < self.lit.value
        elif self.op == Operation.LT_EQ:
            return value <= self.lit.value
        elif self.op == Operation.GT:
            return value > self.lit.value
        elif self.op == Operation.GT_EQ:
            return value >= self.lit.value
        elif self.op == Operation.EQ:
            return value == self.lit.value
        elif self.op == Operation.NOT_EQ:
            return value != self.lit.value
        else:
            raise ValueError(f"{self.op} is not a valid literal predicate")

    def test_set_predicate(self, value: Any) -> bool:
        if self._literals is None:
            raise ValidationException("Literals must not be none", ())

        if self.op == Operation.IN:
            return value in self._literals
        elif self.op == Operation.NOT_IN:
            return value not in self._literals
        else:
            raise ValueError(f"{self.op} is not a valid set predicate")


class UnboundPredicate(Predicate):

    def __init__(self, op, term, value=None, lit=None, values=None, literals=None):
        self._literals = None
        num_set_args = sum([1 for x in [value, lit, values, literals] if x is not None])

        if num_set_args > 1:
            raise ValueError(f"Only one of value={value}, lit={lit}, values={values}, literals={literals} may be set")
        super(UnboundPredicate, self).__init__(op, term)
        if isinstance(value, BaseLiteral):
            lit = value
            value = None
        if value is not None:
            self._literals = [Literals.from_(value)]
        elif lit is not None:
            self._literals = [lit]
        elif values is not None:
            self._literals = map(Literals.from_, values)
        elif literals is not None:
            self._literals = literals

    @property
    def literals(self):
        return self._literals

    @property
    def lit(self):
        if self.op in [Operation.IN, Operation.NOT_IN]:
            raise ValueError(f"{self.op} predicate cannot return a literal")

        return None if self.literals is None else self.literals[0]

    def negate(self):
        return UnboundPredicate(self.op.negate(), self.term, literals=self.literals)

    def bind(self, struct, case_sensitive=True):
        bound = self.term.bind(struct, case_sensitive=case_sensitive)

        if self.literals is None:
            return self.bind_unary_operation(bound)
        elif self.op in [Operation.IN, Operation.NOT_IN]:
            return self.bind_in_operation(bound)

        return self.bind_literal_operation(bound)

    def bind_unary_operation(self, bound_term: BoundTerm) -> BoundPredicate:
        from .expressions import Expressions
        if self.op == Operation.IS_NULL:
            if bound_term.ref.field.is_required:
                return Expressions.always_false()
            return BoundPredicate(Operation.IS_NULL, bound_term, is_unary_predicate=True)
        elif self.op == Operation.NOT_NULL:
            if bound_term.ref.field.is_required:
                return Expressions.always_true()
            return BoundPredicate(Operation.NOT_NULL, bound_term, is_unary_predicate=True)
        elif self.op in [Operation.IS_NAN, Operation.NOT_NAN]:
            if not self.floating_type(bound_term.ref.type.type_id):
                raise ValidationException(f"{self.op} cannot be used with a non-floating column", ())
            return BoundPredicate(self.op, bound_term, is_unary_predicate=True)

        raise ValidationException(f"Operation must be in [IS_NULL, NOT_NULL, IS_NAN, NOT_NAN] was:{self.op}", ())

    def bind_in_operation(self, bound_term):
        from .expressions import Expressions

        def convert_literal(lit):
            converted = lit.to(bound_term)
            ValidationException.check(converted is not None,
                                      "Invalid Value for conversion to type %s: %s (%s)",
                                      (bound_term.type, lit, lit.__class__.__name__))
            return converted

        converted_literals = filter(lambda x: x != Literals.above_max() and x != Literals.below_min(),
                                    [convert_literal(lit) for lit in self.literals])
        if len(converted_literals) == 0:
            return Expressions.always_true() if Operation.NOT_IN else Expressions.always_false()
        literal_set = set(converted_literals)
        if len(literal_set) == 1:
            if self.op == Operation.IN:
                return BoundPredicate(Operation.EQ, bound_term, literal_set[0])
            elif self.op == Operation.NOT_IN:
                return BoundPredicate(Operation.NOT_EQ, bound_term, literal_set[0])
            else:
                raise ValidationException("Operation must be in or not in", ())

        return BoundPredicate(self.op, bound_term, literals=literal_set, is_set_predicate=True)

    def bind_literal_operation(self, bound_term):
        from .expressions import Expressions

        lit = self.lit.to(bound_term.type)
        ValidationException.check(lit is not None,
                                  "Invalid Value for conversion to type %s: %s (%s)",
                                  (bound_term.type, self.lit, self.lit.__class__.__name__))

        if lit == Literals.above_max():
            if self.op in [Operation.LT, Operation.LT_EQ, Operation.NOT_EQ]:
                return Expressions.always_true()
            elif self.op in [Operation.GT, Operation.GT_EQ, Operation.EQ]:
                return Expressions.always_false()
        elif lit == Literals.below_min():
            if self.op in [Operation.LT, Operation.LT_EQ, Operation.NOT_EQ]:
                return Expressions.always_false()
            elif self.op in [Operation.GT, Operation.GT_EQ, Operation.EQ]:
                return Expressions.always_true()

        return BoundPredicate(self.op, bound_term, lit=lit, is_literal_predicate=True)

    @staticmethod
    def floating_type(type_id: TypeID) -> bool:
        return type_id in [TypeID.FLOAT, TypeID.DOUBLE]

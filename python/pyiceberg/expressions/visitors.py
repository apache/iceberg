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
from functools import singledispatch
from typing import (
    Any,
    Callable,
    Generic,
    List,
    Set,
)

from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundPredicate,
    BoundTerm,
    Not,
    Or,
    T,
    UnboundPredicate,
)
from pyiceberg.expressions.literals import Literal
from pyiceberg.manifest import ManifestFile, PartitionFieldSummary
from pyiceberg.schema import Schema
from pyiceberg.table import PartitionSpec
from pyiceberg.types import (
    DoubleType,
    FloatType,
    IcebergType,
    PrimitiveType,
)


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
            child_result (T): The result of visiting the child of the Not boolean expression
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


@visit.register
def _(_: AlwaysTrue, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an AlwaysTrue boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_true()


@visit.register
def _(_: AlwaysFalse, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an AlwaysFalse boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_false()


@visit.register
def _(obj: Not, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit a Not boolean expression with a concrete BooleanExpressionVisitor"""
    child_result: T = visit(obj.child, visitor=visitor)
    return visitor.visit_not(child_result=child_result)


@visit.register
def _(obj: And, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an And boolean expression with a concrete BooleanExpressionVisitor"""
    left_result: T = visit(obj.left, visitor=visitor)
    right_result: T = visit(obj.right, visitor=visitor)
    return visitor.visit_and(left_result=left_result, right_result=right_result)


@visit.register
def _(obj: UnboundPredicate, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an unbound boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_unbound_predicate(predicate=obj)


@visit.register
def _(obj: BoundPredicate, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit a bound boolean expression with a concrete BooleanExpressionVisitor"""
    return visitor.visit_bound_predicate(predicate=obj)


@visit.register
def _(obj: Or, visitor: BooleanExpressionVisitor[T]) -> T:
    """Visit an Or boolean expression with a concrete BooleanExpressionVisitor"""
    left_result: T = visit(obj.left, visitor=visitor)
    right_result: T = visit(obj.right, visitor=visitor)
    return visitor.visit_or(left_result=left_result, right_result=right_result)


def bind(schema: Schema, expression: BooleanExpression, case_sensitive: bool) -> BooleanExpression:
    """Travers over an expression to bind the predicates to the schema

    Args:
      schema (Schema): A schema to use when binding the expression
      expression (BooleanExpression): An expression containing UnboundPredicates that can be bound
      case_sensitive (bool): Whether to consider case when binding a reference to a field in a schema, defaults to True

    Raises:
        TypeError: In the case a predicate is already bound
    """
    return visit(expression, BindVisitor(schema, case_sensitive))


class BindVisitor(BooleanExpressionVisitor[BooleanExpression]):
    """Rewrites a boolean expression by replacing unbound references with references to fields in a struct schema

    Args:
      schema (Schema): A schema to use when binding the expression
      case_sensitive (bool): Whether to consider case when binding a reference to a field in a schema, defaults to True

    Raises:
        TypeError: In the case a predicate is already bound
    """

    schema: Schema
    case_sensitive: bool

    def __init__(self, schema: Schema, case_sensitive: bool = True) -> None:
        self.schema = schema
        self.case_sensitive = case_sensitive

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

    def visit_unbound_predicate(self, predicate: UnboundPredicate) -> BooleanExpression:
        return predicate.bind(self.schema, case_sensitive=self.case_sensitive)

    def visit_bound_predicate(self, predicate: BoundPredicate) -> BooleanExpression:
        raise TypeError(f"Found already bound predicate: {predicate}")


class BoundBooleanExpressionVisitor(BooleanExpressionVisitor[T], ABC):
    @abstractmethod
    def visit_in(self, term: BoundTerm[T], literals: Set[Literal[Any]]) -> T:
        """Visit a bound In predicate"""

    @abstractmethod
    def visit_not_in(self, term: BoundTerm[T], literals: Set[Literal[Any]]) -> T:
        """Visit a bound NotIn predicate"""

    @abstractmethod
    def visit_is_nan(self, term: BoundTerm[T]) -> T:
        """Visit a bound IsNan predicate"""

    @abstractmethod
    def visit_not_nan(self, term: BoundTerm[T]) -> T:
        """Visit a bound NotNan predicate"""

    @abstractmethod
    def visit_is_null(self, term: BoundTerm[T]) -> T:
        """Visit a bound IsNull predicate"""

    @abstractmethod
    def visit_not_null(self, term: BoundTerm[T]) -> T:
        """Visit a bound NotNull predicate"""

    @abstractmethod
    def visit_equal(self, term: BoundTerm[T], literal: Literal[Any]) -> T:
        """Visit a bound Equal predicate"""

    @abstractmethod
    def visit_not_equal(self, term: BoundTerm[T], literal: Literal[Any]) -> T:
        """Visit a bound NotEqual predicate"""

    @abstractmethod
    def visit_greater_than_or_equal(self, term: BoundTerm[T], literal: Literal[Any]) -> T:
        """Visit a bound GreaterThanOrEqual predicate"""

    @abstractmethod
    def visit_greater_than(self, term: BoundTerm[T], literal: Literal[Any]) -> T:
        """Visit a bound GreaterThan predicate"""

    @abstractmethod
    def visit_less_than(self, term: BoundTerm[T], literal: Literal[Any]) -> T:
        """Visit a bound LessThan predicate"""

    @abstractmethod
    def visit_less_than_or_equal(self, term: BoundTerm[T], literal: Literal[Any]) -> T:
        """Visit a bound LessThanOrEqual predicate"""

    @abstractmethod
    def visit_true(self) -> T:
        """Visit a bound True predicate"""

    @abstractmethod
    def visit_false(self) -> T:
        """Visit a bound False predicate"""

    @abstractmethod
    def visit_not(self, child_result: T) -> T:
        """Visit a bound Not predicate"""

    @abstractmethod
    def visit_and(self, left_result: T, right_result: T) -> T:
        """Visit a bound And predicate"""

    @abstractmethod
    def visit_or(self, left_result: T, right_result: T) -> T:
        """Visit a bound Or predicate"""

    def visit_unbound_predicate(self, predicate: UnboundPredicate[T]):
        """Visit an unbound predicate
        Args:
            predicate (UnboundPredicate[T]): An unbound predicate
        Raises:
            TypeError: This always raises since an unbound predicate is not expected in a bound boolean expression
        """
        raise TypeError(f"Not a bound predicate: {predicate}")

    def visit_bound_predicate(self, predicate: BoundPredicate[T]) -> T:
        """Visit a bound predicate
        Args:
            predicate (BoundPredicate[T]): A bound predicate
        """
        return visit_bound_predicate(predicate, self)


@singledispatch
def visit_bound_predicate(expr, _: BooleanExpressionVisitor[T]) -> T:
    raise TypeError(f"Unknown predicate: {expr}")


@visit_bound_predicate.register(BoundIn)
def _(expr: BoundIn, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_in(term=expr.term, literals=expr.literals)


@visit_bound_predicate.register(BoundNotIn)
def _(expr: BoundNotIn, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_not_in(term=expr.term, literals=expr.literals)


@visit_bound_predicate.register(BoundIsNaN)
def _(expr: BoundIsNaN, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_is_nan(term=expr.term)


@visit_bound_predicate.register(BoundNotNaN)
def _(expr: BoundNotNaN, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_not_nan(term=expr.term)


@visit_bound_predicate.register(BoundIsNull)
def _(expr: BoundIsNull, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_is_null(term=expr.term)


@visit_bound_predicate.register(BoundNotNull)
def _(expr: BoundNotNull, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_not_null(term=expr.term)


@visit_bound_predicate.register(BoundEqualTo)
def _(expr: BoundEqualTo, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_equal(term=expr.term, literal=expr.literal)


@visit_bound_predicate.register(BoundNotEqualTo)
def _(expr: BoundNotEqualTo, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_not_equal(term=expr.term, literal=expr.literal)


@visit_bound_predicate.register(BoundGreaterThanOrEqual)
def _(expr: BoundGreaterThanOrEqual, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    """Visit a bound GreaterThanOrEqual predicate"""
    return visitor.visit_greater_than_or_equal(term=expr.term, literal=expr.literal)


@visit_bound_predicate.register(BoundGreaterThan)
def _(expr: BoundGreaterThan, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_greater_than(term=expr.term, literal=expr.literal)


@visit_bound_predicate.register(BoundLessThan)
def _(expr: BoundLessThan, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_less_than(term=expr.term, literal=expr.literal)


@visit_bound_predicate.register(BoundLessThanOrEqual)
def _(expr: BoundLessThanOrEqual, visitor: BoundBooleanExpressionVisitor[T]) -> T:
    return visitor.visit_less_than_or_equal(term=expr.term, literal=expr.literal)


def rewrite_not(expr: BooleanExpression) -> BooleanExpression:
    return visit(expr, _RewriteNotVisitor())


class _RewriteNotVisitor(BooleanExpressionVisitor[BooleanExpression]):
    """Inverts the negations"""

    def visit_true(self) -> BooleanExpression:
        return AlwaysTrue()

    def visit_false(self) -> BooleanExpression:
        return AlwaysFalse()

    def visit_not(self, child_result: BooleanExpression) -> BooleanExpression:
        return ~child_result

    def visit_and(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return And(left=left_result, right=right_result)

    def visit_or(self, left_result: BooleanExpression, right_result: BooleanExpression) -> BooleanExpression:
        return Or(left=left_result, right=right_result)

    def visit_unbound_predicate(self, predicate) -> BooleanExpression:
        return predicate

    def visit_bound_predicate(self, predicate) -> BooleanExpression:
        return predicate


ROWS_MIGHT_MATCH = True
ROWS_CANNOT_MATCH = False
IN_PREDICATE_LIMIT = 200


def _from_byte_buffer(field_type: IcebergType, val: bytes):
    if not isinstance(field_type, PrimitiveType):
        raise ValueError(f"Expected a PrimitiveType, got: {type(field_type)}")
    return from_bytes(field_type, val)


class _ManifestEvalVisitor(BoundBooleanExpressionVisitor[bool]):
    partition_fields: List[PartitionFieldSummary]
    partition_filter: BooleanExpression

    def __init__(self, partition_struct_schema: Schema, partition_filter: BooleanExpression, case_sensitive: bool = True):
        self.partition_filter = bind(partition_struct_schema, rewrite_not(partition_filter), case_sensitive)

    def eval(self, manifest: ManifestFile) -> bool:
        if partitions := manifest.partitions:
            self.partition_fields = partitions
            return visit(self.partition_filter, self)

        # No partition information
        return ROWS_MIGHT_MATCH

    def visit_in(self, term: BoundTerm, literals: Set[Literal[Any]]) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.lower_bound is None:
            return ROWS_CANNOT_MATCH

        if len(literals) > IN_PREDICATE_LIMIT:
            return ROWS_MIGHT_MATCH

        lower = _from_byte_buffer(term.ref().field.field_type, field.lower_bound)

        if all(lower > val.value for val in literals):
            return ROWS_CANNOT_MATCH

        if field.upper_bound is not None:
            upper = _from_byte_buffer(term.ref().field.field_type, field.upper_bound)
            if all(upper < val.value for val in literals):
                return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_not_in(self, term: BoundTerm, literals: Set[Literal[Any]]) -> bool:
        # because the bounds are not necessarily a min or max value, this cannot be answered using
        # them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
        return ROWS_MIGHT_MATCH

    def visit_is_nan(self, term: BoundTerm) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.contains_nan is False:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_not_nan(self, term: BoundTerm) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.contains_nan is True and field.contains_null is False and field.lower_bound is None:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_is_null(self, term: BoundTerm) -> bool:
        pos = term.ref().accessor.position

        if self.partition_fields[pos].contains_null is False:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_not_null(self, term: BoundTerm) -> bool:
        pos = term.ref().accessor.position

        # contains_null encodes whether at least one partition value is null,
        # lowerBound is null if all partition values are null
        all_null = self.partition_fields[pos].contains_null is True and self.partition_fields[pos].lower_bound is None

        if all_null and type(term.ref().field.field_type) in {DoubleType, FloatType}:
            # floating point types may include NaN values, which we check separately.
            # In case bounds don't include NaN value, contains_nan needs to be checked against.
            all_null = self.partition_fields[pos].contains_nan is False

        if all_null:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_equal(self, term: BoundTerm, literal: Literal[Any]) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.lower_bound is None:
            # values are all null and literal cannot contain null
            return ROWS_CANNOT_MATCH

        lower = _from_byte_buffer(term.ref().field.field_type, field.lower_bound)

        if lower > literal.value:
            return ROWS_CANNOT_MATCH

        upper = _from_byte_buffer(term.ref().field.field_type, field.lower_bound)

        if literal.value > upper:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_not_equal(self, term: BoundTerm, literal: Literal[Any]) -> bool:
        # because the bounds are not necessarily a min or max value, this cannot be answered using
        # them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
        return ROWS_MIGHT_MATCH

    def visit_greater_than_or_equal(self, term: BoundTerm, literal: Literal[Any]) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.upper_bound is None:
            return ROWS_CANNOT_MATCH

        upper = _from_byte_buffer(term.ref().field.field_type, field.upper_bound)

        if literal.value > upper:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_greater_than(self, term: BoundTerm, literal: Literal[Any]) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.upper_bound is None:
            return ROWS_CANNOT_MATCH

        upper = _from_byte_buffer(term.ref().field.field_type, field.upper_bound)

        if literal.value >= upper:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_less_than(self, term: BoundTerm, literal: Literal[Any]) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.lower_bound is None:
            return ROWS_CANNOT_MATCH

        lower = _from_byte_buffer(term.ref().field.field_type, field.lower_bound)

        if literal.value <= lower:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_less_than_or_equal(self, term: BoundTerm, literal: Literal[Any]) -> bool:
        pos = term.ref().accessor.position
        field = self.partition_fields[pos]

        if field.lower_bound is None:
            return ROWS_CANNOT_MATCH

        lower = _from_byte_buffer(term.ref().field.field_type, field.lower_bound)

        if literal.value < lower:
            return ROWS_CANNOT_MATCH

        return ROWS_MIGHT_MATCH

    def visit_true(self) -> bool:
        return ROWS_MIGHT_MATCH

    def visit_false(self) -> bool:
        return ROWS_CANNOT_MATCH

    def visit_not(self, child_result: bool) -> bool:
        return not child_result

    def visit_and(self, left_result: bool, right_result: bool) -> bool:
        return left_result and right_result

    def visit_or(self, left_result: bool, right_result: bool) -> bool:
        return left_result or right_result


def manifest_evaluator(
    partition_spec: PartitionSpec, schema: Schema, partition_filter: BooleanExpression, case_sensitive: bool = True
) -> Callable[[ManifestFile], bool]:
    partition_type = partition_spec.partition_type(schema)
    partition_schema = Schema(*partition_type.fields)
    evaluator = _ManifestEvalVisitor(partition_schema, partition_filter, case_sensitive)
    return evaluator.eval

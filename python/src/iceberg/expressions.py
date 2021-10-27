from enum import Enum

from iceberg.exceptions import OperationError


class Operation(Enum):
    """Operations to be used as components in expressions

    Various operations can be negated or reversed. Negating an
    operation is as simple as using the built-in subtraction operator:

    >>> print(-Operation.TRUE)
    Operation.FALSE
    >>> print(-Operation.IS_NULL)
    Operation.NOT_NULL

    Reversing an operation can be done using the built-in reversed() method:
    >>> print(reversed(Operation.LT))
    Operation.GT
    >>> print(reversed(Operation.EQ))
    Operation.NOT_EQ

    Raises:
        OperationError: This is raised when attempting to negate or reverse
            an operation that cannot be negated or reversed.
    """
    TRUE = "TRUE"
    FALSE = "FALSE"
    IS_NULL = "IS_NULL"
    NOT_NULL = "NOT_NULL"
    IS_NAN = "IS_NAN"
    NOT_NAN = "NOT_NAN"
    LT = "LT"
    LT_EQ = "LT_EQ"
    GT = "GT"
    GT_EQ = "GT_EQ"
    EQ = "EQ"
    NOT_EQ = "NOT_EQ"
    IN = "IN"
    NOT_IN = "NOT_IN"
    NOT = "NOT"
    AND = "AND"
    OR = "OR"

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"Operation.{self.value}"

    def __neg__(self):
        """Returns the operation used when this is negated."""

        try:
            return {
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
            }[self]
        except KeyError:
            raise OperationError(f"No negation defined for operation {self}")

    def __reversed__(self):
        """	Returns the equivalent operation when the left and right operands are exchanged."""

        try:
            return {
                Operation.LT: Operation.GT,
                Operation.LT_EQ: Operation.GT_EQ,
                Operation.GT: Operation.LT,
                Operation.GT_EQ: Operation.LT_EQ,
                Operation.EQ: Operation.EQ,
                Operation.NOT_EQ: Operation.NOT_EQ,
                Operation.AND: Operation.AND,
                Operation.OR: Operation.OR,
            }[self]
        except KeyError:
            raise OperationError(f"No left-right flip for operation {self}")

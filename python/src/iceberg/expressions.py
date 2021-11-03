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

from enum import Enum, auto


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

    The above examples use the OPERATION_NEGATIONS and OPERATION_REVERSALS maps
    which map each enum to it's negated enum or reversed enum, respectively.

    Raises:
        ValueError: This is raised when attempting to negate or reverse
            an operation that cannot be negated or reversed.
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

    def __neg__(self):
        """Returns the operation used when this is negated."""

        try:
            return OPERATION_NEGATIONS[self]
        except KeyError:
            raise ValueError(f"No negation defined for operation {self}")

    def __reversed__(self):
        """Returns the equivalent operation when the left and right operands are exchanged."""

        try:
            return OPERATION_REVERSALS[self]
        except KeyError:
            raise ValueError(f"No left-right flip for operation {self}")


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

OPERATION_REVERSALS = {
    Operation.LT: Operation.GT,
    Operation.LT_EQ: Operation.GT_EQ,
    Operation.GT: Operation.LT,
    Operation.GT_EQ: Operation.LT_EQ,
    Operation.EQ: Operation.EQ,
    Operation.NOT_EQ: Operation.NOT_EQ,
    Operation.AND: Operation.AND,
    Operation.OR: Operation.OR,
}

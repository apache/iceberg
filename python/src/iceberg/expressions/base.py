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
from enum import Enum, auto
from typing import Any, Generic, TypeVar

from iceberg.files import StructProtocol
from iceberg.types import IcebergType

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
        except KeyError:
            raise ValueError(f"No negation defined for operation {self}")


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


class Accessor:
    """An accessor for a specific position in a container that implements the StructProtocol"""

    def __init__(self, position: int, iceberg_type: IcebergType):
        self._position = position
        self._iceberg_type = iceberg_type

    @property
    def position(self):
        """The position in the container to access"""
        return self._position

    @property
    def iceberg_type(self):
        """The IcebergType of the value at the accessor position"""
        return self._value_type

    def get(self, container: StructProtocol) -> Any:
        """Returns the value at self.position in `container`

        Args:
            container(StructProtocol): A container to access at position `self.position`

        Returns:
            Any: The value at position `self.position` in the container
        """
        return container.get(self.position)

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
from abc import abstractmethod
from decimal import Decimal
from typing import (
    Any,
    Callable,
    Dict,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    runtime_checkable,
)
from uuid import UUID


class FrozenDict(Dict[Any, Any]):
    def __setitem__(self, instance: Any, value: Any) -> None:
        raise AttributeError("FrozenDict does not support assignment")

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise AttributeError("FrozenDict does not support .update()")


EMPTY_DICT = FrozenDict()


K = TypeVar("K")
V = TypeVar("V")


# from https://stackoverflow.com/questions/2912231/is-there-a-clever-way-to-pass-the-key-to-defaultdicts-default-factory
class KeyDefaultDict(Dict[K, V]):
    def __init__(self, default_factory: Callable[[K], V]):
        super().__init__()
        self.default_factory = default_factory

    def __missing__(self, key: K) -> V:
        if self.default_factory is None:
            raise KeyError(key)
        else:
            val = self.default_factory(key)
            self[key] = val
            return val


Identifier = Tuple[str, ...]
Properties = Dict[str, str]
RecursiveDict = Dict[str, Union[str, "RecursiveDict"]]

# Represents the literal value
L = TypeVar("L", str, bool, int, float, bytes, UUID, Decimal, covariant=True)


@runtime_checkable
class StructProtocol(Protocol):  # pragma: no cover
    """A generic protocol used by accessors to get and set at positions of an object"""

    @abstractmethod
    def get(self, pos: int) -> Any:
        ...

    @abstractmethod
    def set(self, pos: int, value: Any) -> None:
        ...

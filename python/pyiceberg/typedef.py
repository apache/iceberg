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

from abc import abstractmethod
from decimal import Decimal
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
    Union,
    runtime_checkable,
)
from uuid import UUID

from pydantic import BaseModel, Extra, PrivateAttr

if TYPE_CHECKING:
    from pyiceberg.types import NestedField


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
    def __getitem__(self, pos: int) -> Any:
        ...

    @abstractmethod
    def __setitem__(self, pos: int, value: Any) -> None:
        ...


class IcebergBaseModel(BaseModel):
    """
    This class extends the Pydantic BaseModel to set default values by overriding them.

    This is because we always want to set by_alias to True. In Python, the dash can't
    be used in variable names, and this is used throughout the Iceberg spec.

    The same goes for exclude_none, if a field is None we want to omit it from
    serialization, for example, the doc attribute on the NestedField object.
    Default non-null values will be serialized.

    This is recommended by Pydantic:
    https://pydantic-docs.helpmanual.io/usage/model_config/#change-behaviour-globally
    """

    class Config:
        keep_untouched = (cached_property,)
        allow_population_by_field_name = True
        frozen = True

    def _exclude_private_properties(self, exclude: Optional[Set[str]] = None) -> Set[str]:
        # A small trick to exclude private properties. Properties are serialized by pydantic,
        # regardless if they start with an underscore.
        # This will look at the dict, and find the fields and exclude them
        return set.union(
            {field for field in self.__dict__ if field.startswith("_") and not field == "__root__"}, exclude or set()
        )

    def dict(self, exclude_none: bool = True, exclude: Optional[Set[str]] = None, **kwargs: Any) -> Dict[str, Any]:
        return super().dict(exclude_none=exclude_none, exclude=self._exclude_private_properties(exclude), **kwargs)

    def json(self, exclude_none: bool = True, exclude: Optional[Set[str]] = None, by_alias: bool = True, **kwargs: Any) -> str:
        return super().json(
            exclude_none=exclude_none, exclude=self._exclude_private_properties(exclude), by_alias=by_alias, **kwargs
        )


class PydanticStruct(IcebergBaseModel):
    class Config:
        frozen = False

    def __setitem__(self, pos: int, value: Any) -> None:
        positions = list(self.__fields__.values())
        field = positions[pos]
        if value is None:
            if field.default is not None:
                value = field.default
            elif field.default_factory is not None:
                value = field.default_factory()

        self.__setattr__(field.name, value)

    def __getitem__(self, pos: int) -> Any:
        positions = list(self.__fields__.values())
        return self.__getattribute__(positions[pos].name)


class Record(PydanticStruct):
    """A generic record with optional named attributes"""

    _data: List[Any] = PrivateAttr()
    _keywords: List[str] = PrivateAttr()

    class Config:
        # To allow dynamic fields
        extra = Extra.allow

    @staticmethod
    def of(*data: Any) -> Record:
        r = Record(length=len(data))
        for pos, d in enumerate(data):
            r[pos] = d
        return r

    @property
    def has_keywords(self) -> bool:
        # If there are no public fields, it is position based
        return len(self.__dict__) > 0

    def __init__(self, length: int = 0, fields: Optional[Tuple[NestedField, ...]] = None) -> None:
        super().__init__()
        if fields is not None:
            self._keywords = [field.name for field in fields]
            for keyword in self._keywords:
                setattr(self, keyword, None)
        else:
            self._data = [None] * length

    def __setitem__(self, pos: int, value: Any) -> None:
        if self.has_keywords:
            setattr(self, self._keywords[pos], value)
        else:
            self._data[pos] = value

    def __getitem__(self, pos: int) -> Any:
        if self.has_keywords:
            return getattr(self, self._keywords[pos])
        else:
            return self._data[pos]

    def __repr__(self) -> str:
        if self.has_keywords:
            values = (f"{field_name}={field_value}" for field_name, field_value in self.__dict__.items())
            return f"Record[{', '.join(values)}]"
        else:
            return f"Record[{', '.join(repr(v) for v in self._data)}]"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Record):
            return False

        if self.has_keywords != other.has_keywords:
            return False

        if self.has_keywords:
            return self.__dict__ == other.__dict__
        else:
            return self._data == other._data

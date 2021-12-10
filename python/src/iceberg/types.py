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

import decimal
import math
import struct
from base64 import b64encode
from datetime import date, datetime, time
from decimal import Decimal as PythonDecimal
from typing import Any, Dict
from typing import List as PythonList
from typing import Optional, Tuple, Type, Union
from uuid import UUID as PythonUUID

import mmh3
from numpy import float32, float64, isinf, isnan


# intended for use inside this module only
class IcebergMetaType(type):
    """
    MetaClass to:
        Generate a str for an `IcebergType`
        Generate a repr for an `IcebergType`
        Freeze attributes that define a generic `IcebergType`

    Note: for internal iceberg use only
    """

    _always_frozen = {"_strname", "_frozen_attrs", "_always_frozen"}
    # freeze setting of generic attributes
    def __setattr__(cls, key, value):
        if key in getattr(cls, "_frozen_attrs", set()) or key in cls._always_frozen:
            raise AttributeError(f"{key} may not be altered on type {cls}")
        type.__setattr__(cls, key, value)

    # freeze deleting of generic attributes
    def __delattr__(cls, key):
        if key in getattr(cls, "_frozen_attrs", set()) or key in cls._always_frozen:
            raise AttributeError(f"{key} may not be deleted on type {cls}")
        type.__delattr__(cls, key)

    def __getitem__(cls, key):
        return cls._get_generic(cls, key)

    def __subclasscheck__(cls, other):
        """custom subclass check for when checking `issubclass` on an IcebergType to ensure covariance on generics"""
        if cls == other:
            return True
        if generic_class.is_generic_type(
            cls, True, True
        ) and generic_class.is_generic_type(other, True, True):

            unspecified_generic = generic_class.get_unspecified_generic_type(cls)
            generic_names = [
                g[1]
                for g in generic_class.generics.values()
                if g[0] == unspecified_generic
            ][0]
            return all(
                issubclass(sv, tv)
                for sv, tv in [
                    (type.__getattribute__(other, a), type.__getattribute__(cls, a))
                    for a in generic_names
                ]
            )
        try:
            return _is_subclass(other, cls)
        except AttributeError:
            return False

    # basic repr
    def __repr__(cls):
        return cls.__name__

    def __str__(cls):
        return getattr(cls, "_strname", cls.__name__)


class IcebergType(metaclass=IcebergMetaType):
    """Base type for all Iceberg Types"""

    def __setattr__(self, key, value):
        if key in getattr(self, "_frozen_attrs", set()) or key in getattr(
            type(self), "_always_frozen", set()
        ):
            raise AttributeError(f"{key} may not be altered on isntance {self}")
        object.__setattr__(self, key, value)

    # freeze deleting of generic attributes
    def __delattr__(self, key):
        if key in getattr(self, "_frozen_attrs", set()) or key in getattr(
            type(self), "_always_frozen", set()
        ):
            raise AttributeError(f"{key} may not be deleted on instance {self}")
        object.__delattr__(self, key)

    @classmethod
    def can_cast(cls, _type: Type["IcebergType"]):
        return cls == _type


class PrimitiveType(IcebergType):
    """
    base type for primitives `IcebergType`s
    Primitives include an instance attribute `value` which is used as the underlying value to work with the type
    a `PrimitiveType` should type the instance `value` most specific to that type
    """

    value: Union[bytes, bool, int, float32, float64, PythonDecimal, str, dict]

    def __init__(self, value):

        if issubclass(type(value), PrimitiveType):
            try:
                self.value = value.to(type(self)).value
            except AttributeError:
                raise TypeError(f"Cannot convert {value} to type {type(self)}")
        else:
            self.value = value

    def __repr__(self) -> str:
        return f"{repr(type(self))}(value={self.value})"

    def __str__(self) -> str:
        return f"{str(type(self))}({self.value})"

    def __bool__(self) -> bool:
        return bool(self.value)

    def __eq__(self, other) -> bool:
        return type(other) == type(self) and self.value == other.value

    def __bytes__(self) -> bytes:
        return type(self).to_bytes(self.value)

    @classmethod
    def to_bytes(cls, value):
        return bytes(value)

    def __hash__(self) -> int:
        """https://iceberg.apache.org/#spec/#appendix-b-32-bit-hash-requirements"""
        return type(self).hash(self.value)

    @classmethod
    def hash(cls, value):
        return mmh3.hash(cls.to_bytes(value))

    def to(self, _type: Type["PrimitiveType"], coerce: bool = False):
        if type(self).can_cast(_type) or coerce:
            return _type(self.value)
        raise TypeError(f"Cannot cast {type(self)} to {_type}.")


class Number(PrimitiveType):
    """
    base `PrimitiveType` for `IcebergType`s for numeric types
    per https://iceberg.apache.org/#spec/#primitive-types these include int, long, float, double, decimal
    """

    value: Union[int, float32, float64, PythonDecimal]

    def __float__(self) -> int:
        return float(self.value)

    def __int__(self) -> float:
        return int(self.value)

    def __math(self, op, other=None):
        with decimal.localcontext() as ctx:
            if isinstance(self, Decimal):
                ctx.prec = self.precision
            op_f = getattr(self.value, op)
            try:
                if op in ("__add__", "__sub__", "__div__", "__mul__",):
                    other = other.to(type(self))
                    return type(self)(op_f(other.value))
                if op in ("__pow__", "__mod__",):
                    other = type(self)(other)
                    return type(self)(op_f(other.value))
                if op in ("__lt__", "__eq__"):
                    other = other.to(type(self))
                    return op_f(other.value)
            except TypeError:
                raise TypeError(
                    f"Cannot compare {self} with {other}. Perhaps try coercing to the appropriate type as {other}.to({type(self)}, coerce=True)."
                )
            except AttributeError:
                raise TypeError(
                    f"Cannot compare {self} with {other}. Ensure try creating an appropriate type {type(self)}({other})."
                )

            if op in ("__neg__", "__abs__"):
                return type(self)(op_f())

    def __add__(self, other: "Number") -> "Number":
        return self.__math("__add__", other)

    def __sub__(self, other: "Number") -> "Number":
        return self.__math("__sub__", other)

    def __mul__(self, other: "Number") -> "Number":
        return self.__math("__mul__", other)

    def __div__(self, other: "Number") -> "Number":
        return self.__math("__div__", other)

    def __neg__(self) -> "Number":
        return self.__math("__neg__")

    def __abs__(self) -> "Number":
        return self.__math("__abs__")

    def __pow__(self, other: "Number", mod: Optional["Number"] = None) -> "Number":
        return self.__math("__pow__", other)

    def __mod__(self, other: "Number") -> "Number":
        return self.__math("__mod__", other)

    def __lt__(self, other) -> bool:
        return self.__math("__lt__", other)

    def __eq__(self, other) -> bool:
        return self.__math("__eq__", other) and self._neg == other._neg

    def __gt__(self, other) -> bool:
        return not self.__le__(other)

    def __le__(self, other) -> bool:
        return self.__lt__(other) or self.__eq__(other)

    def __ge__(self, other) -> bool:
        return self.__gt__(other) or self.__eq__(other)

    def __hash__(self) -> int:
        return super().__hash__()


# helper for checking subclass in some generic methods
# should not be used outside of this module
def _is_subclass(cls, other):  # pragma: no cover
    try:
        return other in cls.__mro__
    except AttributeError:
        return False


class generic_class(type):
    generics: Dict[str, Tuple[Type[IcebergType], PythonList[str]]] = dict()

    def __new__(
        cls,
        name: str,
        attributes: PythonList[
            Tuple[
                str,
                Type[Union[IcebergType, "Instance"]],
                Tuple[str, Type[Union[IcebergType, "Instance"]], Any],
            ]
        ],
        base_type: type = IcebergType,
        doc: str = "",
        meta_doc: str = "",
    ):
        """facilitates generating generic type factories such as `List` e.g. `List[Integer]`
        in the spirit of builtin `type` e.g.:
            def Robot_init(self, name):
                self.name = name

            Robot2 = type("Robot2",
                          (),
                          {"counter":0,
                           "__init__": Robot_init,})

        Args:
            name: the name of the generic type used in repr and str
            attributes: list of generic attribute names and types - must provide at least a name type or both for each attribute
            base_type: class for generic to inherit from
            doc: docstring of specfied generic e.g. List[Integer]
            meta_doc: docstring of unspecified generic e.g. List

        Examples:
            >>>List = generic_class("List", [("type", IcebergType)], PrimitiveType)
            >>>List[Integer]
            List[type=Integer]
        """
        attributes = [a if len(a) == 3 else (*a, None) for a in attributes]
        attribute_names = [a[0] for a in attributes]
        attribute_types = [a[1] for a in attributes]
        attribute_defaults = dict(zip(attribute_names, [a[2] for a in attributes]))

        def get_generic(cls, attrs):
            attrs = attrs if isinstance(attrs, tuple) else (attrs,)

            if len(attrs) > len(attributes):
                raise TypeError(
                    f"""Too many generic parameters provided. Expected {len(attribute_names)} 
                    parameter(s) of subtypes of ({",".join(repr(i) for i in attribute_types)}). 
                    Provided {attrs}"""
                )

            kwargs = dict(zip(attribute_names, attrs))
            for k, v in attribute_defaults.items():
                if kwargs.get(k) is None:
                    if v is None:
                        raise TypeError(
                            f"Generic attribute `{str(k)}` on {name} requires a value and has no default."
                        )
                    else:
                        kwargs[k] = v
            for given_attribute, defined_attribute in zip(attrs, attribute_types):
                try:
                    is_instance = issubclass(defined_attribute, Instance)
                except TypeError:
                    is_instance = False
                if not is_instance:
                    defined_attribute = getattr(
                        defined_attribute, "__args__", (defined_attribute,)
                    )

                if is_instance:
                    if not issubclass(type(given_attribute), defined_attribute.type):
                        raise TypeError(
                            f"Improper types of {cls}. Given {repr(given_attribute)} instance of {type(given_attribute)}, expected instance of type {defined_attribute.type}."
                        )
                else:

                    if not any(
                        issubclass(given_attribute, t) for t in defined_attribute
                    ):
                        raise TypeError(
                            f"Improper types of {cls}. Given type {repr(given_attribute)}, expected one of {repr(defined_attribute)}."
                        )
            if attrs in cls._implemented:
                return cls._implemented[attrs]
            else:

                class _Type(
                    *(
                        (base_type, _Factory)
                        if not issubclass(_Factory, base_type)
                        else (_Factory,)
                    )
                ):
                    __doc__ = f"""Generic instance of {name} with generic attributes {kwargs}

                    {doc}
                    """

                for k, v in kwargs.items():
                    setattr(_Type, k, v)
                setattr(
                    _Type,
                    "__name__",
                    f"{name}[{', '.join(f'{k}={repr(v)}' for k,v in kwargs.items())}]",
                )
                setattr(
                    _Type, "__args__", attrs,
                )
                type.__setattr__(
                    _Type,
                    "_strname",
                    f"{name}[{', '.join(f'{str(v)}' for _, v in kwargs.items())}]",
                )
                type.__setattr__(
                    _Type,
                    "_frozen_attrs",
                    getattr(base_type, "_frozen_attrs", set()) | set(attribute_names),
                )
                setattr(
                    _Type,
                    "__annotations__",
                    dict(zip(attribute_names, attribute_types)),
                )

            cls._implemented[attrs] = _Type
            return _Type

        class _Factory(IcebergType):
            __doc__ = f"{meta_doc if meta_doc else name}"
            _implemented = dict()

        setattr(
            _Factory, "_get_generic", get_generic,
        )

        setattr(
            _Factory, "__name__", name,
        )
        type.__setattr__(_Factory, "_frozen_attrs", {"_get_generic", "_implemented"})
        cls.generics[name] = (_Factory, attribute_names)
        return _Factory

    def get_unspecified_generic_type(cls):
        """get the unspecified generic type of `cls` e.g. List for List[Integer]

        Args:
            cls: type to test
        """
        try:
            ret = [
                generic[0]
                for generic in generic_class.generics.values()
                if _is_subclass(cls, generic[0])
            ]
            return ret[0]
        except (TypeError, IndexError):
            raise TypeError(f"{cls} is not a generic nor specified generic IcebergType")

    def is_generic_type(cls, subtype: bool = False, subtype_only: bool = False):
        """Tell if `cls` is a generic IcebergType

        Args:
            cls: type to test
            subtype: test if cls is a subtype e.g. List[Integer] is a subtype of List
            subtype_only(optional): return True only if type is not fully defined e.g. List and not List[Integer]
        """
        if subtype:
            return any(
                _is_subclass(cls, generic[0])
                and (cls != generic[0] if subtype_only else True)
                for generic in generic_class.generics.values()
            )

        return cls in [generic[0] for generic in generic_class.generics.values()]


Instance = generic_class(
    "Instance",
    [("type", Union[int, float, str, bool])],
    meta_doc="""
    Instance used to represent that a generic parameter requires an instance of a primitive python type
    intended for usage with `generic_class` only

    Examples:
        >>> Fixed = generic_class("Fixed", [("length", Instance[int])], Binary)
        >>> Fixed[5]
        Fixed[length=5]
        >>> Fixed['hello']
        TypeError: Improper types of Fixed. Given 'hello' instance of <class 'str'>, expected instance of type <class 'int'>.

""",
)


class _ListBase(PrimitiveType):
    """base type for behavior of a List"""

    @classmethod
    def can_cast(cls, _type):

        return (
            issubclass(_type, List)
            and hasattr(_type, "type")
            and cls.type.can_cast(_type.type)
        )


List = generic_class(
    "List",
    [("type", IcebergType)],
    _ListBase,
    doc="""Note: see `List` for more details

Args:
    value (list): list of values of type of List e.g. Integers if List.type==Integer

""",
    meta_doc="""A list with elements of any data type: `list<E>` from https://iceberg.apache.org/#spec/#primitive-types

Args:
    type: type of elements contained in list

Attributes:
        type: type of elements contained in list

Examples:
    >>> List[Integer]
    List[type=Integer]
""",
)


class _MapBase(PrimitiveType):
    """base type for behavior of a Map"""

    @classmethod
    def can_cast(cls, _type):

        return (
            issubclass(_type, Map)
            and hasattr(_type, "key_type")
            and cls.key_type.can_cast(_type.key_type)
            and cls.value_type.can_cast(_type.value_type)
        )


Map = generic_class(
    "Map",
    [("key_type", IcebergType), ("value_type", IcebergType)],
    _MapBase,
    doc="""Note: see `Map` for more details

Args:
    value (dict): dictionary of key, value pairs matching (key_type, value_type)

""",
    meta_doc="""A map with keys and values of any data type: `Map<K, V>` from https://iceberg.apache.org/#spec/#primitive-types

Args:
    key_type: the type of the keys of the map
    value_type: the type of the values of the map

Attributes:
    key_type: the type of the keys of the map
    value_type: the type of the values of the map

Examples:
    >>> Map[String, Integer]
    Map[key_type=String, value_type=Integer]
""",
)


Fixed = generic_class(
    "Fixed",
    [("length", Instance[int])],
    Binary,
    doc="""Note: see `Fixed` for more details

Args:
    value: the value the fixed will represent

""",
    meta_doc="""Fixed-length byte array of length L: `Fixed(L)` from https://iceberg.apache.org/#spec/#primitive-types

Args:
    length (int): fixed length of the byte buffer

Attributes:
    length (int): fixed length of the byte buffer

Examples:
    >>> Fixed[8]
    Fixed[length=8]
""",
)


class _DecimalBase(Number):
    """base type for behavior of a decimal type e.g. Decimal[precision=28, scale=3]"""

    _neg: bool
    _frozen_attrs = {"_neg"}

    def __init__(self, value: Union[float, str, int]):
        with decimal.localcontext() as ctx:
            ctx.prec = self.precision
            super().__init__(value)

            if isinstance(self.value, Number):
                self.value = PythonDecimal(str(self.value.value))
            else:
                self.value = PythonDecimal(str(self.value))
            self._scale = PythonDecimal(10) ** -self.scale
            self.value = self.value.quantize(self._scale)
            object.__setattr__(self, "_neg", self.value < 0)

    @staticmethod
    def _unscale(value) -> int:
        value_tuple = value.as_tuple()
        return int(
            ("-" if value_tuple.sign else "")
            + "".join([str(d) for d in value_tuple.digits])
        )

    @classmethod
    def to_bytes(cls, value) -> bytes:
        unscaled_value = _DecimalBase._unscale(value)
        number_of_bytes = int(math.ceil(unscaled_value.bit_length() / 8))
        return unscaled_value.to_bytes(length=number_of_bytes, byteorder="big")

    @classmethod
    def can_cast(cls, _type):
        return (
            issubclass(_type, Decimal)
            and hasattr(_type, "precision")
            and _type.precision >= cls.precision
            and _type.scale == cls.scale
        )


Decimal = generic_class(
    "Decimal",
    [("precision", Instance[int]), ("scale", Instance[int])],
    _DecimalBase,
    doc="""Note: see `Decimal` for more details

Args:
    value: the value the decimal will represent

    """,
    meta_doc="""Fixed-point decimal; precision P, scale S: `decimal(P,S)` from https://iceberg.apache.org/#spec/#primitive-types

Args:
    precision (int): the number of digits in value
    scale (int): the number of digits to the right of the decimal point in value

Attributes:
    precision (int): the number of digits in value
    scale (int): the number of digits to the right of the decimal point in value

Examples:
    >>> Decimal[32, 3]
    Decimal[precision=32, scale=3]
""",
)


NestedField = generic_class(
    "NestedField",
    [
        ("type", IcebergType),
        ("optional", Instance[bool]),
        ("field_id", Instance[int]),
        ("name", Instance[str]),
        ("doc", Instance[str], ""),
    ],
    IcebergType,
    meta_doc="""equivalent of `NestedField` type from Java implementation""",
)


def _struct():  # pragma: no cover
    """
    this function serves only to encapsulate
    logic for creating a struct factory
    and is not meant to serve as a factory itself
    """

    class _StructBase(IcebergType):
        fields: tuple

        def __init__(self, *fields):
            if not len(fields) == len(self._types) or not all(
                isinstance(field, _type) for field, _type in zip(fields, self._types)
            ):
                raise TypeError(
                    f"Must provide all generic parameters of matching types. Provided {self._types}. Provided {fields}"
                )
            self.fields = fields

    def get_generic(cls, types):
        types = types if isinstance(types, tuple) else (types,)
        if types in cls._implemented:
            return cls._implemented[types]
        else:

            class _StructType(_StructBase, Struct):
                """
                A generic instance of Struct
                """

                _types = types

                def __new__(cls, *_):
                    return object.__new__(cls)

                def __repr__(self):
                    return f"{type(self)}({', '.join(repr(f) for f in self.fields)})"

            setattr(_StructType, "__annotations__", types)
            setattr(
                _StructType, "__name__", f"Struct{list(types)}",
            )
            cls._implemented[types] = _StructType
            return _StructType

    class Struct(IcebergType):
        """A record with named fields of any data type: `struct` from https://iceberg.apache.org/#schemas/

        Note: this type can infer it's own generics for convenience e.g. Struct(Integer(5), Long(6)) -> Struct[Integer, Long](Integer(value=5), Long(value=6))

        Examples:
            >>>Struct(Integer(5), Long(6))
            Struct[Integer, Long](Integer(value=5), Long(value=6))

            >>>Struct[
                NestedField[Decimal[32, 3], True, 0, "c1"],
                NestedField[Float, False, 1, "c2"],
            ]
            Struct[NestedField[type=Decimal[precision=32, scale=3], optional=True, field_id=0, name='c1', doc=''], NestedField[type=Float, optional=False, field_id=1, name='c2', doc='']]
        """

        _implemented = dict()

        def __new__(cls, *fields: IcebergType):
            """
            calling Struct will give you an instance of the generic of the appropriate type by inference
            """
            cls = cls[tuple(type(f) for f in fields)]
            return cls(*fields)

    setattr(
        Struct, "_get_generic", get_generic,
    )
    setattr(
        Struct, "__name__", "Struct",
    )
    type.__setattr__(
        Struct, "_frozen_attrs", {"_get_generic", "_implemented", "_types"}
    )

    return Struct


Struct = _struct()

generic_class.generics["Struct"] = (Struct,)

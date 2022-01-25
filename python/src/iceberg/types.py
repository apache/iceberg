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

"""Data types used in describing Iceberg schemas
This module implements the data types described in the Iceberg specification for Iceberg schemas. To
describe an Iceberg table schema, these classes can be used in the construction of a StructType instance.

Examples:
    >>>StructType[
        NestedField[Decimal[32, 3], 0, "c1", True],
        NestedField[Float, 1, "c2", False],
    ]

    StructType[field0=NestedField[type=Decimal[scale=32, precision=3], field_id=0, name='c1', optional=True, doc=''], field1=NestedField[type=Float, field_id=1, name='c2', optional=False, doc='']]

Notes:
  - https://iceberg.apache.org/#spec/#primitive-types
"""

import re
from typing import Dict, Optional, Type, Union


class IcebergMetaType(type):
    """
    MetaClass to:
        Generate a repr for an `IcebergType`
        Facilitate generating generics with __getitem__
        Covariant `issubclass`
        Check type genericism/primitivism

    Note: for internal iceberg use only
    """

    def __repr__(cls):
        return cls.__name__

    def __str__(cls):
        return re.sub("[a-zA-Z0-9_]*?=", "", cls.__name__)

    def __eq__(cls, other):
        try:
            return cls.generic == other.generic and all(
                getattr(cls, k) == getattr(other, k)
                for k in cls.generic_attributes.keys()
            )
        except AttributeError:
            return cls is other

    def __hash__(cls):
        return id(cls)

    def __subclasscheck__(
        cls, other
    ):  # custom subclass check for when checking `issubclass` on an IcebergType to ensure covariance on generics
        if cls == other:
            return True
        if cls.is_specified_generic() and other.is_specified_generic():
            for a in cls.generic_attributes.keys():
                sv, tv = getattr(other, a), getattr(cls, a)
                # check Const values
                if not isinstance(sv, type):
                    if not isinstance(tv, type):
                        if type(sv) == type(tv) and sv == tv:
                            continue
                    return False
                if not issubclass(sv, tv):
                    return False
            return True
        return cls in other.__mro__

    def __getitem__(cls, key):
        return cls._get_generic(cls, key)

    def is_specified_generic(cls):
        return bool(getattr(cls, "generic", False))

    def is_generic(cls):
        return (
            bool(getattr(cls, "_get_generic", False)) and not cls.is_specified_generic()
        )

    def is_primitive(cls):
        return PrimitiveType in cls.__mro__


class IcebergType(metaclass=IcebergMetaType):
    """Base type for all Iceberg Types"""


class PrimitiveType(IcebergType):
    """Base class for all Iceberg Primitive Types"""

    def __init__(self, value: Optional[Union[bytes, bool, int, float, str]] = None):
        self._value = value

    def __repr__(self) -> str:
        return f"{type(self).__name__}"


class NumberType(PrimitiveType):
    """Type base for all numeric types e.g. Integer, Long, Float, Double"""


class IntegralType(NumberType):
    """Type base for all integral types e.g. Integer, Long"""


class FloatingType(NumberType):
    """Type base for all floating types e.g. Float, Double"""


class BooleanType(PrimitiveType):
    """A boolean data type in Iceberg can be represented using an instance of this class."""


class IntegerType(IntegralType):
    """An Integer data type in Iceberg can be represented using an instance of this class. Integers in Iceberg are
    32-bit signed and can be promoted to Longs.

    Attributes:
        max (int): The maximum allowed value for Integers, inherited from the canonical Iceberg implementation
          in Java (returns `2147483647`)
        min (int): The minimum allowed value for Integers, inherited from the canonical Iceberg implementation
          in Java (returns `-2147483648`)
    """

    max: int = 2147483647

    min: int = -2147483648


class LongType(IntegralType):
    """A Long data type in Iceberg can be represented using an instance of this class. Longs in Iceberg are
    64-bit signed integers.

    Attributes:
        max (int): The maximum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java. (returns `9223372036854775807`)
        min (int): The minimum allowed value for Longs, inherited from the canonical Iceberg implementation
          in Java (returns `-9223372036854775808`)
    """

    max: int = 9223372036854775807

    min: int = -9223372036854775808


class FloatType(FloatingType):
    """A Float data type in Iceberg can be represented using an instance of this class. Floats in Iceberg are
    32-bit IEEE 754 floating points and can be promoted to Doubles.
    """


class DoubleType(FloatingType):
    """A Double data type in Iceberg can be represented using an instance of this class. Doubles in Iceberg are
    64-bit IEEE 754 floating points.
    """


class DateType(PrimitiveType):
    """A Date data type in Iceberg can be represented using an instance of this class. Dates in Iceberg are
    calendar dates without a timezone or time.
    """


class TimeType(PrimitiveType):
    """A Time data type in Iceberg can be represented using an instance of this class. Times in Iceberg
    have microsecond precision and are a time of day without a date or timezone.
    """


class TimestampType(PrimitiveType):
    """A Timestamp data type in Iceberg can be represented using an instance of this class. Timestamps in
    Iceberg have microsecond precision and include a date and a time of day without a timezone.
    """


class TimestamptzType(PrimitiveType):
    """A Timestamptz data type in Iceberg can be represented using an instance of this class. Timestamptzs in
    Iceberg are stored as UTC and include a date and a time of day with a timezone.
    """


class StringType(PrimitiveType):
    """A String data type in Iceberg can be represented using an instance of this class. Strings in
    Iceberg are arbitrary-length character sequences and are encoded with UTF-8.
    """


class UUIDType(PrimitiveType):
    """A UUID data type in Iceberg can be represented using an instance of this class. UUIDs in
    Iceberg are universally unique identifiers.
    """


class BinaryType(PrimitiveType):
    """A Binary data type in Iceberg can be represented using an instance of this class. Binarys in
    Iceberg are arbitrary-length byte arrays.
    """


# a factory for generic type factories
class generic_class(type):
    def __new__(
        cls,
        name: str,
        generic_attributes: Dict[
            str, Union[Type[IcebergType], Type[int], Type[str], Type[bool]]
        ],
        base_type: type = IcebergType,
        doc: str = "",
        meta_doc: str = "",
        defaults: Dict[str, Union[IcebergType, int, float, str, bool]] = {},
    ):
        """facilitates generating generic type factories such as `List` e.g. `List[Integer]`

        Args:
            name: the name of the generic type used in repr and str
            generic_attributes: dict of generic attribute names:types
            base_type: class for generic to inherit from
            doc: docstring of specfied generic type e.g. FixedType[8]
            meta_doc: docstring of unspecified generic e.g. FixedType

        Examples:
            >>>FixedType = generic_class("FixedType", {"length", Const[int])], PrimitiveType)
            >>>FixedType[8]
            FixedType[length=8]
        """

        def get_generic(cls, attrs):
            # take the attribute value specified at type definition
            attrs = attrs if isinstance(attrs, tuple) else (attrs,)
            if attrs in cls._implemented:
                return cls._implemented[attrs]

            kwargs = defaults.copy()
            kwargs = {**kwargs, **dict(zip(generic_attributes.keys(), attrs))}
            kwargs = {k: kwargs[k] for k in generic_attributes.keys()}

            # basic check of generic attributes
            if len(kwargs) != len(generic_attributes):
                raise TypeError(
                    f"""Wrong number of generic parameters provided. Expected {len(generic_attributes)}
                    parameter(s) of subtypes of ({",".join(repr(i) for i in generic_attributes.values())}).
                    Provided {attrs}"""
                )
            # specified generic type class
            class _Type(_Factory):
                __doc__ = f"""Generic instance of {name} with generic attributes {generic_attributes}

                {doc}
                """
                generic = _Factory

            # add generic attrs as class attributes
            for k, v in list(kwargs.items()) + [
                ("generic_attributes", generic_attributes)
            ]:
                setattr(_Type, k, v)

            # pretty name for repr
            setattr(
                _Type,
                "__name__",
                f"{name}[{', '.join(f'{k}={repr(v)}' for k,v in kwargs.items())}]",
            )

            # save type in _Factory so generics can equate Fixed[8]==Fixed[8]
            cls._implemented[attrs] = _Type
            return _Type

        class _Factory(base_type):  # type: ignore
            __doc__ = f"{meta_doc if meta_doc else name}"
            _implemented: Dict[Type[IcebergType], Type[IcebergType]] = dict()

        setattr(
            _Factory,
            "_get_generic",
            get_generic,
        )

        setattr(
            _Factory,
            "__name__",
            name,
        )
        return _Factory


FixedType = generic_class(
    "FixedType",
    {"length": int},
    PrimitiveType,
    doc="""A fixed data type in Iceberg.

    Example:
        >>> FixedType[8]
        FixedType[length=8]
        >>> FixedType[8]==FixedType[8]
        True
    """,
)

DecimalType = generic_class(
    "DecimalType",
    {"precision": int, "scale": int},
    PrimitiveType,
    doc="""A decimal data type in Iceberg.

    Example:
        >>> DecimalType[32, 3]
        DecimalType[precision=32, scale=3]
        >>> DecimalType[32, 3]==DecimalType[32, 3]
        True
    """,
)

NestedField = generic_class(
    "NestedField",
    {"type": IcebergType, "field_id": int, "name": str, "optional": bool, "doc": str},
    meta_doc="""equivalent of `NestedField` type from Java implementation""",
    defaults={"optional": True, "doc": ""},
)


ListType = generic_class(
    "ListType",
    {"type": NestedField},  # type: ignore
    doc="""a variable length array type in Iceberg
    Example:
        >>> ListType[NestedField[DecimalType[32, 3], 0, "c1", True]]
        ListType[type=NestedField[type=DecimalType[scale=32, precision=3], field_id=0, name='c1', optional=True, doc='']]
""",
)

MapType = generic_class(
    "MapType",
    {"key_type": NestedField, "value_type": NestedField},  # type: ignore
    doc="""a variable length set of key, value pairs type in Iceberg
    Example:
        >>> MapType[NestedField[DecimalType[32, 3], 0, "c1", True], NestedField[Float, 1, "c2", False]]
        MapType[key_type=NestedField[type=DecimalType[scale=32, precision=3], field_id=0, name='c1', optional=True, doc=''], value_type=NestedField[type=Float, field_id=1, name='c2', optional=False, doc='']]
""",
)


def _struct():
    def get_generic(cls, types):
        types = types if isinstance(types, tuple) else (types,)
        if types in cls._implemented:
            return cls._implemented[types]

        generic_attributes = {f"field{i}": f for i, f in enumerate(types)}

        class _StructType(StructType):
            """
            A generic instance of Struct
            """

            def __repr__(self):
                return f"{type(self)}({', '.join(repr(f) for f in self.fields)})"

        for k, v in list(generic_attributes.items()) + [
            ("generic_attributes", generic_attributes),
            ("generic", StructType),
            ("fields", list(generic_attributes.values())),
        ]:
            setattr(_StructType, k, v)

        setattr(
            _StructType,
            "__name__",
            f'StructType{[k+"="+repr(v) for k, v in _StructType.generic_attributes.items()]}'.replace(
                '"', ""
            ),
        )
        cls._implemented[types] = _StructType
        return _StructType

    class StructType(IcebergType):
        """A record with named fields of any data type: `struct` from https://iceberg.apache.org/#schemas/

        Examples:
            >>>StructType[
                NestedField[Decimal[32, 3], 0, "c1", True],
                NestedField[Float, 1, "c2", False],
            ]

            StructType[field0=NestedField[type=Decimal[scale=32, precision=3], field_id=0, name='c1', optional=True, doc=''], field1=NestedField[type=Float, field_id=1, name='c2', optional=False, doc='']]"""

        _implemented: Dict[Type[IcebergType], Type[IcebergType]] = dict()

    setattr(
        StructType,
        "_get_generic",
        get_generic,
    )

    return StructType


StructType = _struct()

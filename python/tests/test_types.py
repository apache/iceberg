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

import pytest

from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatingType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    NumberType,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


@pytest.mark.parametrize(
    ("input_type", "_repr"),
    [
        (BooleanType, "BooleanType"),
        (IntegerType, "IntegerType"),
        (LongType, "LongType"),
        (FloatType, "FloatType"),
        (DoubleType, "DoubleType"),
        (DateType, "DateType"),
        (TimeType, "TimeType"),
        (TimestampType, "TimestampType"),
        (TimestamptzType, "TimestamptzType"),
        (StringType, "StringType"),
        (UUIDType, "UUIDType"),
        (BinaryType, "BinaryType"),
    ],
)
def test_non_generic_primitive_repr_str(input_type, _repr):
    assert str(input_type) == repr(input_type) == _repr


def test_fixed_type():
    type_var = FixedType[5]
    assert type_var.length == 5
    assert str(type_var) == "FixedType[5]"
    assert repr(type_var) == "FixedType[length=5]"
    assert type_var == eval(str(type_var))


def test_decimal_type():
    type_var = DecimalType[9, 2]
    assert type_var.precision == 9
    assert type_var.scale == 2
    assert str(type_var) == "DecimalType[9, 2]"
    assert repr(type_var) == "DecimalType[precision=9, scale=2]"
    assert type_var == eval(str(type_var))


def test_struct_type():
    type_var = StructType[
        NestedField[IntegerType, 1, "optional_field"],
        NestedField[FixedType[5], 2, "required_field", False],
        NestedField[
            StructType[
                NestedField[DecimalType[8, 2], 4, "optional_field"],
                NestedField[LongType, 5, "required_field", False],
            ],
            3,
            "required_field",
            False,
        ],
    ]

    assert (
        repr(type_var)
        == "StructType[field0=NestedField[type=IntegerType, field_id=1, name='optional_field', optional=True, doc=''], field1=NestedField[type=FixedType[length=5], field_id=2, name='required_field', optional=False, doc=''], field2=NestedField[type=StructType[field0=NestedField[type=DecimalType[precision=8, scale=2], field_id=4, name='optional_field', optional=True, doc=''], field1=NestedField[type=LongType, field_id=5, name='required_field', optional=False, doc='']], field_id=3, name='required_field', optional=False, doc='']]"
    )
    assert (
        str(type_var)
        == "StructType[NestedField[IntegerType, 1, 'optional_field', True, ''], NestedField[FixedType[5], 2, 'required_field', False, ''], NestedField[StructType[NestedField[DecimalType[8, 2], 4, 'optional_field', True, ''], NestedField[LongType, 5, 'required_field', False, '']], 3, 'required_field', False, '']]"
    )
    assert len(type_var.fields) == 3
    assert type_var == eval(str(type_var))


def test_list_type():
    type_var = ListType[
        NestedField[
            StructType[
                NestedField[DecimalType[8, 2], 2, "optional_field"],
                NestedField[LongType, 3, "required_field", False],
            ],
            1,
            "required_field",
            False,
        ]
    ]
    assert issubclass(type_var.type.type, StructType)
    assert len(type_var.type.type.fields) == 2
    assert type_var.type.field_id == 1
    assert type_var == eval(str(type_var))


def test_map_type():
    type_var = MapType[
        NestedField[DoubleType, 1, "optional_field"],
        NestedField[UUIDType, 2, "required_field", False],
    ]
    assert type_var.key_type.type == DoubleType
    assert type_var.key_type.field_id == 1
    assert type_var.value_type.type == UUIDType
    assert type_var.value_type.field_id == 2
    assert type_var == eval(str(type_var))


def test_nested_field():
    field_var = NestedField[
        StructType[
            NestedField[
                ListType[NestedField[DoubleType, 3, "required_field3", False]],
                2,
                "optional_field2",
            ],
            NestedField[
                4,
                "required_field4",
                MapType[
                    NestedField[TimeType, 5, "optional_field5"],
                    NestedField[UUIDType, 6, "required_field6", False],
                ],
            ],
        ],
        1,
        "optional_field1",
    ]
    assert field_var.optional
    assert field_var.field_id == 1
    assert issubclass(field_var.type, StructType)
    assert field_var == eval(str(field_var))


@pytest.mark.parametrize(
    ("cls", "other", "res"),
    [
        (BooleanType, PrimitiveType, True),
        (IntegerType, PrimitiveType, True),
        (DoubleType, NumberType, True),
        (FloatType, FloatingType, True),
        (
            NestedField[DecimalType[8, 2], 4, "optional_field"],
            NestedField[DecimalType[8, 2], 4, "optional_field"],
            True,
        ),
        (
            NestedField[DecimalType[8, 1], 4, "optional_field"],
            NestedField[DecimalType[8, 2], 4, "optional_field"],
            False,
        ),
        (
            NestedField[DecimalType[8, 2], 4, "optional_field"],
            NestedField[DecimalType[8, 2], 4, "an_optional_field"],
            False,
        ),
        (
            StructType[
                NestedField[IntegerType, 1, "optional_field"],
                NestedField[FixedType[5], 2, "required_field", False],
                NestedField[
                    StructType[
                        NestedField[DecimalType[8, 2], 4, "optional_field"],
                        NestedField[LongType, 5, "required_field", False],
                    ],
                    3,
                    "required_field",
                    False,
                ],
            ],
            StructType[
                NestedField[IntegerType, 1, "optional_field"],
                NestedField[FixedType[5], 2, "required_field", False],
                NestedField[
                    StructType[
                        NestedField[DecimalType[8, 2], 4, "optional_field"],
                        NestedField[LongType, 5, "required_field", False],
                    ],
                    3,
                    "required_field",
                    False,
                ],
            ],
            True,
        ),
        (
            StructType[
                NestedField[IntegerType, 1, "optional_field"],
                NestedField[FixedType[5], 2, "required_field", False],
                NestedField[
                    StructType[
                        NestedField[DecimalType[8, 2], 4, "optional_field"],
                        NestedField[LongType, 23, "required_field", False],
                    ],
                    3,
                    "required_field",
                    False,
                ],
            ],
            StructType[
                NestedField[IntegerType, 1, "optional_field"],
                NestedField[FixedType[5], 2, "required_field", False],
                NestedField[
                    StructType[
                        NestedField[DecimalType[8, 2], 4, "optional_field"],
                        NestedField[LongType, 5, "required_field", False],
                    ],
                    3,
                    "required_field",
                    False,
                ],
            ],
            False,
        ),
    ],
)
def test_subclass(cls, other, res):
    assert issubclass(cls, other) == res

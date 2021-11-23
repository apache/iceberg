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

import pytest

from iceberg.types import (
    UUID,
    Binary,
    Boolean,
    Date,
    Decimal,
    Double,
    Fixed,
    Float,
    IcebergType,
    Integer,
    List,
    Long,
    Map,
    NestedField,
    Number,
    PrimitiveType,
    String,
    Struct,
    Time,
    Timestamp,
    Timestamptz,
    generic_class,
)


@pytest.mark.parametrize(
    "type, expected_result",
    [
        (UUID, True),
        (Number, True),
        (String, True),
        (Fixed, True),
        (Decimal, True),
        (Map, True),
        (List, True),
        (Float, True),
        (Boolean, True),
        (Double, True),
        (Long, True),
        (PrimitiveType, True),
        (IcebergType, True),
        (Integer, True),
        (Struct, True),
        (NestedField, True),
        (NestedField[Decimal[32, 3], True, 0, "c1"], True),
        (Map[String, Boolean], True),
        (Map[Integer, Float], True),
        (Map[Long, Boolean], True),
        (List[Integer], True),
        (List[String], True),
        (List[Map[Long, Boolean]], True),
        (Decimal[32, 3], True),
        (
            Struct[
                NestedField[Decimal[32, 3], True, 0, "c1"],
                NestedField[Float, False, 1, "c2"],
            ],
            True,
        ),
        (type, False),
        (object, False),
        (int, False),
        (float, False),
        (decimal.Decimal, False),
        (str, False),
        (bool, False),
    ],
)
def test_is_icebergtype(type, expected_result):
    assert issubclass(type, IcebergType) == expected_result


# https://iceberg.apache.org/#spec/#appendix-b-32-bit-hash-requirements
@pytest.mark.parametrize(
    "instance,expected",
    [
        (UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"), 1488055340),
        (Boolean(True), 1392991556),
        (Integer(34), 2017239379),
        (Long(34), 2017239379),
        (Float(1), -142385009),
        (Double(1), -142385009),
        (Decimal[9, 2]("14.20"), -500754589),
        (String("iceberg"), 1210000089),
        (Binary(b"\x00\x01\x02\x03"), -188683207),
        (Fixed[8](b"\x00\x01\x02\x03"), -188683207),
        (Date("2017-11-16"), -653330422),
        (Time(22, 31, 8), -662762989),
        (Timestamp("2017-11-16T14:31:08-08:00"), -2047944441),
        (Timestamptz("2017-11-16T14:31:08-08:00"), -2047944441),
    ],
)
def test_hashing(instance, expected):
    assert hash(instance) == expected


@pytest.mark.parametrize(
    "type, expected_result",
    [
        (UUID, "UUID"),
        (Boolean, "Boolean"),
        (Long, "Long"),
        (Double, "Double"),
        (Decimal, "Decimal"),
        (Integer, "Integer"),
        (Float, "Float"),
        (Fixed, "Fixed"),
        (Number, "Number"),
        (String, "String"),
        (Map, "Map"),
        (PrimitiveType, "PrimitiveType"),
        (List, "List"),
        (IcebergType, "IcebergType"),
    ],
)
def test_type_repr(type, expected_result):
    assert repr(type) == expected_result == str(type)


@pytest.mark.parametrize(
    "instance, expected_result",
    [
        (
            UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            "UUID(value=f79c3e09-677c-4bbd-a479-3f349cb785e7)",
        ),
        (String("hello"), "String(value=hello)"),
        (Boolean(True), "Boolean(value=True)"),
        (Boolean(False), "Boolean(value=False)"),
        (Integer(math.pi), "Integer(value=3)"),
        (Integer(1), "Integer(value=1)"),
        (Integer(1.0), "Integer(value=1)"),
        (Integer("1"), "Integer(value=1)"),
        (Long(math.pi), "Long(value=3)"),
        (Long(1), "Long(value=1)"),
        (Long(1.0), "Long(value=1)"),
        (Long("1"), "Long(value=1)"),
        (Float(math.pi), "Float(value=3.1415927410125732)"),
        (Float(1), "Float(value=1.0)"),
        (Float(1.0), "Float(value=1.0)"),
        (Float("1"), "Float(value=1.0)"),
        (Float("1.0"), "Float(value=1.0)"),
        (Double(math.pi), "Double(value=3.141592653589793)"),
        (Double(1), "Double(value=1.0)"),
        (Double(1.0), "Double(value=1.0)"),
        (Double("1"), "Double(value=1.0)"),
        (Double("1.0"), "Double(value=1.0)"),
        (Fixed[16], "Fixed[length=16]"),
        (Fixed[8], "Fixed[length=8]"),
        (
            Binary(bytes(16)),
            r"Binary(value=b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')",
        ),
        (Decimal[32, 1], "Decimal[precision=32, scale=1]"),
        (Decimal[32, 3](math.pi), "Decimal[precision=32, scale=3](value=3.142)"),
        (Decimal[32, 5](math.pi), "Decimal[precision=32, scale=5](value=3.14159)"),
        (
            Fixed[8](bytes(8)),
            r"Fixed[length=8](value=b'\x00\x00\x00\x00\x00\x00\x00\x00')",
        ),
        (Struct(Integer(5)), "Struct[Integer](Integer(value=5))"),
        (
            Struct(Integer(5), Decimal[32, 3](math.pi)),
            "Struct[Integer, Decimal[precision=32, scale=3]](Integer(value=5), Decimal[precision=32, scale=3](value=3.142))",
        ),
        (
            Struct[Integer, Decimal[32, 3]](Integer(5), Decimal[32, 3](math.pi)),
            "Struct[Integer, Decimal[precision=32, scale=3]](Integer(value=5), Decimal[precision=32, scale=3](value=3.142))",
        ),
        (
            Struct[NestedField[Decimal[32, 3], True, 0, "c1"]],
            """Struct[NestedField[type=Decimal[precision=32, scale=3], optional=True, field_id=0, name='c1', doc='']]""",
        ),
        (
            Struct[
                NestedField[Decimal[32, 3], True, 0, "c1"],
                NestedField[Float, False, 1, "c2"],
            ],
            """Struct[NestedField[type=Decimal[precision=32, scale=3], optional=True, field_id=0, name='c1', doc=''], NestedField[type=Float, optional=False, field_id=1, name='c2', doc='']]""",
        ),
        (
            NestedField[Decimal[32, 3], True, 0, "c1"],
            "NestedField[type=Decimal[precision=32, scale=3], optional=True, field_id=0, name='c1', doc='']",
        ),
        (
            Map[String, Boolean](
                {
                    String("a"): Boolean(True),
                    String("b"): Boolean(False),
                    String("c"): Boolean(True),
                    String("d"): Boolean(False),
                }
            ),
            "Map[key_type=String, value_type=Boolean](value={String(value=a): Boolean(value=True), String(value=b): Boolean(value=False), String(value=c): Boolean(value=True), String(value=d): Boolean(value=False)})",
        ),
        (
            List[Integer]([Integer(5), Integer(1), Integer(12), Integer(3.14)]),
            "List[type=Integer](value=[Integer(value=5), Integer(value=1), Integer(value=12), Integer(value=3)])",
        ),
    ],
)
def test_instance_repr(instance, expected_result):
    assert repr(instance) == expected_result


@pytest.mark.parametrize(
    "specific, unspecific",
    [
        (Decimal[32, 1], Decimal),
        (Map[String, Boolean], Map),
        (List[Integer], List),
        (Fixed[16], Fixed),
        (
            Struct[
                NestedField[Decimal[32, 3], True, 0, "c1"],
                NestedField[Float, False, 1, "c2"],
            ],
            Struct,
        ),
    ],
)
def test_specific_generic_subclass_unspecific(specific, unspecific):
    assert issubclass(specific, unspecific)


def test_integer_under_overflows():
    with pytest.raises(ValueError):
        Integer(2 ** 31)
    with pytest.raises(ValueError):
        Integer(-(2 ** 31) - 1)


def test_cannot_alter_integer_bounds():
    with pytest.raises(AttributeError):
        del Integer.max
    with pytest.raises(AttributeError):
        del Integer.min
    with pytest.raises(AttributeError):
        Integer.min = 0
    with pytest.raises(AttributeError):
        Integer.max = 0


def test_cannot_alter_long_bounds():
    with pytest.raises(AttributeError):
        del Long.max
    with pytest.raises(AttributeError):
        del Long.min
    with pytest.raises(AttributeError):
        Long.min = 0
    with pytest.raises(AttributeError):
        Long.max = 0


def test_long_not_under_overflows():
    Long(2 ** 63 - 1)
    Long(-(2 ** 63))


def test_long_under_overflows():
    with pytest.raises(ValueError):
        Long(2 ** 63)
    with pytest.raises(ValueError):
        Long(-(2 ** 63) - 1)


def test_cannot_alter_floating_neg():
    with pytest.raises(AttributeError):
        del Float(3.14)._neg
    with pytest.raises(AttributeError):
        Float(3.14)._neg = 0
    with pytest.raises(AttributeError):
        del Double(3.14)._neg
    with pytest.raises(AttributeError):
        Double(3.14)._neg = 0


@pytest.mark.parametrize(
    "_from, to, coerce",
    [
        (Integer(5), Long, False),
        (Integer(5), Double, True),
        (Integer(5), Float, True),
        (Float(3.14), Double, True),
        (Decimal[32, 3](math.pi), Decimal[33, 3], False),
    ],
)
def test_number_casting_succeeds(_from, to, coerce):
    assert _from.to(to, coerce)


@pytest.mark.parametrize(
    "_from, to",
    [
        (Integer(5), Double),
        (Integer(5), Float),
        (Long(9), Decimal[9, 2]),
        (Long(5), Double),
        (Long(5), Float),
        (Decimal[32, 3](math.pi), Decimal[31, 3]),
        (Decimal[32, 3](math.pi), Decimal[32, 2]),
    ],
)
def test_number_casting_fails(_from, to):
    with pytest.raises(TypeError):
        _from.to(to)


@pytest.mark.parametrize(
    "operation, result",
    [
        (Integer(5) <= Integer(5), True),
        (Long(5) <= Long(5), True),
        (Integer(5) < Integer(5), False),
        (Long(5) < Long(5), False),
        (Long(5) < Integer(5), False),
        (Double(5) < Float(5), False),
        (Decimal[32, 3](5) <= Decimal[32, 3](5), True),
        (Decimal[32, 3](5) < Decimal[32, 3](5), False),
        (Long(2) - Long(1), Long(1)),
        (
            Double(math.pi) == Float(math.pi),
            False,
        ),  # False is the correct value based on ieee754 representation going from 32 to 64 bits
        (Double(math.pi) ** 2, Double(math.pi ** 2)),
        (abs(Float(-6) ** Float(2)), Float(36)),
        ((Float(5) ** 4) % Float(4), Float(1)),
        (Decimal[32, 3](math.pi).to(Integer, True), Integer(3)),
    ],
)
def test_number_arithmetic(operation, result):
    assert operation == result


def test_nested_field():
    field_var = NestedField[
        List[NestedField[Double, False, 2, "required_field"]], True, 1, "optional_field"
    ]
    assert field_var.optional
    assert not field_var.type.type.optional
    assert field_var.field_id == 1
    assert field_var.type == List[NestedField[Double, False, 2, "required_field"]]


@pytest.mark.parametrize(
    "struct, struct_type",
    [
        (Struct(Double(math.pi)), Struct[Double]),
        (Struct(Decimal[32, 3](math.pi)), Struct[Decimal[32, 3]]),
        (
            Struct(
                Map[String, Boolean](
                    {
                        String("a"): Boolean(True),
                        String("b"): Boolean(False),
                        String("c"): Boolean(True),
                        String("d"): Boolean(False),
                    }
                ),
                Float(1.9),
            ),
            Struct[Map[String, Boolean], Float],
        ),
        (
            Struct(List[Boolean]([Boolean(True), Boolean(True), Boolean(False)])),
            Struct[List[Boolean]],
        ),
    ],
)
def test_struct_infer_type(struct, struct_type):
    assert type(struct) == struct_type


@pytest.mark.parametrize(
    "type1, type2, result",
    [
        (List[Integer], List[Number], True),
        (List[Integer], List, True),
        (Map[String, Integer], Map[IcebergType, IcebergType], True),
        (List[Integer], List[String], False),
        (List[Map[String, Integer]], List[IcebergType], True),
        (List[Map[String, Integer]], List[Map], True),
        (List[Map[String, Integer]], List[Map[String, Number]], True),
        (List[Map[String, Integer]], List[Map[Float, Number]], False),
        (List[Map[String, Decimal[32, 3]]], List[Map[String, Number]], True),
        (List, Map, False),
        (Double, IcebergType, True),
        (Float, Number, True),
        (Long, Fixed, False),
    ],
)
def test_issubclass(type1, type2, result):
    assert issubclass(type1, type2) == result


@pytest.mark.parametrize(
    "type, result",
    [
        (List[Integer], List),
        (Map[String, Integer], Map),
        (List[Integer], List),
        (List[Map[String, Integer]], List),
        (List[Map[String, Decimal[32, 3]]], List),
        (NestedField[Double, False, 2, "required_field"], NestedField),
        (List, List),
        (Decimal, Decimal),
        (Fixed[8], Fixed),
        (Decimal[16, 3], Decimal),
        (Double, None),
        (Float, None),
        (Long, None),
    ],
)
def test_get_unspecified_generic_type(type, result):
    if result is None:
        with pytest.raises(TypeError):
            generic_class.get_unspecified_generic_type(type)
    else:
        assert generic_class.get_unspecified_generic_type(type) == result


@pytest.mark.parametrize(
    "type_, r1, r2, r3",
    [
        (List[Integer], False, True, True),
        (Map[String, Integer], False, True, True),
        (List[Integer], False, True, True),
        (List[Map[String, Integer]], False, True, True),
        (List[Map[String, Decimal[32, 3]]], False, True, True),
        (NestedField[String, False, 2, "required_field"], False, True, True),
        (List, True, True, False),
        (Decimal, True, True, False),
        (Fixed[8], False, True, True),
        (Decimal[16, 3], False, True, True),
        (Double, False, False, False),
        (Float, False, False, False),
        (Long, False, False, False),
    ],
)
def test_is_generic_type(type_, r1, r2, r3):
    assert generic_class.is_generic_type(type_) == r1
    assert generic_class.is_generic_type(type_, True) == r2
    assert generic_class.is_generic_type(type_, True, True) == r3


@pytest.mark.parametrize(
    "order",
    [
        Float("-inf")
        < Float("-nan")
        < Float(-4e10)
        < Float(-24)
        < Float("-0")
        < Float(0)
        < Float(24)
        < Float(4e10)
        < Float("nan")
        < Float("inf"),
        Float("-inf")
        <= Float("-nan")
        <= Float(-4e10)
        <= Float(-24)
        <= Float("-0")
        <= Float(0)
        <= Float(24)
        <= Float(4e10)
        <= Float("nan")
        <= Float("inf"),
        Double("-inf")
        < Double("-nan")
        < Double(-4e10)
        < Double(-24)
        < Double("-0")
        < Double(0)
        < Double(24)
        < Double(4e10)
        < Double("nan")
        < Double("inf"),
        Double("-inf")
        <= Double("-nan")
        <= Double(-4e10)
        <= Double(-24)
        <= Double("-0")
        <= Double(0)
        <= Double(24)
        <= Double(4e10)
        <= Double("nan")
        <= Double("inf"),
        not Float("-inf")
        > Float("-nan")
        > Float(-4e10)
        > Float(-24)
        > Float("-0")
        > Float(0)
        > Float(24)
        > Float(4e10)
        > Float("nan")
        > Float("inf"),
        not Float("-inf")
        >= Float("-nan")
        >= Float(-4e10)
        >= Float(-24)
        >= Float("-0")
        >= Float(0)
        >= Float(24)
        >= Float(4e10)
        >= Float("nan")
        >= Float("inf"),
        not Double("-inf")
        > Double("-nan")
        > Double(-4e10)
        > Double(-24)
        > Double("-0")
        > Double(0)
        > Double(24)
        > Double(4e10)
        > Double("nan")
        > Double("inf"),
        not Double("-inf")
        >= Double("-nan")
        >= Double(-4e10)
        >= Double(-24)
        >= Double("-0")
        >= Double(0)
        >= Double(24)
        >= Double(4e10)
        >= Double("nan")
        >= Double("inf"),
    ],
)
def test_floating_sort_order(order):
    assert order

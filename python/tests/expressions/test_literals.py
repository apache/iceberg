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
# pylint:disable=eval-used

import datetime
import uuid
from decimal import Decimal
from typing import (
    Any,
    List,
    Set,
    Type,
)

import pytest
from typing_extensions import assert_type

from pyiceberg.expressions.literals import (
    BinaryLiteral,
    BooleanLiteral,
    DateLiteral,
    DecimalLiteral,
    DoubleLiteral,
    FixedLiteral,
    FloatAboveMax,
    FloatBelowMin,
    FloatLiteral,
    IntAboveMax,
    IntBelowMin,
    Literal,
    LongLiteral,
    StringLiteral,
    TimeLiteral,
    TimestampLiteral,
    literal,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    PrimitiveType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def test_literal_from_none_error() -> None:
    with pytest.raises(TypeError) as e:
        literal(None)  # type: ignore
    assert "Invalid literal value: None" in str(e.value)


def test_literal_from_nan_error() -> None:
    with pytest.raises(ValueError) as e:
        literal(float("nan"))
    assert "Cannot create expression literal from NaN." in str(e.value)


@pytest.mark.parametrize(
    "literal_class",
    [
        BooleanLiteral,
        LongLiteral,
        FloatLiteral,
        DoubleLiteral,
        DateLiteral,
        TimeLiteral,
        TimestampLiteral,
        DecimalLiteral,
        StringLiteral,
        FixedLiteral,
        BinaryLiteral,
    ],
)
def test_literal_classes_with_none_type_error(literal_class: Type[PrimitiveType]) -> None:
    with pytest.raises(TypeError) as e:
        literal_class(None)
    assert "Invalid literal value: None" in str(e.value)


@pytest.mark.parametrize("literal_class", [FloatLiteral, DoubleLiteral])
def test_literal_classes_with_nan_value_error(literal_class: Type[PrimitiveType]) -> None:
    with pytest.raises(ValueError) as e:
        literal_class(float("nan"))
    assert "Cannot create expression literal from NaN." in str(e.value)


# Numeric


def test_numeric_literal_comparison() -> None:
    small_lit = literal(10).to(IntegerType())
    big_lit = literal(1000).to(IntegerType())
    assert small_lit != big_lit
    assert small_lit == literal(10)
    assert small_lit < big_lit
    assert small_lit <= big_lit
    assert big_lit > small_lit
    assert big_lit >= small_lit


def test_integer_to_long_conversion() -> None:
    lit = literal(34).to(IntegerType())
    long_lit = lit.to(LongType())

    assert lit.value == long_lit.value


def test_integer_to_float_conversion() -> None:
    lit = literal(34).to(IntegerType())
    float_lit = lit.to(FloatType())

    assert lit.value == float_lit.value


def test_integer_to_double_conversion() -> None:
    lit = literal(34).to(IntegerType())
    dbl_lit = lit.to(DoubleType())

    assert lit.value == dbl_lit.value


@pytest.mark.parametrize(
    "decimal_type, decimal_value", [(DecimalType(9, 0), "34"), (DecimalType(9, 2), "34.00"), (DecimalType(9, 4), "34.0000")]
)
def test_integer_to_decimal_conversion(decimal_type: DecimalType, decimal_value: str) -> None:
    lit = literal(34).to(IntegerType())

    assert lit.to(decimal_type).value.as_tuple() == Decimal(decimal_value).as_tuple()  # type: ignore


def test_integer_to_date_conversion() -> None:
    one_day = "2022-03-28"
    date_delta = (datetime.date.fromisoformat(one_day) - datetime.date.fromisoformat("1970-01-01")).days
    date_lit = literal(date_delta).to(DateType())

    assert isinstance(date_lit, DateLiteral)
    assert date_lit.value == date_delta


def test_long_to_integer_within_bound() -> None:
    lit = literal(34).to(LongType())
    int_lit = lit.to(IntegerType())

    assert lit.value == int_lit.value


def test_long_to_integer_outside_bound() -> None:
    big_lit = literal(IntegerType.max + 1).to(LongType())
    above_max_lit = big_lit.to(IntegerType())
    assert above_max_lit == IntAboveMax()

    small_lit = literal(IntegerType.min - 1).to(LongType())
    below_min_lit = small_lit.to(IntegerType())
    assert below_min_lit == IntBelowMin()


def test_long_to_float_conversion() -> None:
    lit = literal(34).to(LongType())
    float_lit = lit.to(FloatType())

    assert lit.value == float_lit.value


def test_long_to_double_conversion() -> None:
    lit = literal(34).to(LongType())
    dbl_lit = lit.to(DoubleType())

    assert lit.value == dbl_lit.value


def test_long_to_time() -> None:
    long_lit = literal(51661919000).to(LongType())
    time_lit = long_lit.to(TimeType())

    assert isinstance(time_lit, TimeLiteral)
    assert time_lit.value == long_lit.value


def test_long_to_timestamp() -> None:
    long_lit = literal(1647305201).to(LongType())
    timestamp_lit = long_lit.to(TimestampType())

    assert timestamp_lit.value == long_lit.value


@pytest.mark.parametrize(
    "decimal_type, decimal_value", [(DecimalType(9, 0), "34"), (DecimalType(9, 2), "34.00"), (DecimalType(9, 4), "34.0000")]
)
def test_long_to_decimal_conversion(decimal_type: DecimalType, decimal_value: str) -> None:
    lit = literal(34).to(LongType())

    assert lit.to(decimal_type).value.as_tuple() == Decimal(decimal_value).as_tuple()  # type: ignore


def test_float_to_double() -> None:
    lit = literal(34.56).to(FloatType())
    dbl_lit = lit.to(DoubleType())

    assert lit.value == dbl_lit.value


@pytest.mark.parametrize(
    "decimal_type, decimal_value", [(DecimalType(9, 1), "34.6"), (DecimalType(9, 2), "34.56"), (DecimalType(9, 4), "34.5600")]
)
def test_float_to_decimal_conversion(decimal_type: DecimalType, decimal_value: str) -> None:
    lit = literal(34.56).to(FloatType())

    assert lit.to(decimal_type).value.as_tuple() == Decimal(decimal_value).as_tuple()  # type: ignore


def test_double_to_float_within_bound() -> None:
    lit = literal(34.56).to(DoubleType())
    float_lit = lit.to(FloatType())

    assert lit.value == float_lit.value


def test_double_to_float_outside_bound() -> None:
    big_lit = literal(FloatType.max + 1.0e37).to(DoubleType())
    above_max_lit = big_lit.to(FloatType())
    assert above_max_lit == FloatAboveMax()

    small_lit = literal(FloatType.min - 1.0e37).to(DoubleType())
    below_min_lit = small_lit.to(FloatType())
    assert below_min_lit == FloatBelowMin()


@pytest.mark.parametrize(
    "decimal_type, decimal_value", [(DecimalType(9, 1), "34.6"), (DecimalType(9, 2), "34.56"), (DecimalType(9, 4), "34.5600")]
)
def test_double_to_decimal_conversion(decimal_type: DecimalType, decimal_value: str) -> None:
    lit = literal(34.56).to(DoubleType())

    assert lit.to(decimal_type).value.as_tuple() == Decimal(decimal_value).as_tuple()  # type: ignore


def test_decimal_to_decimal_conversion() -> None:
    lit = literal(Decimal("34.11").quantize(Decimal(".01")))

    assert lit.value.as_tuple() == lit.to(DecimalType(9, 2)).value.as_tuple()
    assert lit.value.as_tuple() == lit.to(DecimalType(11, 2)).value.as_tuple()
    with pytest.raises(ValueError) as e:
        _ = lit.to(DecimalType(9, 0))
    assert "Could not convert 34.11 into a decimal(9, 0)" in str(e.value)
    with pytest.raises(ValueError) as e:
        _ = lit.to(DecimalType(9, 1))
    assert "Could not convert 34.11 into a decimal(9, 1)" in str(e.value)
    with pytest.raises(ValueError) as e:
        _ = lit.to(DecimalType(9, 3))
    assert "Could not convert 34.11 into a decimal(9, 3)" in str(e.value)


def test_timestamp_to_date() -> None:
    epoch_lit = TimestampLiteral(int(datetime.datetime.fromisoformat("1970-01-01T01:23:45.678").timestamp() * 1_000_000))
    date_lit = epoch_lit.to(DateType())

    assert date_lit.value == 0


def test_string_literal() -> None:
    sqrt2 = literal("1.414").to(StringType())
    pi = literal("3.141").to(StringType())
    pi_string_lit = StringLiteral("3.141")
    pi_double_lit = literal(3.141).to(DoubleType())

    assert literal("3.141").to(IntegerType()) == literal(3)
    assert literal("3.141").to(LongType()) == literal(3)

    assert sqrt2 != pi
    assert pi != pi_double_lit
    assert pi == pi_string_lit
    assert pi == pi
    assert sqrt2 < pi
    assert sqrt2 <= pi
    assert pi > sqrt2
    assert pi >= sqrt2
    assert str(pi) == "3.141"


def test_string_to_string_literal() -> None:
    assert literal("abc") == literal("abc").to(StringType())


def test_string_to_date_literal() -> None:
    one_day = "2017-08-18"
    date_lit = literal(one_day).to(DateType())

    date_delta = (datetime.date.fromisoformat(one_day) - datetime.date.fromisoformat("1970-01-01")).days
    assert date_delta == date_lit.value


def test_string_to_time_literal() -> None:
    time_str = literal("14:21:01.919")
    time_lit = time_str.to(TimeType())

    avro_val = 51661919000

    assert isinstance(time_lit, TimeLiteral)  # type: ignore
    assert avro_val == time_lit.value  # type: ignore


def test_string_to_timestamp_literal() -> None:
    timestamp_str = literal("2017-08-18T14:21:01.919234+00:00")
    timestamp = timestamp_str.to(TimestamptzType())

    avro_val = 1503066061919234
    assert avro_val == timestamp.value

    timestamp_str = literal("2017-08-18T14:21:01.919234")
    timestamp = timestamp_str.to(TimestampType())
    assert avro_val == timestamp.value

    timestamp_str = literal("2017-08-18T14:21:01.919234-07:00")
    timestamp = timestamp_str.to(TimestamptzType())
    avro_val = 1503091261919234
    assert avro_val == timestamp.value


def test_timestamp_with_zone_without_zone_in_literal() -> None:
    timestamp_str = literal("2017-08-18T14:21:01.919234")
    with pytest.raises(ValueError) as e:
        _ = timestamp_str.to(timestamp_str.to(TimestamptzType()))
    assert "Missing zone offset: 2017-08-18T14:21:01.919234 (must be ISO-8601)" in str(e.value)


def test_invalid_timestamp_in_literal() -> None:
    timestamp_str = literal("abc")
    with pytest.raises(ValueError) as e:
        _ = timestamp_str.to(timestamp_str.to(TimestamptzType()))
    assert "Invalid timestamp with zone: abc (must be ISO-8601)" in str(e.value)


def test_timestamp_without_zone_with_zone_in_literal() -> None:
    timestamp_str = literal("2017-08-18T14:21:01.919234+07:00")
    with pytest.raises(ValueError) as e:
        _ = timestamp_str.to(TimestampType())
    assert "Zone offset provided, but not expected: 2017-08-18T14:21:01.919234+07:00" in str(e.value)


def test_invalid_timestamp_with_zone_in_literal() -> None:
    timestamp_str = literal("abc")
    with pytest.raises(ValueError) as e:
        _ = timestamp_str.to(TimestampType())
    assert "Invalid timestamp without zone: abc (must be ISO-8601)" in str(e.value)


def test_string_to_uuid_literal() -> None:
    expected = uuid.uuid4()
    uuid_str = literal(str(expected))
    uuid_lit = uuid_str.to(UUIDType())

    assert expected.bytes == uuid_lit.value


def test_string_to_decimal_literal() -> None:
    decimal_str = literal("34.560")
    decimal_lit = decimal_str.to(DecimalType(9, 3))

    assert 3 == abs(decimal_lit.value.as_tuple().exponent)  # type: ignore
    assert Decimal("34.560").as_tuple() == decimal_lit.value.as_tuple()  # type: ignore


def test_string_to_boolean_literal() -> None:
    assert literal(True) == literal("true").to(BooleanType())
    assert literal(True) == literal("True").to(BooleanType())
    assert literal(False) == literal("false").to(BooleanType())
    assert literal(False) == literal("False").to(BooleanType())


def test_invalid_string_to_boolean_literal() -> None:
    invalid_boolean_str = literal("unknown")
    with pytest.raises(ValueError) as e:
        _ = invalid_boolean_str.to(BooleanType())
    assert "Could not convert unknown into a boolean" in str(e.value)


# MISC


def test_python_date_conversion() -> None:
    one_day_str = "2022-03-28"

    from_str_lit = literal(one_day_str).to(DateType())

    assert isinstance(from_str_lit, DateLiteral)  # type: ignore
    assert from_str_lit.value == 19079  # type: ignore


@pytest.mark.parametrize(
    "lit, primitive_type",
    [
        (literal(True), BooleanType()),
        (literal(34), IntegerType()),
        (literal(3400000000), LongType()),
        (literal(34.11), FloatType()),
        (literal(3.5028235e38), DoubleType()),
        (literal(Decimal(34.55).quantize(Decimal("0.01"))), DecimalType(9, 2)),
        (literal("2017-08-18"), DateType()),
        (literal("14:21:01.919"), TimeType()),
        (literal("2017-08-18T14:21:01.919"), TimestampType()),
        (literal("abc"), StringType()),
        (literal(uuid.uuid4()), UUIDType()),
        (literal(bytes([0x01, 0x02, 0x03])), FixedType(3)),
    ],
)
def test_identity_conversions(lit: Literal[Any], primitive_type: PrimitiveType) -> None:
    expected = lit.to(primitive_type)
    assert expected is expected.to(primitive_type)


def test_fixed_literal() -> None:
    fixed_lit012 = literal(bytes([0x00, 0x01, 0x02]))
    fixed_lit013 = literal(bytes([0x00, 0x01, 0x03]))
    assert fixed_lit012 == fixed_lit012
    assert fixed_lit012 != fixed_lit013
    assert fixed_lit012 < fixed_lit013
    assert fixed_lit012 <= fixed_lit013
    assert fixed_lit013 > fixed_lit012
    assert fixed_lit013 >= fixed_lit012


def test_binary_literal() -> None:
    bin_lit012 = literal(bytes([0x00, 0x01, 0x02]))
    bin_lit013 = literal(bytes([0x00, 0x01, 0x03]))
    assert bin_lit012 == bin_lit012
    assert bin_lit012 != bin_lit013
    assert bin_lit012 < bin_lit013
    assert bin_lit012 <= bin_lit013
    assert bin_lit013 > bin_lit012
    assert bin_lit013 >= bin_lit012
    # None related


def test_raise_on_comparison_to_none() -> None:
    bin_lit012 = literal(bytes([0x00, 0x01, 0x02]))
    fixed_lit012 = literal(bytes([0x00, 0x01, 0x02]))

    with pytest.raises(AttributeError):
        _ = bin_lit012 < None

    with pytest.raises(AttributeError):
        _ = bin_lit012 <= None

    with pytest.raises(AttributeError):
        _ = bin_lit012 > None

    with pytest.raises(AttributeError):
        _ = bin_lit012 >= None

    with pytest.raises(AttributeError):
        _ = fixed_lit012 < None

    with pytest.raises(AttributeError):
        _ = fixed_lit012 <= None

    with pytest.raises(AttributeError):
        _ = fixed_lit012 > None

    with pytest.raises(AttributeError):
        _ = fixed_lit012 >= None


def test_binary_to_fixed() -> None:
    lit = literal(bytes([0x00, 0x01, 0x02]))
    fixed_lit = lit.to(FixedType(3))
    assert fixed_lit is not None
    assert lit.value == fixed_lit.value

    with pytest.raises(TypeError) as e:
        _ = lit.to(FixedType(4))
    assert "Cannot convert BinaryLiteral into fixed[4], different length: 4 <> 3" in str(e.value)


def test_binary_to_smaller_fixed_none() -> None:
    lit = literal(bytes([0x00, 0x01, 0x02]))

    with pytest.raises(TypeError) as e:
        _ = lit.to(FixedType(2))
    assert "Cannot convert BinaryLiteral into fixed[2], different length: 2 <> 3" in str(e.value)


def test_binary_to_uuid() -> None:
    test_uuid = uuid.uuid4()
    lit = literal(test_uuid.bytes)
    uuid_lit = lit.to(UUIDType())
    assert uuid_lit is not None
    assert lit.value == uuid_lit.value
    assert uuid_lit.value == test_uuid.bytes


def test_incompatible_binary_to_uuid() -> None:
    lit = literal(bytes([0x00, 0x01, 0x02]))
    with pytest.raises(TypeError) as e:
        _ = lit.to(UUIDType())
        assert "Cannot convert BinaryLiteral into uuid, different length: 16 <> 3" in str(e.value)


def test_fixed_to_binary() -> None:
    lit = literal(bytes([0x00, 0x01, 0x02])).to(FixedType(3))
    binary_lit = lit.to(BinaryType())
    assert binary_lit is not None
    assert lit.value == binary_lit.value


def test_fixed_to_smaller_fixed_none() -> None:
    lit = literal(bytes([0x00, 0x01, 0x02])).to(FixedType(3))
    with pytest.raises(ValueError) as e:
        lit.to(lit.to(FixedType(2)))
    assert "Could not convert b'\\x00\\x01\\x02' into a fixed[2]" in str(e.value)


def test_fixed_to_uuid() -> None:
    test_uuid = uuid.uuid4()
    lit = literal(test_uuid.bytes).to(FixedType(16))
    uuid_lit = lit.to(UUIDType())
    assert uuid_lit is not None
    assert lit.value == uuid_lit.value
    assert uuid_lit.value == test_uuid.bytes


def test_incompatible_fixed_to_uuid() -> None:
    lit = literal(bytes([0x00, 0x01, 0x02])).to(FixedType(3))
    with pytest.raises(TypeError) as e:
        _ = lit.to(UUIDType())
        assert "Cannot convert BinaryLiteral into uuid, different length: 16 <> 3" in str(e.value)


def test_above_max_float() -> None:
    a = FloatAboveMax()
    # singleton
    assert a == FloatAboveMax()
    assert str(a) == "FloatAboveMax"
    assert repr(a) == "FloatAboveMax()"
    assert a.value == FloatType.max
    assert a == eval(repr(a))
    assert a.to(FloatType()) == FloatAboveMax()


def test_below_min_float() -> None:
    b = FloatBelowMin()
    # singleton
    assert b == FloatBelowMin()
    assert str(b) == "FloatBelowMin"
    assert repr(b) == "FloatBelowMin()"
    assert b == eval(repr(b))
    assert b.value == FloatType.min
    assert b.to(FloatType()) == FloatBelowMin()


def test_above_max_int() -> None:
    a = IntAboveMax()
    # singleton
    assert a == IntAboveMax()
    assert str(a) == "IntAboveMax"
    assert repr(a) == "IntAboveMax()"
    assert a.value == IntegerType.max
    assert a == eval(repr(a))
    assert a.to(IntegerType()) == IntAboveMax()


def test_below_min_int() -> None:
    b = IntBelowMin()
    # singleton
    assert b == IntBelowMin()
    assert str(b) == "IntBelowMin"
    assert repr(b) == "IntBelowMin()"
    assert b == eval(repr(b))
    assert b.to(IntegerType()) == IntBelowMin()


def test_invalid_boolean_conversions() -> None:
    assert_invalid_conversions(
        literal(True),
        [
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DateType(),
            TimeType(),
            TimestampType(),
            TimestamptzType(),
            DecimalType(9, 2),
            StringType(),
            UUIDType(),
            BinaryType(),
        ],
    )


def test_invalid_long_conversions() -> None:
    assert_invalid_conversions(
        literal(34).to(LongType()),
        [BooleanType(), StringType(), UUIDType(), FixedType(1), BinaryType()],
    )


@pytest.mark.parametrize(
    "lit",
    [
        literal(34.11).to(FloatType()),
        # double
        literal(34.11).to(DoubleType()),
    ],
)
@pytest.mark.parametrize(
    "test_type",
    [
        BooleanType(),
        IntegerType(),
        LongType(),
        DateType(),
        TimeType(),
        TimestampType(),
        TimestamptzType(),
        StringType(),
        UUIDType(),
        FixedType(1),
        BinaryType(),
    ],
)
def test_invalid_float_conversions(lit: Literal[Any], test_type: PrimitiveType) -> None:
    with pytest.raises(TypeError):
        _ = lit.to(test_type)


@pytest.mark.parametrize("lit", [literal("2017-08-18").to(DateType())])
@pytest.mark.parametrize(
    "test_type",
    [
        BooleanType(),
        IntegerType(),
        LongType(),
        FloatType(),
        DoubleType(),
        TimeType(),
        TimestampType(),
        TimestamptzType(),
        DecimalType(9, 2),
        StringType(),
        UUIDType(),
        FixedType(1),
        BinaryType(),
    ],
)
def test_invalid_datetime_conversions(lit: Literal[Any], test_type: PrimitiveType) -> None:
    assert_invalid_conversions(lit, [test_type])


def test_invalid_time_conversions() -> None:
    assert_invalid_conversions(
        literal("14:21:01.919").to(TimeType()),
        [
            BooleanType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DateType(),
            TimestampType(),
            TimestamptzType(),
            DecimalType(9, 2),
            StringType(),
            UUIDType(),
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_timestamp_conversions() -> None:
    assert_invalid_conversions(
        literal("2017-08-18T14:21:01.919").to(TimestampType()),
        [
            BooleanType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            TimeType(),
            DecimalType(9, 2),
            StringType(),
            UUIDType(),
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_decimal_conversion_scale() -> None:
    lit = literal(Decimal("34.11"))
    with pytest.raises(ValueError) as e:
        lit.to(DecimalType(9, 4))
    assert "Could not convert 34.11 into a decimal(9, 4)" in str(e.value)


def test_invalid_decimal_conversions() -> None:
    assert_invalid_conversions(
        literal(Decimal("34.11")),
        [
            BooleanType(),
            DateType(),
            TimeType(),
            TimestampType(),
            TimestamptzType(),
            StringType(),
            UUIDType(),
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_string_conversions() -> None:
    assert_invalid_conversions(
        literal("abc"),
        [FloatType(), DoubleType(), FixedType(1), BinaryType()],
    )


def test_invalid_uuid_conversions() -> None:
    assert_invalid_conversions(
        literal(uuid.uuid4()),
        [
            BooleanType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DateType(),
            TimeType(),
            TimestampType(),
            TimestamptzType(),
            DecimalType(9, 2),
            StringType(),
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_fixed_conversions() -> None:
    assert_invalid_conversions(
        literal(bytes([0x00, 0x01, 0x02])).to(FixedType(3)),
        [
            BooleanType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DateType(),
            TimeType(),
            TimestampType(),
            TimestamptzType(),
            DecimalType(9, 2),
            StringType(),
            UUIDType(),
        ],
    )


def test_invalid_binary_conversions() -> None:
    assert_invalid_conversions(
        literal(bytes([0x00, 0x01, 0x02])),
        [
            BooleanType(),
            IntegerType(),
            LongType(),
            FloatType(),
            DoubleType(),
            DateType(),
            TimeType(),
            TimestampType(),
            TimestamptzType(),
            DecimalType(9, 2),
            StringType(),
            UUIDType(),
        ],
    )


def assert_invalid_conversions(lit: Literal[Any], types: List[PrimitiveType]) -> None:
    for type_var in types:
        with pytest.raises(TypeError):
            _ = lit.to(type_var)


def test_compare_floats() -> None:
    lhs = literal(18.15).to(FloatType())
    rhs = literal(19.25).to(FloatType())
    assert lhs != rhs
    assert lhs < rhs
    assert lhs <= rhs
    assert not lhs > rhs
    assert not lhs >= rhs


def test_string_to_int_max_value() -> None:
    assert isinstance(literal(str(IntegerType.max + 1)).to(IntegerType()), IntAboveMax)


def test_string_to_int_min_value() -> None:
    assert isinstance(literal(str(IntegerType.min - 1)).to(IntegerType()), IntBelowMin)


def test_string_to_integer_type_invalid_value() -> None:
    with pytest.raises(ValueError) as e:
        _ = literal("abc").to(IntegerType())
    assert "Could not convert abc into a int" in str(e.value)


def test_string_to_long_type_invalid_value() -> None:
    with pytest.raises(ValueError) as e:
        _ = literal("abc").to(LongType())
    assert "Could not convert abc into a long" in str(e.value)


def test_string_to_date_type_invalid_value() -> None:
    with pytest.raises(ValueError) as e:
        _ = literal("abc").to(DateType())
    assert "Could not convert abc into a date" in str(e.value)


def test_string_to_time_type_invalid_value() -> None:
    with pytest.raises(ValueError) as e:
        _ = literal("abc").to(TimeType())
    assert "Could not convert abc into a time" in str(e.value)


def test_string_to_decimal_type_invalid_value() -> None:
    with pytest.raises(ValueError) as e:
        _ = literal("18.15").to(DecimalType(10, 0))
    assert "Could not convert 18.15 into a decimal(10, 0), scales differ 0 <> 2" in str(e.value)


def test_decimal_literal_increment() -> None:
    dec = DecimalLiteral(Decimal("10.123"))
    # Twice to check that we don't mutate the value
    assert dec.increment() == DecimalLiteral(Decimal("10.124"))
    assert dec.increment() == DecimalLiteral(Decimal("10.124"))
    # To check that the scale is still the same
    assert dec.increment().value.as_tuple() == Decimal("10.124").as_tuple()


def test_decimal_literal_dencrement() -> None:
    dec = DecimalLiteral(Decimal("10.123"))
    # Twice to check that we don't mutate the value
    assert dec.decrement() == DecimalLiteral(Decimal("10.122"))
    assert dec.decrement() == DecimalLiteral(Decimal("10.122"))
    # To check that the scale is still the same
    assert dec.decrement().value.as_tuple() == Decimal("10.122").as_tuple()


def test_uuid_literal_initialization() -> None:
    test_uuid = uuid.UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7")
    uuid_literal = literal(test_uuid)
    assert isinstance(uuid_literal, Literal)
    assert test_uuid.bytes == uuid_literal.value


#   __  __      ___
#  |  \/  |_  _| _ \_  _
#  | |\/| | || |  _/ || |
#  |_|  |_|\_, |_|  \_, |
#          |__/     |__/

assert_type(literal("str"), Literal[str])
assert_type(literal(True), Literal[bool])
assert_type(literal(123), Literal[int])
assert_type(literal(123.4), Literal[float])
assert_type(literal(bytes([0x01, 0x02, 0x03])), Literal[bytes])
assert_type(literal(Decimal("19.25")), Literal[Decimal])
assert_type({literal(1), literal(2), literal(3)}, Set[Literal[int]])

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

import pytest

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
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def test_literal_from_none_error():
    with pytest.raises(TypeError) as e:
        literal(None)
    assert "Invalid literal value: None" in str(e.value)


@pytest.mark.parametrize(
    "literalClass",
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
def test_string_literal_with_none_value_error(literalClass):
    with pytest.raises(TypeError) as e:
        literalClass(None)
    assert "Invalid literal value: None" in str(e.value)


# Numeric


def test_numeric_literal_comparison():
    small_lit = literal(10).to(IntegerType())
    big_lit = literal(1000).to(IntegerType())
    assert small_lit != big_lit
    assert small_lit == literal(10)
    assert small_lit < big_lit
    assert small_lit <= big_lit
    assert big_lit > small_lit
    assert big_lit >= small_lit


def test_integer_to_long_conversion():
    lit = literal(34).to(IntegerType())
    long_lit = lit.to(LongType())

    assert lit.value == long_lit.value


def test_integer_to_float_conversion():
    lit = literal(34).to(IntegerType())
    float_lit = lit.to(FloatType())

    assert lit.value == float_lit.value


def test_integer_to_double_conversion():
    lit = literal(34).to(IntegerType())
    dbl_lit = lit.to(DoubleType())

    assert lit.value == dbl_lit.value


@pytest.mark.parametrize(
    "decimalType, decimalValue", [(DecimalType(9, 0), "34"), (DecimalType(9, 2), "34.00"), (DecimalType(9, 4), "34.0000")]
)
def test_integer_to_decimal_conversion(decimalType, decimalValue):
    lit = literal(34).to(IntegerType())

    assert lit.to(decimalType).value.as_tuple() == Decimal(decimalValue).as_tuple()


def test_date_to_integer_conversion():
    one_day = "2022-03-28"
    date_delta = (datetime.date.fromisoformat(one_day) - datetime.date.fromisoformat("1970-01-01")).days
    int_lit = literal(date_delta).to(IntegerType())

    assert isinstance(int_lit, LongLiteral)
    assert int_lit.value == date_delta


def test_long_to_integer_within_bound():
    lit = literal(34).to(LongType())
    int_lit = lit.to(IntegerType())

    assert lit.value == int_lit.value


def test_long_to_integer_outside_bound():
    big_lit = literal(IntegerType.max + 1).to(LongType())
    above_max_lit = big_lit.to(IntegerType())
    assert above_max_lit == IntAboveMax()

    small_lit = literal(IntegerType.min - 1).to(LongType())
    below_min_lit = small_lit.to(IntegerType())
    assert below_min_lit == IntBelowMin()


def test_long_to_float_conversion():
    lit = literal(34).to(LongType())
    float_lit = lit.to(FloatType())

    assert lit.value == float_lit.value


def test_long_to_double_conversion():
    lit = literal(34).to(LongType())
    dbl_lit = lit.to(DoubleType())

    assert lit.value == dbl_lit.value


def test_long_to_time():
    long_lit = literal(51661919000).to(LongType())
    time_lit = long_lit.to(TimeType())

    assert isinstance(time_lit, TimeLiteral)
    assert time_lit.value == long_lit.value


def test_long_to_timestamp():
    long_lit = literal(1647305201).to(LongType())
    timestamp_lit = long_lit.to(TimestampType())

    assert timestamp_lit.value == long_lit.value


@pytest.mark.parametrize(
    "decimalType, decimalValue", [(DecimalType(9, 0), "34"), (DecimalType(9, 2), "34.00"), (DecimalType(9, 4), "34.0000")]
)
def test_long_to_decimal_conversion(decimalType, decimalValue):
    lit = literal(34).to(LongType())

    assert lit.to(decimalType).value.as_tuple() == Decimal(decimalValue).as_tuple()


def test_float_to_double():
    lit = literal(34.56).to(FloatType())
    dbl_lit = lit.to(DoubleType())

    assert lit.value == dbl_lit.value


@pytest.mark.parametrize(
    "decimalType, decimalValue", [(DecimalType(9, 1), "34.6"), (DecimalType(9, 2), "34.56"), (DecimalType(9, 4), "34.5600")]
)
def test_float_to_decimal_conversion(decimalType, decimalValue):
    lit = literal(34.56).to(FloatType())

    assert lit.to(decimalType).value.as_tuple() == Decimal(decimalValue).as_tuple()


def test_double_to_float_within_bound():
    lit = literal(34.56).to(DoubleType())
    float_lit = lit.to(FloatType())

    assert lit.value == float_lit.value


def test_double_to_float_outside_bound():
    big_lit = literal(FloatType.max + 1.0e37).to(DoubleType())
    above_max_lit = big_lit.to(FloatType())
    assert above_max_lit == FloatAboveMax()

    small_lit = literal(FloatType.min - 1.0e37).to(DoubleType())
    below_min_lit = small_lit.to(FloatType())
    assert below_min_lit == FloatBelowMin()


@pytest.mark.parametrize(
    "decimalType, decimalValue", [(DecimalType(9, 1), "34.6"), (DecimalType(9, 2), "34.56"), (DecimalType(9, 4), "34.5600")]
)
def test_double_to_decimal_conversion(decimalType, decimalValue):
    lit = literal(34.56).to(DoubleType())

    assert lit.to(decimalType).value.as_tuple() == Decimal(decimalValue).as_tuple()


def test_decimal_to_decimal_conversion():
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


def test_timestamp_to_date():
    epoch_lit = TimestampLiteral(int(datetime.datetime.fromisoformat("1970-01-01T01:23:45.678").timestamp() * 1_000_000))
    date_lit = epoch_lit.to(DateType())

    assert date_lit.value == datetime.date(1970, 1, 1)


def test_string_literal():
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


def test_string_to_string_literal():
    assert literal("abc") == literal("abc").to(StringType())


def test_string_to_date_literal():
    one_day = "2017-08-18"
    date_lit = literal(one_day).to(DateType()).to(IntegerType())

    date_delta = (datetime.date.fromisoformat(one_day) - datetime.date.fromisoformat("1970-01-01")).days
    assert date_delta == date_lit.value


def test_string_to_time_literal():
    time_str = literal("14:21:01.919")
    time_lit = time_str.to(TimeType())

    avro_val = 51661919000

    assert isinstance(time_lit, TimeLiteral)
    assert avro_val == time_lit.value


def test_string_to_timestamp_literal():
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


def test_timestamp_with_zone_without_zone_in_literal():
    timestamp_str = literal("2017-08-18T14:21:01.919234")
    with pytest.raises(ValueError) as e:
        _ = timestamp_str.to(timestamp_str.to(TimestamptzType()))
    assert "Invalid timestamp with zone: 2017-08-18T14:21:01.919234 (must be ISO-8601)" in str(e.value)


def test_timestamp_without_zone_with_zone_in_literal():
    timestamp_str = literal("2017-08-18T14:21:01.919234+07:00")
    with pytest.raises(ValueError) as e:
        _ = timestamp_str.to(TimestampType())
    assert "Could not convert 2017-08-18T14:21:01.919234+07:00 into a timestamp" in str(e.value)


def test_string_to_uuid_literal():
    expected = uuid.uuid4()
    uuid_str = literal(str(expected))
    uuid_lit = uuid_str.to(UUIDType())

    assert expected == uuid_lit.value


def test_string_to_decimal_literal():
    decimal_str = literal("34.560")
    decimal_lit = decimal_str.to(DecimalType(9, 3))

    assert 3 == abs(decimal_lit.value.as_tuple().exponent)
    assert Decimal("34.560").as_tuple() == decimal_lit.value.as_tuple()


# MISC


def test_python_date_conversion():
    one_day_str = "2022-03-28"
    one_day_date = datetime.date(2022, 3, 28)

    from_str_lit = literal(one_day_str).to(DateType())
    from_date_lit = literal(one_day_date)

    assert isinstance(from_date_lit, DateLiteral)
    assert from_str_lit == from_date_lit


@pytest.mark.parametrize(
    "lit, primitive_type",
    [
        (literal(True), BooleanType()),
        (literal(34), IntegerType()),
        (literal(3400000000), LongType()),
        (literal(34.11), FloatType()),
        (literal(3.5028235e38), DoubleType()),
        (literal(Decimal(34.55).quantize(Decimal("0.01"))), DecimalType(9, 2)),
        (literal(datetime.date(2017, 8, 18)), DateType()),
        (literal("2017-08-18"), DateType()),
        (literal("14:21:01.919"), TimeType()),
        (literal("2017-08-18T14:21:01.919"), TimestampType()),
        (literal("abc"), StringType()),
        (literal(uuid.uuid4()), UUIDType()),
        (literal(bytes([0x01, 0x02, 0x03])), FixedType(3)),
        (literal(bytes([0x03, 0x04, 0x05, 0x06])), BinaryType()),
    ],
)
def test_identity_conversions(lit, primitive_type):
    expected = lit.to(primitive_type)
    assert expected is expected.to(primitive_type)


def test_fixed_literal():
    fixed_lit012 = literal(bytes([0x00, 0x01, 0x02]))
    fixed_lit013 = literal(bytes([0x00, 0x01, 0x03]))
    assert fixed_lit012 == fixed_lit012
    assert fixed_lit012 != fixed_lit013
    assert fixed_lit012 < fixed_lit013
    assert fixed_lit012 <= fixed_lit013
    assert fixed_lit013 > fixed_lit012
    assert fixed_lit013 >= fixed_lit012


def test_binary_literal():
    bin_lit012 = literal(bytes([0x00, 0x01, 0x02]))
    bin_lit013 = literal(bytes([0x00, 0x01, 0x03]))
    assert bin_lit012 == bin_lit012
    assert bin_lit012 != bin_lit013
    assert bin_lit012 < bin_lit013
    assert bin_lit012 <= bin_lit013
    assert bin_lit013 > bin_lit012
    assert bin_lit013 >= bin_lit012
    # None related


def test_raise_on_comparison_to_none():
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


def test_binary_to_fixed():
    lit = literal(bytes([0x00, 0x01, 0x02]))
    fixed_lit = lit.to(FixedType(3))
    assert fixed_lit is not None
    assert lit.value == fixed_lit.value

    with pytest.raises(TypeError) as e:
        _ = lit.to(FixedType(4))
    assert "Cannot convert BinaryLiteral into fixed[4], different length: 4 <> 3" in str(e.value)


def test_binary_to_smaller_fixed_none():
    lit = literal(bytes([0x00, 0x01, 0x02]))

    with pytest.raises(TypeError) as e:
        _ = lit.to(FixedType(2))
    assert "Cannot convert BinaryLiteral into fixed[2], different length: 2 <> 3" in str(e.value)


def test_fixed_to_binary():
    lit = literal(bytes([0x00, 0x01, 0x02])).to(FixedType(3))
    binary_lit = lit.to(BinaryType())
    assert binary_lit is not None
    assert lit.value == binary_lit.value


def test_fixed_to_smaller_fixed_none():
    lit = literal(bytes([0x00, 0x01, 0x02])).to(FixedType(3))
    with pytest.raises(ValueError) as e:
        lit.to(lit.to(FixedType(2)))
    assert "Could not convert b'\\x00\\x01\\x02' into a fixed[2]" in str(e.value)


def test_above_max_float():
    a = FloatAboveMax()
    # singleton
    assert a == FloatAboveMax()
    assert str(a) == "FloatAboveMax"
    assert repr(a) == "FloatAboveMax()"
    assert a.value == FloatType.max
    assert a == eval(repr(a))
    with pytest.raises(TypeError) as e:
        a.to(IntegerType())
    assert "Cannot change the type of FloatAboveMax" in str(e.value)


def test_below_min_float():
    b = FloatBelowMin()
    # singleton
    assert b == FloatBelowMin()
    assert str(b) == "FloatBelowMin"
    assert repr(b) == "FloatBelowMin()"
    assert b == eval(repr(b))
    assert b.value == FloatType.min
    with pytest.raises(TypeError) as e:
        b.to(IntegerType())
    assert "Cannot change the type of FloatBelowMin" in str(e.value)


def test_above_max_int():
    a = IntAboveMax()
    # singleton
    assert a == IntAboveMax()
    assert str(a) == "IntAboveMax"
    assert repr(a) == "IntAboveMax()"
    assert a.value == IntegerType.max
    assert a == eval(repr(a))
    with pytest.raises(TypeError) as e:
        a.to(IntegerType())
    assert "Cannot change the type of IntAboveMax" in str(e.value)


def test_below_min_int():
    b = IntBelowMin()
    # singleton
    assert b == IntBelowMin()
    assert str(b) == "IntBelowMin"
    assert repr(b) == "IntBelowMin()"
    assert b == eval(repr(b))
    assert b.value == IntegerType.min
    with pytest.raises(TypeError) as e:
        b.to(IntegerType())
    assert "Cannot change the type of IntBelowMin" in str(e.value)


def test_invalid_boolean_conversions():
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


def test_invalid_long_conversions():
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
def test_invalid_float_conversions(lit, test_type):
    with pytest.raises(TypeError):
        _ = lit.to(test_type)


@pytest.mark.parametrize("lit", [literal("2017-08-18").to(DateType())])
@pytest.mark.parametrize(
    "test_type",
    [
        BooleanType(),
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
def test_invalid_datetime_conversions(lit, test_type):
    assert_invalid_conversions(lit, (test_type,))


def test_invalid_time_conversions():
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


def test_invalid_timestamp_conversions():
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


def test_invalid_decimal_conversion_scale():
    lit = literal(Decimal("34.11"))
    with pytest.raises(ValueError) as e:
        lit.to(DecimalType(9, 4))
    assert "Could not convert 34.11 into a decimal(9, 4)" in str(e.value)


def test_invalid_decimal_conversions():
    assert_invalid_conversions(
        literal(Decimal("34.11")),
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
            StringType(),
            UUIDType(),
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_string_conversions():
    assert_invalid_conversions(
        literal("abc"),
        [BooleanType(), FloatType(), DoubleType(), FixedType(1), BinaryType()],
    )


def test_invalid_uuid_conversions():
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


def test_invalid_fixed_conversions():
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


def test_invalid_binary_conversions():
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


def assert_invalid_conversions(lit, types=None):
    for type_var in types:
        with pytest.raises(TypeError):
            _ = lit.to(type_var)

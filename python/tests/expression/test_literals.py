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

import math
import uuid
from datetime import datetime
from decimal import Decimal

import dateutil.parser
import pytest
from fastavro.write import LOGICAL_WRITERS
from pytest import raises

from iceberg.expression.literals import (
    EPOCH,
    AboveMax,
    BaseLiteral,
    BelowMin,
    ComparableLiteral,
    DateLiteral,
    Literal,
    StringLiteral,
    TimestampLiteral,
    literal,
)
from iceberg.types import (
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

# Base


def test_literal_from_none_error():
    with pytest.raises(TypeError) as e:
        literal(None)
    assert "Unimplemented Type Literal for value" in str(e.value)


def test_string_literal_with_none_value_error():
    with pytest.raises(TypeError) as e:
        StringLiteral(None)
    assert "Cannot set value of BaseLiteral to None" in str(e.value)


@pytest.mark.parametrize(
    "clazz",
    [
        Literal(),
        BaseLiteral("baseLiteral", 123),
        ComparableLiteral("comparableLiteral", 456),
    ],
)
def test_base_class_not_implemented_error(clazz):
    with pytest.raises(NotImplementedError):
        clazz.to(IntegerType())


def test_base_literal():
    b = BaseLiteral("baseLiteral", 3.14)
    assert repr(b) == "baseLiteral"
    assert str(b) == "3.14"
    assert b.value == 3.14


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
    lit = literal(34)
    long_lit = lit.to(LongType())

    assert lit.value == long_lit.value


def test_integer_to_float_conversion():
    lit = literal(34)
    float_lit = lit.to(FloatType())

    assert math.isclose(lit.value, float_lit.value)


def test_integer_to_double_conversion():
    lit = literal(34)
    dbl_lit = lit.to(DoubleType())

    assert math.isclose(lit.value, dbl_lit.value)


@pytest.mark.parametrize(
    "decimalType, decimalValue", [(DecimalType(9, 0), "34"), (DecimalType(9, 2), "34.00"), (DecimalType(9, 4), "34.0000")]
)
def test_integer_to_decimal_conversion(decimalType, decimalValue):
    lit = literal(34)

    assert lit.to(decimalType).value.as_tuple() == Decimal(decimalValue).as_tuple()


def test_integer_to_date_conversion():
    lit = literal(20220303)
    dbl_lit = lit.to(DateType())

    assert isinstance(dbl_lit, DateLiteral)


def test_long_to_integer_within_bound():
    lit = literal(34).to(LongType())
    int_lit = lit.to(IntegerType())

    assert lit.value == int_lit.value


def test_long_to_integer_outside_bound():
    big_lit = literal(IntegerType.max + 1).to(LongType())
    above_max_lit = big_lit.to(IntegerType())
    assert above_max_lit == AboveMax()

    small_lit = literal(IntegerType.min - 1).to(LongType())
    below_min_lit = small_lit.to(IntegerType())
    assert below_min_lit == BelowMin()


def test_long_to_float_conversion():
    lit = literal(34).to(LongType())
    float_lit = lit.to(FloatType())

    assert math.isclose(lit.value, float_lit.value)


def test_long_to_double_conversion():
    lit = literal(34).to(LongType())
    dbl_lit = lit.to(DoubleType())

    assert math.isclose(lit.value, dbl_lit.value)


def test_long_to_time():
    long_lit = literal(51661919000).to(LongType())
    time_lit = long_lit.to(TimeType())

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
    lit = literal(34.56)
    dbl_lit = lit.to(DoubleType())

    assert math.isclose(lit.value, dbl_lit.value)


@pytest.mark.parametrize(
    "decimalType, decimalValue", [(DecimalType(9, 1), "34.6"), (DecimalType(9, 2), "34.56"), (DecimalType(9, 4), "34.5600")]
)
def test_float_to_decimal_conversion(decimalType, decimalValue):
    lit = literal(34.56)

    assert lit.to(decimalType).value.as_tuple() == Decimal(decimalValue).as_tuple()


def test_double_to_float_within_bound():
    lit = literal(34.56).to(DoubleType())
    float_lit = lit.to(FloatType())

    assert math.isclose(lit.value, float_lit.value)


def test_double_to_float_outside_bound():
    big_lit = literal(FloatType.max + 1.0e37).to(DoubleType())
    above_max_lit = big_lit.to(FloatType())
    assert above_max_lit == AboveMax()

    small_lit = literal(FloatType.min - 1.0e37).to(DoubleType())
    below_min_lit = small_lit.to(FloatType())
    assert below_min_lit == BelowMin()


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
    assert lit.to(DecimalType(9, 0)) is None
    assert lit.to(DecimalType(9, 1)) is None
    assert lit.to(DecimalType(9, 3)) is None


def test_timestamp_to_date():
    epoch_lit = TimestampLiteral(EPOCH.timestamp() * 1000_000)
    date_lit = epoch_lit.to(DateType())

    assert date_lit.value == 0


# STRING


def test_string_literal():
    sqrt2 = literal("1.414").to(StringType())
    phi = literal("3.141").to(StringType())
    phi_string_lit = StringLiteral("3.141")
    phi_double_lit = literal(3.141).to(DoubleType())

    assert sqrt2 != phi
    assert phi != phi_double_lit
    assert phi == phi_string_lit
    assert phi == phi
    assert sqrt2 < phi
    assert sqrt2 <= phi
    assert phi > sqrt2
    assert phi >= sqrt2
    assert str(phi) == '"3.141"'


def test_string_to_string_literal():
    assert literal("abc") == literal("abc").to(StringType())


def test_string_to_date_literal():
    date_str = literal("2017-08-18")
    date = date_str.to(DateType())

    avro_val = LOGICAL_WRITERS["int-date"](datetime.strptime("2017-08-18", "%Y-%m-%d"), None)
    assert avro_val == date.value


def test_string_to_time_literal():
    time_str = literal("14:21:01.919")
    time_lit = time_str.to(TimeType())

    avro_val = LOGICAL_WRITERS["long-time-micros"](datetime.strptime("14:21:01.919", "%H:%M:%S.%f").time(), None)

    assert avro_val == time_lit.value


def test_string_to_timestamp_literal():
    timestamp_str = literal("2017-08-18T14:21:01.919+00:00")
    timestamp = timestamp_str.to(TimestamptzType())

    avro_val = LOGICAL_WRITERS["long-timestamp-micros"](dateutil.parser.parse("2017-08-18T14:21:01.919+00:00"), None)
    assert avro_val == timestamp.value

    timestamp_str = literal("2017-08-18T14:21:01.919")
    timestamp = timestamp_str.to(TimestampType())
    assert avro_val == timestamp.value

    timestamp_str = literal("2017-08-18T14:21:01.919-07:00")
    timestamp = timestamp_str.to(TimestamptzType())
    avro_val = LOGICAL_WRITERS["long-timestamp-micros"](dateutil.parser.parse("2017-08-18T21:21:01.919+00:00"), None)
    assert avro_val == timestamp.value


def test_timestamp_with_zone_without_zone_in_literal():
    with raises(RuntimeError):
        timestamp_str = literal("2017-08-18T14:21:01.919")
        timestamp_str.to(TimestamptzType())


def test_timestamp_without_zone_with_zone_in_literal():
    with raises(RuntimeError):
        timestamp_str = literal("2017-08-18T14:21:01.919+07:00")
        timestamp_str.to(TimestampType())


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

    assert decimal_str.to(DecimalType(9, 2)) is None
    assert decimal_str.to(DecimalType(9, 4)) is None


# MISC


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
        (literal(bytearray([0x03, 0x04, 0x05, 0x06])), BinaryType()),
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
    # None related
    assert not fixed_lit012 < None
    assert not fixed_lit012 <= None
    assert fixed_lit012 > None
    assert fixed_lit012 >= None


def test_binary_literal():
    bin_lit012 = literal(bytearray([0x00, 0x01, 0x02]))
    bin_lit013 = literal(bytearray([0x00, 0x01, 0x03]))
    assert bin_lit012 == bin_lit012
    assert bin_lit012 != bin_lit013
    assert bin_lit012 < bin_lit013
    assert bin_lit012 <= bin_lit013
    assert bin_lit013 > bin_lit012
    assert bin_lit013 >= bin_lit012
    # None related
    assert not bin_lit012 < None
    assert not bin_lit012 <= None
    assert bin_lit012 > None
    assert bin_lit012 >= None


def test_binary_to_fixed():
    lit = literal(bytearray([0x00, 0x01, 0x02]))
    fixed_lit = lit.to(FixedType(3))
    assert fixed_lit is not None
    assert lit.value == fixed_lit.value
    assert lit.to(FixedType(4)) is None
    assert lit.to(FixedType(2)) is None


def test_fixed_to_binary():
    lit = literal(bytes([0x00, 0x01, 0x02]))
    binary_lit = lit.to(BinaryType())
    assert binary_lit is not None
    assert lit.value == binary_lit.value


def test_above_max():
    a = AboveMax()
    # singleton
    assert a == AboveMax()
    assert str(a) == "AboveMax"
    with pytest.raises(RuntimeError) as e:
        a.value()
    assert "AboveMax has no value" in str(e.value)
    with pytest.raises(RuntimeError) as e:
        a.to(IntegerType())
    assert "Cannot change the type of AboveMax" in str(e.value)


def test_below_min():
    b = BelowMin()
    # singleton
    assert b == BelowMin()
    assert str(b) == "BelowMin"
    with pytest.raises(RuntimeError) as e:
        b.value()
    assert "BelowMin has no value" in str(e.value)
    with pytest.raises(RuntimeError) as e:
        b.to(IntegerType())
    assert "Cannot change the type of BelowMin" in str(e.value)


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
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_integer_conversions():
    assert_invalid_conversions(
        literal(34),
        [BooleanType(), TimeType(), TimestampType(), TimestamptzType(), StringType(), UUIDType(), FixedType(1), BinaryType()],
    )


def test_invalid_long_conversions():
    assert_invalid_conversions(
        literal(34).to(LongType()),
        [BooleanType(), DateType(), StringType(), UUIDType(), FixedType(1), BinaryType()],
    )


@pytest.mark.parametrize(
    "lit",
    [
        literal(34.11),
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
    assert lit.to(test_type) is None


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
            DecimalType(9, 4),
            StringType(),
            UUIDType(),
            FixedType(1),
            BinaryType(),
        ],
    )


def test_invalid_string_conversions():
    assert_invalid_conversions(
        literal("abc"),
        [BooleanType(), IntegerType(), LongType(), FloatType(), DoubleType(), FixedType(1), BinaryType()],
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
            FixedType(1),
        ],
    )


def test_invalid_binary_conversions():
    assert_invalid_conversions(
        literal(bytearray([0x00, 0x01, 0x02])),
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
            FixedType(1),
        ],
    )


def assert_invalid_conversions(lit, types=None):
    for type_var in types:
        assert lit.to(type_var) is None

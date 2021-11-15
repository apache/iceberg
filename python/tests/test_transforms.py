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

from datetime import datetime
from decimal import Decimal, getcontext
from uuid import UUID

import pytest

from iceberg import transforms
from iceberg.transforms import UnknownTransform
from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


@pytest.mark.parametrize(
    "test_input,test_type,expected",
    [
        (1, IntegerType, 1392991556),
        (34, IntegerType, 2017239379),
        (34, LongType, 2017239379),
        (1, FloatType, -142385009),
        (1, DoubleType, -142385009),
        (17486, DateType, -653330422),
        (81068000000, TimeType, -662762989),
        (
            int(
                datetime.fromisoformat("2017-11-16T22:31:08+00:00").timestamp()
                * 1000000
            ),
            TimestampType,
            -2047944441,
        ),
        (
            int(
                datetime.fromisoformat("2017-11-16T14:31:08-08:00").timestamp()
                * 1000000
            ),
            TimestamptzType,
            -2047944441,
        ),
    ],
)
def test_spec_values_int(test_input, test_type, expected):
    assert transforms.Bucket._FUNCTIONS_MAP[test_type][0](test_input) == expected


@pytest.mark.parametrize(
    "test_input,test_type,scale_factor,expected",
    [
        (Decimal("14.20"), DecimalType(9, 2), Decimal(10) ** -2, 59),
        (
            Decimal("137302769811943318102518958871258.37580"),
            DecimalType(38, 5),
            Decimal(10) ** -5,
            63,
        ),
    ],
)
def test_decimal_bucket(test_input, test_type, scale_factor, expected):
    getcontext().prec = 38
    assert (
        transforms.bucket(test_type, 100).apply(test_input.quantize(scale_factor))
        == expected
    )


@pytest.mark.parametrize(
    "bucket,value,expected",
    [
        (transforms.bucket(IntegerType, 100), 34, 79),
        (transforms.bucket(LongType, 100), 34, 79),
        (transforms.bucket(DateType, 100), 17486, 26),
        (transforms.bucket(TimeType, 100), 81068000000, 59),
        (transforms.bucket(TimestampType, 100), 1510871468000000, 7),
        (transforms.bucket(DecimalType(9, 2), 100), Decimal("14.20"), 59),
        (transforms.bucket(StringType, 100), "iceberg", 89),
        (
            transforms.bucket(UUIDType, 100),
            UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            40,
        ),
        (transforms.bucket(FixedType(3), 128), b"foo", 32),
        (transforms.bucket(BinaryType, 128), b"\x00\x01\x02\x03", 57),
    ],
)
def test_buckets(bucket, value, expected):
    assert bucket.apply(value) == expected


@pytest.mark.parametrize(
    "date,time_transform_name,expected",
    [
        (47, "year", "2017"),
        (575, "month", "2017-12"),
        (17501, "day", "2017-12-01"),
    ],
)
def test_time_to_human_string(date, time_transform_name, expected):
    assert (
        getattr(transforms, time_transform_name)(DateType).to_human_string(date)
        == expected
    )


@pytest.mark.parametrize("time_transform_name", ["year", "month", "day", "hour"])
def test_null_human_string(time_transform_name):
    assert (
        getattr(transforms, time_transform_name)(TimestamptzType).to_human_string(None)
        == "null"
    )


@pytest.mark.parametrize(
    "timestamp,time_transform_name,expected",
    [
        (47, "year", "2017"),
        (575, "month", "2017-12"),
        (17501, "day", "2017-12-01"),
        (420042, "hour", "2017-12-01-18"),
    ],
)
def test_ts_to_human_string(timestamp, time_transform_name, expected):
    assert (
        getattr(transforms, time_transform_name)(TimestampType).to_human_string(
            timestamp
        )
        == expected
    )


@pytest.mark.parametrize("time_transform_name", ["year", "month", "day", "hour"])
def test_null_human_string(time_transform_name):
    assert (
        getattr(transforms, time_transform_name)(TimestampType).to_human_string(None)
        == "null"
    )


@pytest.mark.parametrize(
    "timestamp,time_transform_name,expected",
    [
        (47, "year", "2017"),
        (575, "month", "2017-12"),
        (17501, "day", "2017-12-01"),
        (420042, "hour", "2017-12-01-18"),
    ],
)
def test_ts_to_human_string(timestamp, time_transform_name, expected):
    assert (
        getattr(transforms, time_transform_name)(TimestamptzType).to_human_string(
            timestamp
        )
        == expected
    )


@pytest.mark.parametrize("time_transform_name", ["year", "month", "day", "hour"])
def test_null_human_string(time_transform_name):
    assert (
        getattr(transforms, time_transform_name)(TimestamptzType).to_human_string(None)
        == "null"
    )


@pytest.mark.parametrize(
    "type_var,value,expected",
    [
        (LongType, None, "null"),
        (DateType, 17501, "2017-12-01"),
        (TimeType, 36775038194, "10:12:55.038194"),
        (TimestamptzType, 1512151975038194, "2017-12-01T18:12:55.038194Z"),
        (TimestampType, 1512151975038194, "2017-12-01T18:12:55.038194"),
        (LongType, -1234567890000, "-1234567890000"),
        (StringType, "a/b/c=d", "a/b/c=d"),
        (DecimalType(9, 2), Decimal("-1.50"), "-1.50"),
        (FixedType(100), b"foo", "Zm9v"),
    ],
)
def test_identity_human_string(type_var, value, expected):
    identity = transforms.identity(type_var)
    assert identity.to_human_string(value) == expected


@pytest.mark.parametrize("type_var", [IntegerType, LongType])
@pytest.mark.parametrize(
    "input_var,expected",
    [(1, 0), (5, 0), (9, 0), (10, 10), (11, 10), (-1, -10), (-10, -10), (-12, -20)],
)
def test_truncate_integer(type_var, input_var, expected):
    trunc = transforms.truncate(type_var, 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize(
    "input_var,expected",
    [
        (Decimal(12.34).quantize(Decimal(".01")), Decimal("12.30")),
        (Decimal(12.30).quantize(Decimal(".01")), Decimal("12.30")),
        (Decimal(12.20).quantize(Decimal(".01")), Decimal("12.20")),
        (Decimal(0.05).quantize(Decimal(".01")), Decimal("0.00")),
        (Decimal(-0.05).quantize(Decimal(".01")), Decimal("-0.10")),
    ],
)
def test_truncate_decimal(input_var, expected):
    trunc = transforms.truncate(DecimalType(9, 2), 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize("input_var,expected", [("abcdefg", "abcde"), ("abc", "abc")])
def test_truncate_string(input_var, expected):
    trunc = transforms.truncate(StringType, 5)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize(
    "type_var",
    [
        BinaryType,
        DateType,
        DecimalType(8, 5),
        DoubleType,
        FixedType(8),
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
        TimeType,
        UUIDType,
    ],
)
def test_bucket_method(type_var):
    bucket_transform = transforms.bucket(type_var, 8)
    assert str(bucket_transform) == str(eval(repr(bucket_transform)))
    assert bucket_transform.can_transform(type_var)
    assert bucket_transform.result_type(type_var) == IntegerType
    assert bucket_transform.num_buckets == 8
    assert bucket_transform.apply(None) is None
    assert bucket_transform.to_human_string("test") == "test"


@pytest.mark.parametrize(
    "type_var,value,expected",
    [
        (BinaryType, b"foo", "Zm9v"),
        (DecimalType(8, 5), Decimal("14.20"), "14.20"),
        (IntegerType, 123, "123"),
        (LongType, 123, "123"),
        (StringType, "foo", "foo"),
    ],
)
def test_truncate_method(type_var, value, expected):
    truncate_transform = transforms.truncate(type_var, 8)
    assert str(truncate_transform) == str(eval(repr(truncate_transform)))
    assert truncate_transform.can_transform(type_var)
    assert truncate_transform.result_type(type_var) == type_var
    assert truncate_transform.to_human_string(None) == "null"
    assert truncate_transform.to_human_string(value) == expected
    assert truncate_transform.width == 8
    assert truncate_transform.apply(None) is None
    assert truncate_transform.preserves_order()
    assert truncate_transform.satisfies_order_of(truncate_transform)


@pytest.mark.parametrize(
    "type_var",
    [
        DateType,
        TimestampType,
        TimestamptzType,
    ],
)
def test_time_methods(type_var):
    assert transforms.year(type_var) == eval(repr(transforms.year(type_var)))
    assert transforms.month(type_var) == eval(repr(transforms.month(type_var)))
    assert transforms.day(type_var) == eval(repr(transforms.day(type_var)))
    assert transforms.year(type_var).can_transform(type_var)
    assert transforms.month(type_var).can_transform(type_var)
    assert transforms.day(type_var).can_transform(type_var)
    assert transforms.year(type_var).preserves_order()
    assert transforms.month(type_var).preserves_order()
    assert transforms.day(type_var).preserves_order()
    assert transforms.year(type_var).result_type(type_var) == IntegerType
    assert transforms.month(type_var).result_type(type_var) == IntegerType
    assert transforms.day(type_var).result_type(type_var) == DateType
    assert transforms.year(type_var).dedup_name() == "time"
    assert transforms.month(type_var).dedup_name() == "time"
    assert transforms.day(type_var).dedup_name() == "time"


@pytest.mark.parametrize(
    "transform,value,expected",
    [
        (transforms.day(DateType), 17501, 17501),
        (transforms.month(DateType), 17501, 575),
        (transforms.year(DateType), 17501, 47),
        (transforms.year(TimestampType), 1512151975038194, 47),
        (transforms.month(TimestamptzType), 1512151975038194, 575),
        (transforms.day(TimestampType), 1512151975038194, 17501),
    ],
)
def test_time_apply_method(transform, value, expected):
    assert transform.apply(value) == expected


@pytest.mark.parametrize(
    "type_var",
    [
        TimestampType,
        TimestamptzType,
    ],
)
def test_hour_method(type_var):
    assert transforms.hour(type_var) == eval(repr(transforms.hour(type_var)))
    assert transforms.hour(type_var).can_transform(type_var)
    assert transforms.hour(type_var).result_type(type_var) == IntegerType
    assert transforms.hour(type_var).apply(1512151975038194) == 420042
    assert transforms.hour(type_var).dedup_name() == "time"


@pytest.mark.parametrize(
    "type_var",
    [
        BinaryType,
        BooleanType,
        DateType,
        DecimalType(8, 2),
        DoubleType,
        FixedType(16),
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
        TimeType,
        UUIDType,
    ],
)
def test_identity_method(type_var):
    identity_transform = transforms.identity(type_var)
    assert str(identity_transform) == str(eval(repr(identity_transform)))
    assert identity_transform.can_transform(type_var)
    assert identity_transform.result_type(type_var) == type_var
    assert identity_transform.apply("test") == "test"


@pytest.mark.parametrize(
    "type_var",
    [
        ListType(
            NestedField(
                False,
                1,
                "required_field",
                StructType(
                    [
                        NestedField(True, 2, "optional_field", DecimalType(8, 2)),
                        NestedField(False, 3, "required_field", LongType),
                    ]
                ),
            )
        ),
        MapType(
            NestedField(True, 1, "optional_field", DoubleType),
            NestedField(False, 2, "required_field", UUIDType),
        ),
        StructType(
            [
                NestedField(True, 1, "optional_field", IntegerType),
                NestedField(False, 2, "required_field", FixedType(5)),
                NestedField(
                    False,
                    3,
                    "required_field",
                    StructType(
                        [
                            NestedField(True, 4, "optional_field", DecimalType(8, 2)),
                            NestedField(False, 5, "required_field", LongType),
                        ]
                    ),
                ),
            ]
        ),
    ],
)
def test_identity_nested_type(type_var):
    identity_transform = transforms.identity(type_var)
    assert str(identity_transform) == str(eval(repr(identity_transform)))
    assert not identity_transform.can_transform(type_var)


def test_void_transform():
    void_transform = transforms.always_null()
    assert void_transform == eval(repr(void_transform))
    assert void_transform.apply("test") is None
    assert void_transform.can_transform(BooleanType)
    assert void_transform.result_type(BooleanType) == BooleanType
    assert not void_transform.preserves_order()
    assert void_transform.satisfies_order_of(transforms.always_null())
    assert not void_transform.satisfies_order_of(transforms.year(DateType))
    assert void_transform.to_human_string("test") == "null"
    assert void_transform.dedup_name() == "void"


def test_unknown_transform():
    unknown_transform = UnknownTransform(FixedType(8), "unknown")
    assert str(unknown_transform) == str(eval(repr(unknown_transform)))
    with pytest.raises(AttributeError):
        unknown_transform.apply("test")
    assert unknown_transform.can_transform(FixedType(8))
    assert not unknown_transform.can_transform(FixedType(5))
    assert unknown_transform.result_type(BooleanType) == StringType


@pytest.mark.parametrize(
    "type_var,transform,expected",
    [
        (BinaryType, "bucket[100]", transforms.bucket(BinaryType, 100)),
        (BooleanType, "identity", transforms.identity(BooleanType)),
        (DateType, "year", transforms.year(DateType)),
        (DecimalType(8, 2), "truncate[5]", transforms.truncate(DecimalType(8, 2), 5)),
        (DoubleType, "identity", transforms.identity(DoubleType)),
        (FixedType(16), "bucket[32]", transforms.bucket(FixedType(16), 32)),
        (FloatType, "identity", transforms.identity(FloatType)),
        (IntegerType, "void", transforms.always_null()),
        (LongType, "bucket[16]", transforms.bucket(LongType, 16)),
        (StringType, "truncate[8]", transforms.truncate(StringType, 8)),
        (TimestampType, "month", transforms.month(TimestampType)),
        (TimestamptzType, "hour", transforms.hour(TimestamptzType)),
        (TimeType, "day", UnknownTransform(TimeType, "day")),
        (UUIDType, "bucket[16]", transforms.bucket(UUIDType, 16)),
    ],
)
def test_from_string(type_var, transform, expected):
    assert repr(transforms.from_string(type_var, transform)) == repr(expected)
    assert transform == str(expected)


@pytest.mark.parametrize(
    "transform,other_transform,expected",
    [
        (transforms.identity(BooleanType), transforms.identity(IntegerType), True),
        (transforms.identity(BooleanType), transforms.always_null(), False),
        (transforms.year(DateType), transforms.year(DateType), True),
        (transforms.year(DateType), transforms.month(DateType), False),
        (transforms.year(DateType), transforms.day(DateType), False),
        (transforms.year(DateType), transforms.hour(TimestampType), False),
        (transforms.hour(TimestampType), transforms.month(DateType), True),
        (transforms.day(TimestamptzType), transforms.month(DateType), True),
        (transforms.day(TimestamptzType), transforms.always_null(), False),
        (
            transforms.truncate(StringType, 8),
            transforms.truncate(StringType, 16),
            False,
        ),
        (
            transforms.truncate(StringType, 16),
            transforms.truncate(StringType, 16),
            True,
        ),
        (
            transforms.truncate(StringType, 32),
            transforms.truncate(StringType, 16),
            True,
        ),
        (
            transforms.truncate(StringType, 16),
            transforms.truncate(IntegerType, 8),
            False,
        ),
    ],
)
def test_satisfies_order_of(transform, other_transform, expected):
    assert transform.satisfies_order_of(other_transform) == expected


def test_invalid_cases():
    with pytest.raises(ValueError):
        transforms.hour(DateType)
    with pytest.raises(ValueError):
        transforms.day(IntegerType)
    with pytest.raises(ValueError):
        transforms.month(BinaryType)
    with pytest.raises(ValueError):
        transforms.year(UUIDType)
    with pytest.raises(ValueError):
        transforms.bucket(BooleanType, 8)
    with pytest.raises(ValueError):
        transforms.truncate(UUIDType, 8)
    with pytest.raises(ValueError):
        transforms.Time(DateType, "hour")

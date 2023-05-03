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
# pylint: disable=eval-used,protected-access,redefined-outer-name
from datetime import date
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID

import mmh3 as mmh3
import pytest

from pyiceberg import transforms
from pyiceberg.expressions import (
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotIn,
    BoundNotNull,
    BoundNotStartsWith,
    BoundReference,
    BoundStartsWith,
    EqualTo,
    GreaterThanOrEqual,
    In,
    LessThanOrEqual,
    NotIn,
    NotNull,
    NotStartsWith,
    Reference,
    StartsWith,
)
from pyiceberg.expressions.literals import (
    DateLiteral,
    DecimalLiteral,
    TimestampLiteral,
    literal,
)
from pyiceberg.schema import Accessor
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TimeTransform,
    Transform,
    TruncateTransform,
    UnknownTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.typedef import IcebergBaseModel
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
    NestedField,
    PrimitiveType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from pyiceberg.utils.datetime import (
    date_str_to_days,
    date_to_days,
    time_to_micros,
    timestamp_to_micros,
    timestamptz_to_micros,
)


@pytest.mark.parametrize(
    "test_input,test_type,expected",
    [
        (1, IntegerType(), 1392991556),
        (34, IntegerType(), 2017239379),
        (34, LongType(), 2017239379),
        (date_to_days(date(2017, 11, 16)), DateType(), -653330422),
        (date_str_to_days("2017-11-16"), DateType(), -653330422),
        (time_to_micros("22:31:08"), TimeType(), -662762989),
        (
            timestamp_to_micros("2017-11-16T22:31:08"),
            TimestampType(),
            -2047944441,
        ),
        (
            timestamptz_to_micros("2017-11-16T14:31:08-08:00"),
            TimestamptzType(),
            -2047944441,
        ),
        (b"\x00\x01\x02\x03", BinaryType(), -188683207),
        (b"\x00\x01\x02\x03", FixedType(4), -188683207),
        ("iceberg", StringType(), 1210000089),
        (UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"), UUIDType(), 1488055340),
    ],
)
def test_bucket_hash_values(test_input: Any, test_type: PrimitiveType, expected: Any) -> None:
    assert BucketTransform(num_buckets=8).transform(test_type, bucket=False)(test_input) == expected


@pytest.mark.parametrize(
    "transform,value,expected",
    [
        (BucketTransform(100).transform(IntegerType()), 34, 79),
        (BucketTransform(100).transform(LongType()), 34, 79),
        (BucketTransform(100).transform(DateType()), 17486, 26),
        (BucketTransform(100).transform(TimeType()), 81068000000, 59),
        (BucketTransform(100).transform(TimestampType()), 1510871468000000, 7),
        (BucketTransform(100).transform(DecimalType(9, 2)), Decimal("14.20"), 59),
        (BucketTransform(100).transform(StringType()), "iceberg", 89),
        (
            BucketTransform(100).transform(UUIDType()),
            UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            40,
        ),
        (BucketTransform(128).transform(FixedType(3)), b"foo", 32),
        (BucketTransform(128).transform(BinaryType()), b"\x00\x01\x02\x03", 57),
    ],
)
def test_buckets(transform: Callable[[Any], int], value: Any, expected: int) -> None:
    assert transform(value) == expected


@pytest.mark.parametrize(
    "type_var",
    [
        BinaryType(),
        DateType(),
        DecimalType(8, 5),
        FixedType(8),
        IntegerType(),
        LongType(),
        StringType(),
        TimestampType(),
        TimestamptzType(),
        TimeType(),
        UUIDType(),
    ],
)
def test_bucket_method(type_var: PrimitiveType) -> None:
    bucket_transform = BucketTransform(8)  # type: ignore
    assert str(bucket_transform) == str(eval(repr(bucket_transform)))
    assert bucket_transform.can_transform(type_var)
    assert bucket_transform.result_type(type_var) == IntegerType()
    assert bucket_transform.num_buckets == 8
    assert bucket_transform.apply(None) is None
    assert bucket_transform.to_human_string(type_var, "test") == "test"


def test_string_with_surrogate_pair() -> None:
    string_with_surrogate_pair = "string with a surrogate pair: 💰"
    as_bytes = bytes(string_with_surrogate_pair, "UTF-8")
    bucket_transform = BucketTransform(100).transform(StringType(), bucket=False)
    assert bucket_transform(string_with_surrogate_pair) == mmh3.hash(as_bytes)


@pytest.mark.parametrize(
    "date_val,date_transform,expected",
    [
        (47, YearTransform(), "2017"),
        (575, MonthTransform(), "2017-12"),
        (17501, DayTransform(), "2017-12-01"),
    ],
)
def test_date_to_human_string(date_val: int, date_transform: TimeTransform[Any], expected: str) -> None:
    assert date_transform.to_human_string(DateType(), date_val) == expected


@pytest.mark.parametrize(
    "date_transform",
    [
        YearTransform(),
        MonthTransform(),
        DayTransform(),
    ],
)
def test_none_date_to_human_string(date_transform: TimeTransform[Any]) -> None:
    assert date_transform.to_human_string(DateType(), None) == "null"


def test_hour_to_human_string() -> None:
    assert HourTransform().to_human_string(TimestampType(), None) == "null"
    assert HourTransform().to_human_string(TimestampType(), 420042) == "2017-12-01-18"  # type: ignore


@pytest.mark.parametrize(
    "negative_value,time_transform,expected",
    [
        (-1, YearTransform(), "1969"),
        (-1, MonthTransform(), "1969-12"),
        (-1, DayTransform(), "1969-12-31"),
        (-1, HourTransform(), "1969-12-31-23"),
    ],
)
def test_negative_value_to_human_string(negative_value: int, time_transform: TimeTransform[Any], expected: str) -> None:
    assert time_transform.to_human_string(TimestampType(), negative_value) == expected


@pytest.mark.parametrize(
    "type_var",
    [
        DateType(),
        TimestampType(),
        TimestamptzType(),
    ],
)
def test_time_methods(type_var: PrimitiveType) -> None:
    assert YearTransform().can_transform(type_var)
    assert MonthTransform().can_transform(type_var)
    assert DayTransform().can_transform(type_var)
    assert YearTransform().preserves_order
    assert MonthTransform().preserves_order
    assert DayTransform().preserves_order
    assert YearTransform().result_type(type_var) == IntegerType()
    assert MonthTransform().result_type(type_var) == IntegerType()
    assert DayTransform().result_type(type_var) == DateType()
    assert YearTransform().dedup_name == "time"
    assert MonthTransform().dedup_name == "time"
    assert DayTransform().dedup_name == "time"


@pytest.mark.parametrize(
    "transform,type_var,value,expected",
    [
        (DayTransform(), DateType(), 17501, 17501),
        (DayTransform(), DateType(), -1, -1),
        (MonthTransform(), DateType(), 17501, 575),
        (MonthTransform(), DateType(), -1, -1),
        (YearTransform(), DateType(), 17501, 47),
        (YearTransform(), DateType(), -1, -1),
        (YearTransform(), TimestampType(), 1512151975038194, 47),
        (YearTransform(), TimestampType(), -1, -1),
        (MonthTransform(), TimestamptzType(), 1512151975038194, 575),
        (MonthTransform(), TimestamptzType(), -1, -1),
        (DayTransform(), TimestampType(), 1512151975038194, 17501),
        (DayTransform(), TimestampType(), -1, -1),
    ],
)
def test_time_apply_method(transform: TimeTransform[Any], type_var: PrimitiveType, value: int, expected: int) -> None:
    assert transform.transform(type_var)(value) == expected


@pytest.mark.parametrize(
    "type_var",
    [
        TimestampType(),
        TimestamptzType(),
    ],
)
def test_hour_method(type_var: PrimitiveType) -> None:
    assert HourTransform().can_transform(type_var)
    assert HourTransform().result_type(type_var) == IntegerType()
    assert HourTransform().transform(type_var)(1512151975038194) == 420042  # type: ignore
    assert HourTransform().dedup_name == "time"


@pytest.mark.parametrize(
    "transform,other_transform",
    [
        (YearTransform(), MonthTransform()),
        (YearTransform(), DayTransform()),
        (YearTransform(), HourTransform()),
        (MonthTransform(), DayTransform()),
        (MonthTransform(), HourTransform()),
        (DayTransform(), HourTransform()),
    ],
)
def test_satisfies_order_of_method(transform: TimeTransform[Any], other_transform: TimeTransform[Any]) -> None:
    assert transform.satisfies_order_of(transform)
    assert other_transform.satisfies_order_of(transform)
    assert not transform.satisfies_order_of(other_transform)
    assert not transform.satisfies_order_of(VoidTransform())
    assert not other_transform.satisfies_order_of(IdentityTransform())


@pytest.mark.parametrize(
    "type_var,value,expected",
    [
        (LongType(), None, "null"),
        (DateType(), 17501, "2017-12-01"),
        (TimeType(), 36775038194, "10:12:55.038194"),
        (TimestamptzType(), 1512151975038194, "2017-12-01T18:12:55.038194+00:00"),
        (TimestampType(), 1512151975038194, "2017-12-01T18:12:55.038194"),
        (LongType(), -1234567890000, "-1234567890000"),
        (StringType(), "a/b/c=d", "a/b/c=d"),
        (DecimalType(9, 2), Decimal("-1.50"), "-1.50"),
        (FixedType(100), b"foo", "Zm9v"),
    ],
)
def test_identity_human_string(type_var: PrimitiveType, value: Any, expected: str) -> None:
    identity = IdentityTransform()  # type: ignore
    assert identity.to_human_string(type_var, value) == expected


@pytest.mark.parametrize(
    "type_var",
    [
        BinaryType(),
        BooleanType(),
        DateType(),
        DecimalType(8, 2),
        DoubleType(),
        FixedType(16),
        FloatType(),
        IntegerType(),
        LongType(),
        StringType(),
        TimestampType(),
        TimestamptzType(),
        TimeType(),
        UUIDType(),
    ],
)
def test_identity_method(type_var: PrimitiveType) -> None:
    identity_transform = IdentityTransform()  # type: ignore
    assert str(identity_transform) == str(eval(repr(identity_transform)))
    assert identity_transform.can_transform(type_var)
    assert identity_transform.result_type(type_var) == type_var
    assert identity_transform.transform(type_var)("test") == "test"


@pytest.mark.parametrize("type_var", [IntegerType(), LongType()])
@pytest.mark.parametrize(
    "input_var,expected",
    [(1, 0), (5, 0), (9, 0), (10, 10), (11, 10), (-1, -10), (-10, -10), (-12, -20)],
)
def test_truncate_integer(type_var: PrimitiveType, input_var: int, expected: int) -> None:
    trunc = TruncateTransform(10)  # type: ignore
    assert trunc.transform(type_var)(input_var) == expected


@pytest.mark.parametrize(
    "input_var,expected",
    [
        (Decimal("12.34"), Decimal("12.30")),
        (Decimal("12.30"), Decimal("12.30")),
        (Decimal("12.29"), Decimal("12.20")),
        (Decimal("0.05"), Decimal("0.00")),
        (Decimal("-0.05"), Decimal("-0.10")),
    ],
)
def test_truncate_decimal(input_var: Decimal, expected: Decimal) -> None:
    trunc = TruncateTransform(10)  # type: ignore
    assert trunc.transform(DecimalType(9, 2))(input_var) == expected


@pytest.mark.parametrize("input_var,expected", [("abcdefg", "abcde"), ("abc", "abc")])
def test_truncate_string(input_var: str, expected: str) -> None:
    trunc = TruncateTransform(5)  # type: ignore
    assert trunc.transform(StringType())(input_var) == expected


@pytest.mark.parametrize(
    "type_var,value,expected_human_str,expected",
    [
        (BinaryType(), b"\x00\x01\x02\x03", "AAECAw==", b"\x00"),
        (BinaryType(), bytes("\u2603de", "utf-8"), "4piDZGU=", b"\xe2"),
        (DecimalType(8, 5), Decimal("14.21"), "14.21", Decimal("14.21")),
        (IntegerType(), 123, "123", 123),
        (LongType(), 123, "123", 123),
        (StringType(), "foo", "foo", "f"),
        (StringType(), "\u2603de", "\u2603de", "\u2603"),
    ],
)
def test_truncate_method(type_var: PrimitiveType, value: Any, expected_human_str: str, expected: Any) -> None:
    truncate_transform = TruncateTransform(1)  # type: ignore
    assert str(truncate_transform) == str(eval(repr(truncate_transform)))
    assert truncate_transform.can_transform(type_var)
    assert truncate_transform.result_type(type_var) == type_var
    assert truncate_transform.to_human_string(type_var, value) == expected_human_str
    assert truncate_transform.transform(type_var)(value) == expected
    assert truncate_transform.to_human_string(type_var, None) == "null"
    assert truncate_transform.width == 1
    assert truncate_transform.transform(type_var)(None) is None
    assert truncate_transform.preserves_order
    assert truncate_transform.satisfies_order_of(truncate_transform)


def test_unknown_transform() -> None:
    unknown_transform = transforms.UnknownTransform("unknown")  # type: ignore
    assert str(unknown_transform) == str(eval(repr(unknown_transform)))
    with pytest.raises(AttributeError):
        unknown_transform.transform(StringType())("test")
    assert not unknown_transform.can_transform(FixedType(5))
    assert isinstance(unknown_transform.result_type(BooleanType()), StringType)


def test_void_transform() -> None:
    void_transform = VoidTransform()  # type: ignore
    assert void_transform is VoidTransform()
    assert void_transform == eval(repr(void_transform))
    assert void_transform.transform(StringType())("test") is None
    assert void_transform.can_transform(BooleanType())
    assert isinstance(void_transform.result_type(BooleanType()), BooleanType)
    assert not void_transform.preserves_order
    assert void_transform.satisfies_order_of(VoidTransform())
    assert not void_transform.satisfies_order_of(BucketTransform(100))
    assert void_transform.to_human_string(StringType(), "test") == "null"
    assert void_transform.dedup_name == "void"


class TestType(IcebergBaseModel):
    __root__: Transform[Any, Any]


def test_bucket_transform_serialize() -> None:
    assert BucketTransform(num_buckets=22).json() == '"bucket[22]"'


def test_bucket_transform_deserialize() -> None:
    transform = TestType.parse_raw('"bucket[22]"').__root__
    assert transform == BucketTransform(num_buckets=22)


def test_bucket_transform_str() -> None:
    assert str(BucketTransform(num_buckets=22)) == "bucket[22]"


def test_bucket_transform_repr() -> None:
    assert repr(BucketTransform(num_buckets=22)) == "BucketTransform(num_buckets=22)"


def test_truncate_transform_serialize() -> None:
    assert UnknownTransform("unknown").json() == '"unknown"'


def test_unknown_transform_deserialize() -> None:
    transform = TestType.parse_raw('"unknown"').__root__
    assert transform == UnknownTransform("unknown")


def test_unknown_transform_str() -> None:
    assert str(UnknownTransform("unknown")) == "unknown"


def test_unknown_transform_repr() -> None:
    assert repr(UnknownTransform("unknown")) == "UnknownTransform(transform='unknown')"


def test_void_transform_serialize() -> None:
    assert VoidTransform().json() == '"void"'


def test_void_transform_deserialize() -> None:
    transform = TestType.parse_raw('"void"').__root__
    assert transform == VoidTransform()


def test_void_transform_str() -> None:
    assert str(VoidTransform()) == "void"


def test_void_transform_repr() -> None:
    assert repr(VoidTransform()) == "VoidTransform()"


def test_year_transform_serialize() -> None:
    assert YearTransform().json() == '"year"'


def test_year_transform_deserialize() -> None:
    transform = TestType.parse_raw('"year"').__root__
    assert transform == YearTransform()


def test_month_transform_serialize() -> None:
    assert MonthTransform().json() == '"month"'


def test_month_transform_deserialize() -> None:
    transform = TestType.parse_raw('"month"').__root__
    assert transform == MonthTransform()


def test_day_transform_serialize() -> None:
    assert DayTransform().json() == '"day"'


def test_day_transform_deserialize() -> None:
    transform = TestType.parse_raw('"day"').__root__
    assert transform == DayTransform()


def test_hour_transform_serialize() -> None:
    assert HourTransform().json() == '"hour"'


def test_hour_transform_deserialize() -> None:
    transform = TestType.parse_raw('"hour"').__root__
    assert transform == HourTransform()


@pytest.mark.parametrize(
    "transform,transform_str",
    [
        (YearTransform(), "year"),
        (MonthTransform(), "month"),
        (DayTransform(), "day"),
        (HourTransform(), "hour"),
    ],
)
def test_datetime_transform_str(transform: TimeTransform[Any], transform_str: str) -> None:
    assert str(transform) == transform_str


@pytest.mark.parametrize(
    "transform,transform_repr",
    [
        (YearTransform(), "YearTransform()"),
        (MonthTransform(), "MonthTransform()"),
        (DayTransform(), "DayTransform()"),
        (HourTransform(), "HourTransform()"),
    ],
)
def test_datetime_transform_repr(transform: TimeTransform[Any], transform_repr: str) -> None:
    assert repr(transform) == transform_repr


@pytest.fixture
def bound_reference_str() -> BoundReference[str]:
    return BoundReference(field=NestedField(1, "field", StringType(), required=False), accessor=Accessor(position=0, inner=None))


@pytest.fixture
def bound_reference_date() -> BoundReference[int]:
    return BoundReference(field=NestedField(1, "field", DateType(), required=False), accessor=Accessor(position=0, inner=None))


@pytest.fixture
def bound_reference_timestamp() -> BoundReference[int]:
    return BoundReference(
        field=NestedField(1, "field", TimestampType(), required=False), accessor=Accessor(position=0, inner=None)
    )


@pytest.fixture
def bound_reference_decimal() -> BoundReference[Decimal]:
    return BoundReference(
        field=NestedField(1, "field", DecimalType(8, 2), required=False), accessor=Accessor(position=0, inner=None)
    )


@pytest.fixture
def bound_reference_long() -> BoundReference[int]:
    return BoundReference(
        field=NestedField(1, "field", DecimalType(8, 2), required=False), accessor=Accessor(position=0, inner=None)
    )


def test_projection_bucket_unary(bound_reference_str: BoundReference[str]) -> None:
    assert BucketTransform(2).project("name", BoundNotNull(term=bound_reference_str)) == NotNull(term=Reference(name="name"))


def test_projection_bucket_literal(bound_reference_str: BoundReference[str]) -> None:
    assert BucketTransform(2).project("name", BoundEqualTo(term=bound_reference_str, literal=literal("data"))) == EqualTo(
        term="name", literal=1
    )


def test_projection_bucket_set_same_bucket(bound_reference_str: BoundReference[str]) -> None:
    assert BucketTransform(2).project(
        "name", BoundIn(term=bound_reference_str, literals={literal("hello"), literal("world")})
    ) == EqualTo(term="name", literal=1)


def test_projection_bucket_set_in(bound_reference_str: BoundReference[str]) -> None:
    assert BucketTransform(3).project(
        "name", BoundIn(term=bound_reference_str, literals={literal("hello"), literal("world")})
    ) == In(term="name", literals={1, 2})


def test_projection_bucket_set_not_in(bound_reference_str: BoundReference[str]) -> None:
    assert (
        BucketTransform(3).project("name", BoundNotIn(term=bound_reference_str, literals={literal("hello"), literal("world")}))
        is None
    )


def test_projection_year_unary(bound_reference_date: BoundReference[int]) -> None:
    assert YearTransform().project("name", BoundNotNull(term=bound_reference_date)) == NotNull(term="name")


def test_projection_year_literal(bound_reference_date: BoundReference[int]) -> None:
    assert YearTransform().project("name", BoundEqualTo(term=bound_reference_date, literal=DateLiteral(1925))) == EqualTo(
        term="name", literal=5
    )


def test_projection_year_set_same_year(bound_reference_date: BoundReference[int]) -> None:
    assert YearTransform().project(
        "name", BoundIn(term=bound_reference_date, literals={DateLiteral(1925), DateLiteral(1926)})
    ) == EqualTo(term="name", literal=5)


def test_projection_year_set_in(bound_reference_date: BoundReference[int]) -> None:
    assert YearTransform().project(
        "name", BoundIn(term=bound_reference_date, literals={DateLiteral(1925), DateLiteral(2925)})
    ) == In(term="name", literals={8, 5})


def test_projection_year_set_not_in(bound_reference_date: BoundReference[int]) -> None:
    assert (
        YearTransform().project("name", BoundNotIn(term=bound_reference_date, literals={DateLiteral(1925), DateLiteral(2925)}))
        is None
    )


def test_projection_month_unary(bound_reference_date: BoundReference[int]) -> None:
    assert MonthTransform().project("name", BoundNotNull(term=bound_reference_date)) == NotNull(term="name")


def test_projection_month_literal(bound_reference_date: BoundReference[int]) -> None:
    assert MonthTransform().project("name", BoundEqualTo(term=bound_reference_date, literal=DateLiteral(1925))) == EqualTo(
        term="name", literal=63
    )


def test_projection_month_set_same_month(bound_reference_date: BoundReference[int]) -> None:
    assert MonthTransform().project(
        "name", BoundIn(term=bound_reference_date, literals={DateLiteral(1925), DateLiteral(1926)})
    ) == EqualTo(term="name", literal=63)


def test_projection_month_set_in(bound_reference_date: BoundReference[int]) -> None:
    assert MonthTransform().project(
        "name", BoundIn(term=bound_reference_date, literals={DateLiteral(1925), DateLiteral(2925)})
    ) == In(term="name", literals={96, 63})


def test_projection_day_month_not_in(bound_reference_date: BoundReference[int]) -> None:
    assert (
        MonthTransform().project("name", BoundNotIn(term=bound_reference_date, literals={DateLiteral(1925), DateLiteral(2925)}))
        is None
    )


def test_projection_day_unary(bound_reference_timestamp: BoundReference[int]) -> None:
    assert DayTransform().project("name", BoundNotNull(term=bound_reference_timestamp)) == NotNull(term="name")


def test_projection_day_literal(bound_reference_timestamp: BoundReference[int]) -> None:
    assert DayTransform().project(
        "name", BoundEqualTo(term=bound_reference_timestamp, literal=TimestampLiteral(1667696874000))
    ) == EqualTo(term="name", literal=19)


def test_projection_day_set_same_day(bound_reference_timestamp: BoundReference[int]) -> None:
    assert DayTransform().project(
        "name",
        BoundIn(term=bound_reference_timestamp, literals={TimestampLiteral(1667696874001), TimestampLiteral(1667696874000)}),
    ) == EqualTo(term="name", literal=19)


def test_projection_day_set_in(bound_reference_timestamp: BoundReference[int]) -> None:
    assert DayTransform().project(
        "name",
        BoundIn(term=bound_reference_timestamp, literals={TimestampLiteral(1667696874001), TimestampLiteral(1567696874000)}),
    ) == In(term="name", literals={18, 19})


def test_projection_day_set_not_in(bound_reference_timestamp: BoundReference[int]) -> None:
    assert (
        DayTransform().project(
            "name",
            BoundNotIn(term=bound_reference_timestamp, literals={TimestampLiteral(1567696874), TimestampLiteral(1667696874)}),
        )
        is None
    )


def test_projection_day_human(bound_reference_date: BoundReference[int]) -> None:
    date_literal = DateLiteral(17532)
    assert DayTransform().project("dt", BoundEqualTo(term=bound_reference_date, literal=date_literal)) == EqualTo(
        term="dt", literal=17532
    )  # == 2018, 1, 1

    assert DayTransform().project("dt", BoundLessThanOrEqual(term=bound_reference_date, literal=date_literal)) == LessThanOrEqual(
        term="dt", literal=17532
    )  # <= 2018, 1, 1

    assert DayTransform().project("dt", BoundLessThan(term=bound_reference_date, literal=date_literal)) == LessThanOrEqual(
        term="dt", literal=17531
    )  # <= 2017, 12, 31

    assert DayTransform().project(
        "dt", BoundGreaterThanOrEqual(term=bound_reference_date, literal=date_literal)
    ) == GreaterThanOrEqual(
        term="dt", literal=17532
    )  # >= 2018, 1, 1

    assert DayTransform().project("dt", BoundGreaterThan(term=bound_reference_date, literal=date_literal)) == GreaterThanOrEqual(
        term="dt", literal=17533
    )  # >= 2018, 1, 2


def test_projection_hour_unary(bound_reference_timestamp: BoundReference[int]) -> None:
    assert HourTransform().project("name", BoundNotNull(term=bound_reference_timestamp)) == NotNull(term="name")


TIMESTAMP_EXAMPLE = 1667696874000000  # Sun Nov 06 2022 01:07:54
HOUR_IN_MICROSECONDS = 60 * 60 * 1000 * 1000


def test_projection_hour_literal(bound_reference_timestamp: BoundReference[int]) -> None:
    assert HourTransform().project(
        "name", BoundEqualTo(term=bound_reference_timestamp, literal=TimestampLiteral(TIMESTAMP_EXAMPLE))
    ) == EqualTo(term="name", literal=463249)


def test_projection_hour_set_same_hour(bound_reference_timestamp: BoundReference[int]) -> None:
    assert HourTransform().project(
        "name",
        BoundIn(
            term=bound_reference_timestamp,
            literals={TimestampLiteral(TIMESTAMP_EXAMPLE + 1), TimestampLiteral(TIMESTAMP_EXAMPLE)},
        ),
    ) == EqualTo(term="name", literal=463249)


def test_projection_hour_set_in(bound_reference_timestamp: BoundReference[int]) -> None:
    assert HourTransform().project(
        "name",
        BoundIn(
            term=bound_reference_timestamp,
            literals={TimestampLiteral(TIMESTAMP_EXAMPLE + HOUR_IN_MICROSECONDS), TimestampLiteral(TIMESTAMP_EXAMPLE)},
        ),
    ) == In(term="name", literals={463249, 463250})


def test_projection_hour_set_not_in(bound_reference_timestamp: BoundReference[int]) -> None:
    assert (
        HourTransform().project(
            "name",
            BoundNotIn(
                term=bound_reference_timestamp,
                literals={TimestampLiteral(TIMESTAMP_EXAMPLE + HOUR_IN_MICROSECONDS), TimestampLiteral(TIMESTAMP_EXAMPLE)},
            ),
        )
        is None
    )


def test_projection_identity_unary(bound_reference_timestamp: BoundReference[int]) -> None:
    assert IdentityTransform().project("name", BoundNotNull(term=bound_reference_timestamp)) == NotNull(term="name")


def test_projection_identity_literal(bound_reference_timestamp: BoundReference[int]) -> None:
    assert IdentityTransform().project(
        "name", BoundEqualTo(term=bound_reference_timestamp, literal=TimestampLiteral(TIMESTAMP_EXAMPLE))
    ) == EqualTo(term="name", literal=TimestampLiteral(TIMESTAMP_EXAMPLE))


def test_projection_identity_set_in(bound_reference_timestamp: BoundReference[int]) -> None:
    assert IdentityTransform().project(
        "name",
        BoundIn(
            term=bound_reference_timestamp,
            literals={TimestampLiteral(TIMESTAMP_EXAMPLE + HOUR_IN_MICROSECONDS), TimestampLiteral(TIMESTAMP_EXAMPLE)},
        ),
    ) == In(
        term="name",
        literals={TimestampLiteral(TIMESTAMP_EXAMPLE + HOUR_IN_MICROSECONDS), TimestampLiteral(TIMESTAMP_EXAMPLE)},
    )


def test_projection_identity_set_not_in(bound_reference_timestamp: BoundReference[int]) -> None:
    assert IdentityTransform().project(
        "name",
        BoundNotIn(
            term=bound_reference_timestamp,
            literals={TimestampLiteral(TIMESTAMP_EXAMPLE + HOUR_IN_MICROSECONDS), TimestampLiteral(TIMESTAMP_EXAMPLE)},
        ),
    ) == NotIn(
        term="name",
        literals={TimestampLiteral(TIMESTAMP_EXAMPLE + HOUR_IN_MICROSECONDS), TimestampLiteral(TIMESTAMP_EXAMPLE)},
    )


def test_projection_truncate_string_unary(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project("name", BoundNotNull(term=bound_reference_str)) == NotNull(term="name")


def test_projection_truncate_string_literal_eq(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project("name", BoundEqualTo(term=bound_reference_str, literal=literal("data"))) == EqualTo(
        term="name", literal=literal("da")
    )


def test_projection_truncate_string_literal_gt(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project("name", BoundGreaterThan(term=bound_reference_str, literal=literal("data"))) == EqualTo(
        term="name", literal=literal("da")
    )


def test_projection_truncate_string_literal_gte(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundGreaterThanOrEqual(term=bound_reference_str, literal=literal("data"))
    ) == EqualTo(term="name", literal=literal("da"))


def test_projection_truncate_string_set_same_result(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundIn(term=bound_reference_str, literals={literal("hello"), literal("helloworld")})
    ) == EqualTo(term="name", literal=literal("he"))


def test_projection_truncate_string_set_in(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(3).project(
        "name", BoundIn(term=bound_reference_str, literals={literal("hello"), literal("world")})
    ) == In(term="name", literals={literal("hel"), literal("wor")})


def test_projection_truncate_string_set_not_in(bound_reference_str: BoundReference[str]) -> None:
    assert (
        TruncateTransform(3).project("name", BoundNotIn(term=bound_reference_str, literals={literal("hello"), literal("world")}))
        is None
    )


def test_projection_truncate_decimal_literal_eq(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundEqualTo(term=bound_reference_decimal, literal=DecimalLiteral(Decimal(19.25)))
    ) == EqualTo(term="name", literal=Decimal("19.24"))


def test_projection_truncate_decimal_literal_gt(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundGreaterThan(term=bound_reference_decimal, literal=DecimalLiteral(Decimal(19.25)))
    ) == GreaterThanOrEqual(term="name", literal=Decimal("19.26"))


def test_projection_truncate_decimal_literal_gte(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundGreaterThanOrEqual(term=bound_reference_decimal, literal=DecimalLiteral(Decimal(19.25)))
    ) == GreaterThanOrEqual(term="name", literal=Decimal("19.24"))


def test_projection_truncate_decimal_in(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundIn(term=bound_reference_decimal, literals={literal(Decimal(19.25)), literal(Decimal(18.15))})
    ) == In(
        term="name",
        literals={
            Decimal("19.24"),
            Decimal("18.14999999999999857891452847979962825775146484374"),
        },
    )


def test_projection_truncate_long_literal_eq(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundEqualTo(term=bound_reference_decimal, literal=DecimalLiteral(Decimal(19.25)))
    ) == EqualTo(term="name", literal=Decimal("19.24"))


def test_projection_truncate_long_literal_gt(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundGreaterThan(term=bound_reference_decimal, literal=DecimalLiteral(Decimal(19.25)))
    ) == GreaterThanOrEqual(term="name", literal=Decimal("19.26"))


def test_projection_truncate_long_literal_gte(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundGreaterThanOrEqual(term=bound_reference_decimal, literal=DecimalLiteral(Decimal(19.25)))
    ) == GreaterThanOrEqual(term="name", literal=Decimal("19.24"))


def test_projection_truncate_long_in(bound_reference_decimal: BoundReference[Decimal]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundIn(term=bound_reference_decimal, literals={DecimalLiteral(Decimal(19.25)), DecimalLiteral(Decimal(18.15))})
    ) == In(
        term="name",
        literals={
            Decimal("19.24"),
            Decimal("18.14999999999999857891452847979962825775146484374"),
        },
    )


def test_projection_truncate_string_starts_with(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundStartsWith(term=bound_reference_str, literal=literal("hello"))
    ) == StartsWith(term="name", literal=literal("he"))


def test_projection_truncate_string_not_starts_with(bound_reference_str: BoundReference[str]) -> None:
    assert TruncateTransform(2).project(
        "name", BoundNotStartsWith(term=bound_reference_str, literal=literal("hello"))
    ) == NotStartsWith(term="name", literal=literal("he"))

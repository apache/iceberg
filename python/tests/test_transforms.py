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
# pylint: disable=W0123

from decimal import Decimal
from uuid import UUID

import mmh3 as mmh3
import pytest

from pyiceberg import transforms
from pyiceberg.transforms import (
    BucketBytesTransform,
    BucketDecimalTransform,
    BucketNumberTransform,
    BucketStringTransform,
    BucketUUIDTransform,
    IdentityTransform,
    TruncateTransform,
    UnknownTransform,
    VoidTransform,
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
from pyiceberg.utils.datetime import (
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
        (date_to_days("2017-11-16"), DateType(), -653330422),
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
def test_bucket_hash_values(test_input, test_type, expected):
    assert transforms.bucket(test_type, 8).hash(test_input) == expected


@pytest.mark.parametrize(
    "bucket,value,expected",
    [
        (transforms.bucket(IntegerType(), 100), 34, 79),
        (transforms.bucket(LongType(), 100), 34, 79),
        (transforms.bucket(DateType(), 100), 17486, 26),
        (transforms.bucket(TimeType(), 100), 81068000000, 59),
        (transforms.bucket(TimestampType(), 100), 1510871468000000, 7),
        (transforms.bucket(DecimalType(9, 2), 100), Decimal("14.20"), 59),
        (transforms.bucket(StringType(), 100), "iceberg", 89),
        (
            transforms.bucket(UUIDType(), 100),
            UUID("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            40,
        ),
        (transforms.bucket(FixedType(3), 128), b"foo", 32),
        (transforms.bucket(BinaryType(), 128), b"\x00\x01\x02\x03", 57),
    ],
)
def test_buckets(bucket, value, expected):
    assert bucket.apply(value) == expected


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
def test_bucket_method(type_var):
    bucket_transform = transforms.bucket(type_var, 8)
    assert str(bucket_transform) == str(eval(repr(bucket_transform)))
    assert bucket_transform.can_transform(type_var)
    assert bucket_transform.result_type(type_var) == IntegerType()
    assert bucket_transform.num_buckets == 8
    assert bucket_transform.apply(None) is None
    assert bucket_transform.to_human_string("test") == "test"


def test_string_with_surrogate_pair():
    string_with_surrogate_pair = "string with a surrogate pair: 💰"
    as_bytes = bytes(string_with_surrogate_pair, "UTF-8")
    bucket_transform = transforms.bucket(StringType(), 100)
    assert bucket_transform.hash(string_with_surrogate_pair) == mmh3.hash(as_bytes)


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
def test_identity_human_string(type_var, value, expected):
    identity = transforms.identity(type_var)
    assert identity.to_human_string(value) == expected


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
def test_identity_method(type_var):
    identity_transform = transforms.identity(type_var)
    assert str(identity_transform) == str(eval(repr(identity_transform)))
    assert identity_transform.can_transform(type_var)
    assert identity_transform.result_type(type_var) == type_var
    assert identity_transform.apply("test") == "test"


@pytest.mark.parametrize("type_var", [IntegerType(), LongType()])
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
        (Decimal("12.34"), Decimal("12.30")),
        (Decimal("12.30"), Decimal("12.30")),
        (Decimal("12.29"), Decimal("12.20")),
        (Decimal("0.05"), Decimal("0.00")),
        (Decimal("-0.05"), Decimal("-0.10")),
    ],
)
def test_truncate_decimal(input_var, expected):
    trunc = transforms.truncate(DecimalType(9, 2), 10)
    assert trunc.apply(input_var) == expected


@pytest.mark.parametrize("input_var,expected", [("abcdefg", "abcde"), ("abc", "abc")])
def test_truncate_string(input_var, expected):
    trunc = transforms.truncate(StringType(), 5)
    assert trunc.apply(input_var) == expected


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
def test_truncate_method(type_var, value, expected_human_str, expected):
    truncate_transform = transforms.truncate(type_var, 1)
    assert str(truncate_transform) == str(eval(repr(truncate_transform)))
    assert truncate_transform.can_transform(type_var)
    assert truncate_transform.result_type(type_var) == type_var
    assert truncate_transform.to_human_string(value) == expected_human_str
    assert truncate_transform.apply(value) == expected
    assert truncate_transform.to_human_string(None) == "null"
    assert truncate_transform.width == 1
    assert truncate_transform.apply(None) is None
    assert truncate_transform.preserves_order
    assert truncate_transform.satisfies_order_of(truncate_transform)


def test_unknown_transform():
    unknown_transform = transforms.UnknownTransform(FixedType(8), "unknown")
    assert str(unknown_transform) == str(eval(repr(unknown_transform)))
    with pytest.raises(AttributeError):
        unknown_transform.apply("test")
    assert unknown_transform.can_transform(FixedType(8))
    assert not unknown_transform.can_transform(FixedType(5))
    assert isinstance(unknown_transform.result_type(BooleanType()), StringType)


def test_void_transform():
    void_transform = transforms.always_null()
    assert void_transform is transforms.always_null()
    assert void_transform == eval(repr(void_transform))
    assert void_transform.apply("test") is None
    assert void_transform.can_transform(BooleanType())
    assert isinstance(void_transform.result_type(BooleanType()), BooleanType)
    assert not void_transform.preserves_order
    assert void_transform.satisfies_order_of(transforms.always_null())
    assert not void_transform.satisfies_order_of(transforms.bucket(DateType(), 100))
    assert void_transform.to_human_string("test") == "null"
    assert void_transform.dedup_name == "void"


def test_bucket_number_transform_json():
    assert BucketNumberTransform(source_type=IntegerType(), num_buckets=22).json() == '"bucket[22]"'


def test_bucket_number_transform_str():
    assert str(BucketNumberTransform(source_type=IntegerType(), num_buckets=22)) == "bucket[22]"


def test_bucket_number_transform_repr():
    assert (
        repr(BucketNumberTransform(source_type=IntegerType(), num_buckets=22))
        == "transforms.bucket(source_type=IntegerType(), num_buckets=22)"
    )


def test_bucket_decimal_transform_json():
    assert BucketDecimalTransform(source_type=DecimalType(19, 25), num_buckets=22).json() == '"bucket[22]"'


def test_bucket_decimal_transform_str():
    assert str(BucketDecimalTransform(source_type=DecimalType(19, 25), num_buckets=22)) == "bucket[22]"


def test_bucket_decimal_transform_repr():
    assert (
        repr(BucketDecimalTransform(source_type=DecimalType(19, 25), num_buckets=22))
        == "transforms.bucket(source_type=DecimalType(precision=19, scale=25), num_buckets=22)"
    )


def test_bucket_string_transform_json():
    assert BucketStringTransform(StringType(), num_buckets=22).json() == '"bucket[22]"'


def test_bucket_string_transform_str():
    assert str(BucketStringTransform(StringType(), num_buckets=22)) == "bucket[22]"


def test_bucket_string_transform_repr():
    assert (
        repr(BucketStringTransform(StringType(), num_buckets=22)) == "transforms.bucket(source_type=StringType(), num_buckets=22)"
    )


def test_bucket_bytes_transform_json():
    assert BucketBytesTransform(BinaryType(), num_buckets=22).json() == '"bucket[22]"'


def test_bucket_bytes_transform_str():
    assert str(BucketBytesTransform(BinaryType(), num_buckets=22)) == "bucket[22]"


def test_bucket_bytes_transform_repr():
    assert (
        repr(BucketBytesTransform(BinaryType(), num_buckets=22)) == "transforms.bucket(source_type=BinaryType(), num_buckets=22)"
    )


def test_bucket_uuid_transform_json():
    assert BucketUUIDTransform(UUIDType(), num_buckets=22).json() == '"bucket[22]"'


def test_bucket_uuid_transform_str():
    assert str(BucketUUIDTransform(UUIDType(), num_buckets=22)) == "bucket[22]"


def test_bucket_uuid_transform_repr():
    assert repr(BucketUUIDTransform(UUIDType(), num_buckets=22)) == "transforms.bucket(source_type=UUIDType(), num_buckets=22)"


def test_identity_transform_json():
    assert IdentityTransform(StringType()).json() == '"identity"'


def test_identity_transform_str():
    assert str(IdentityTransform(StringType())) == "identity"


def test_identity_transform_repr():
    assert repr(IdentityTransform(StringType())) == "transforms.identity(source_type=StringType())"


def test_truncate_transform_json():
    assert TruncateTransform(StringType(), 22).json() == '"truncate[22]"'


def test_truncate_transform_str():
    assert str(TruncateTransform(StringType(), 22)) == "truncate[22]"


def test_truncate_transform_repr():
    assert repr(TruncateTransform(StringType(), 22)) == "transforms.truncate(source_type=StringType(), width=22)"


def test_unknown_transform_json():
    assert UnknownTransform(StringType(), "unknown").json() == '"unknown"'


def test_unknown_transform_str():
    assert str(UnknownTransform(StringType(), "unknown")) == "unknown"


def test_unknown_transform_repr():
    assert (
        repr(UnknownTransform(StringType(), "unknown"))
        == "transforms.UnknownTransform(source_type=StringType(), transform='unknown')"
    )


def test_void_transform_json():
    assert VoidTransform().json() == '"void"'


def test_void_transform_str():
    assert str(VoidTransform()) == "void"


def test_void_transform_repr():
    assert repr(VoidTransform()) == "transforms.always_null()"

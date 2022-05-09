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

from decimal import Decimal
from uuid import UUID

import mmh3 as mmh3
import pytest

from iceberg import transforms
from iceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    FixedType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)
from iceberg.utils.datetime import (
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


def test_unknown_transform():
    unknown_transform = transforms.UnknownTransform(FixedType(8), "unknown")
    assert str(unknown_transform) == str(eval(repr(unknown_transform)))
    with pytest.raises(AttributeError):
        unknown_transform.apply("test")
    assert unknown_transform.can_transform(FixedType(8))
    assert not unknown_transform.can_transform(FixedType(5))
    assert isinstance(unknown_transform.result_type(BooleanType()), StringType)

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

from iceberg.api.expressions import Literal
from iceberg.api.transforms import (Identity,
                                    Transforms)
from iceberg.api.types import (DateType,
                               DecimalType,
                               LongType,
                               StringType,
                               TimestampType,
                               TimeType)


def test_null_human_string():
    long_type = LongType.get()
    identity = Identity(long_type)
    assert identity.to_human_string(None) == "null"


def test_date_human_string():
    date = DateType.get()

    identity = Transforms.identity(date)
    date_str = "2017-12-01"
    d = Literal.of(date_str).to(date)
    assert identity.to_human_string(d.value) == date_str


def test_time_human_string():
    time = TimeType.get()

    identity = Transforms.identity(time)
    time_str = "10:12:55.038194"
    d = Literal.of(time_str).to(time)
    assert identity.to_human_string(d.value) == time_str


def test_timestamp_with_zone_human_string():
    ts_tz = TimestampType.with_timezone()
    identity = Transforms.identity(ts_tz)
    ts = Literal.of("2017-12-01T10:12:55.038194-08:00").to(ts_tz)

    assert identity.to_human_string(ts.value) == "2017-12-01T18:12:55.038194Z"


def test_timestamp_without_zone_human_string():
    ts_tz = TimestampType.without_timezone()
    identity = Transforms.identity(ts_tz)
    ts_str = "2017-12-01T10:12:55.038194"
    ts = Literal.of(ts_str).to(ts_tz)

    assert identity.to_human_string(ts.value) == ts_str


def test_long_to_human_string():
    long_type = LongType.get()
    identity = Transforms.identity(long_type)
    assert identity.to_human_string(-1234567890000) == "-1234567890000"


def test_string_to_human_string():
    str_type = StringType
    identity = Transforms.identity(str_type)

    with_slash = "a/b/c=d"
    assert identity.to_human_string(with_slash) == with_slash


def test_big_decimal_to_human_string():
    big_dec = DecimalType.of(9, 2)
    identity = Transforms.identity(big_dec)

    dec_str = "-1.50"
    dec_var = Decimal(dec_str)

    assert identity.to_human_string(dec_var) == dec_str

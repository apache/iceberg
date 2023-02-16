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
from decimal import Decimal
import uuid

import dateutil.parser
from fastavro.write import LOGICAL_WRITERS as avro_conversion
from iceberg.api.expressions import Literal
from iceberg.api.types import (BooleanType,
                               DateType,
                               DecimalType,
                               StringType,
                               TimestampType,
                               TimeType,
                               UUIDType)
from pytest import raises


def test_string_to_string_literal():
    assert Literal.of("abc") == Literal.of("abc").to(StringType.get())


def test_string_to_date_literal():
    date_str = Literal.of("2017-08-18")
    date = date_str.to(DateType.get())

    avro_val = avro_conversion["int-date"](datetime.strptime("2017-08-18", "%Y-%m-%d"), None)
    assert avro_val == date.value


def test_string_to_time_literal():
    time_str = Literal.of("14:21:01.919")
    time_lit = time_str.to(TimeType.get())

    avro_val = avro_conversion["long-time-micros"](datetime.strptime("14:21:01.919", "%H:%M:%S.%f").time(), None)

    assert avro_val == time_lit.value


def test_string_to_timestamp_literal():
    timestamp_str = Literal.of("2017-08-18T14:21:01.919+00:00")
    timestamp = timestamp_str.to(TimestampType.with_timezone())

    avro_val = avro_conversion["long-timestamp-micros"](dateutil.parser.parse("2017-08-18T14:21:01.919+00:00"),
                                                        None)
    assert avro_val == timestamp.value

    timestamp_str = Literal.of("2017-08-18T14:21:01.919")
    timestamp = timestamp_str.to(TimestampType.without_timezone())
    assert avro_val == timestamp.value

    timestamp_str = Literal.of("2017-08-18T14:21:01.919-07:00")
    timestamp = timestamp_str.to(TimestampType.with_timezone())
    avro_val = avro_conversion["long-timestamp-micros"](dateutil.parser.parse("2017-08-18T21:21:01.919+00:00"),
                                                        None)
    assert avro_val == timestamp.value


def test_timestamp_with_zone_without_zone_in_literal():
    with raises(RuntimeError):
        timestamp_str = Literal.of("2017-08-18T14:21:01.919")
        timestamp_str.to(TimestampType.with_timezone())


def test_timestamp_without_zone_with_zone_in_literal():
    with raises(RuntimeError):
        timestamp_str = Literal.of("2017-08-18T14:21:01.919+07:00")
        timestamp_str.to(TimestampType.without_timezone())


def test_string_to_uuid_literal():
    expected = uuid.uuid4()
    uuid_str = Literal.of(str(expected))
    uuid_lit = uuid_str.to(UUIDType.get())

    assert expected == uuid_lit.value


def test_string_to_decimal_literal():
    decimal_str = Literal.of("34.560")
    decimal_lit = decimal_str.to(DecimalType.of(9, 3))

    assert 3 == abs(decimal_lit.value.as_tuple().exponent)
    assert Decimal("34.560").as_tuple() == decimal_lit.value.as_tuple()

    assert decimal_str.to(DecimalType.of(9, 2)) is None
    assert decimal_str.to(DecimalType.of(9, 4)) is None


def test_string_to_boolean_literal():
    assert Literal.of(True) == Literal.of("true").to(BooleanType.get())
    assert Literal.of(True) == Literal.of("True").to(BooleanType.get())
    assert Literal.of(False) == Literal.of("false").to(BooleanType.get())
    assert Literal.of(False) == Literal.of("False").to(BooleanType.get())

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

import base64
import math
from datetime import datetime, timedelta
from decimal import Decimal

import pytz

_EPOCH = datetime.utcfromtimestamp(0)
_EPOCH_YEAR = _EPOCH.year


def human_year(year_ordinal: int) -> str:
    return "{0:0=4d}".format(_EPOCH_YEAR + year_ordinal)


def human_month(month_ordinal: int) -> str:
    return "{0:0=4d}-{1:0=2d}".format(
        _EPOCH_YEAR + int(month_ordinal / 12), 1 + int(month_ordinal % 12)
    )


def human_day(day_ordinal: int) -> str:
    time = _EPOCH + timedelta(days=day_ordinal)
    return "{0:0=4d}-{1:0=2d}-{2:0=2d}".format(time.year, time.month, time.day)


def human_hour(hour_ordinal: int) -> str:
    time = _EPOCH + timedelta(hours=hour_ordinal)
    return "{0:0=4d}-{1:0=2d}-{2:0=2d}-{3:0=2d}".format(
        time.year, time.month, time.day, time.hour
    )


def human_time(micros_from_midnight: int) -> str:
    day = _EPOCH + timedelta(microseconds=micros_from_midnight)
    return f"{day.time()}"


def human_timestamptz(timestamp_micros: int) -> str:
    day = _EPOCH + timedelta(microseconds=timestamp_micros)
    return pytz.timezone("UTC").localize(day).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def human_timestamp(timestamp_micros: int) -> str:
    day = _EPOCH + timedelta(microseconds=timestamp_micros)
    return day.isoformat()


def base64encode(buffer: bytes) -> str:
    return base64.b64encode(buffer).decode("ISO-8859-1")


def _unscale_decimal(decimal_value: Decimal) -> int:
    value_tuple = decimal_value.as_tuple()
    return int(
        ("-" if value_tuple.sign else "")
        + "".join([str(d) for d in value_tuple.digits])
    )


def decimal_to_bytes(value: Decimal) -> bytes:
    unscaled_value = _unscale_decimal(value)
    number_of_bytes = int(math.ceil(unscaled_value.bit_length() / 8))
    return unscaled_value.to_bytes(length=number_of_bytes, byteorder="big")


def truncate_decimal(value: Decimal, width: int) -> Decimal:
    unscaled_value = _unscale_decimal(value)
    applied_value = unscaled_value - (((unscaled_value % width) + width) % width)
    return Decimal(f"{applied_value}e{value.as_tuple().exponent}")


def diff_hour(date1: datetime):
    return int((date1 - _EPOCH).total_seconds() / 3600)


def diff_day(date1: datetime):
    return (date1 - _EPOCH).days


def diff_month(date1: datetime):
    return (
        (date1.year - _EPOCH.year) * 12
        + (date1.month - _EPOCH.month)
        - (1 if date1.day < _EPOCH.day else 0)
    )


def diff_year(date1: datetime):
    return (date1.year - _EPOCH.year) - (
        1
        if date1.month < _EPOCH.month
        or (date1.month == _EPOCH.month and date1.day < _EPOCH.day)
        else 0
    )


def to_string(value) -> str:
    return str(value)

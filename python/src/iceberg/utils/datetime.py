#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""Helper methods for working with date/time representations
"""
import re
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)

EPOCH_DATE = date.fromisoformat("1970-01-01")
EPOCH_TIMESTAMP = datetime.fromisoformat("1970-01-01T00:00:00.000000")
ISO_TIMESTAMP = re.compile(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d{1,6})?")
EPOCH_TIMESTAMPTZ = datetime.fromisoformat("1970-01-01T00:00:00.000000+00:00")
ISO_TIMESTAMPTZ = re.compile(r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(.\d{1,6})?[-+]\d\d:\d\d")


def micros_to_days(timestamp: int) -> int:
    """Converts a timestamp in microseconds to a date in days"""
    return (datetime.fromtimestamp(timestamp / 1_000_000) - EPOCH_TIMESTAMP).days


def date_to_days(date_str: str) -> int:
    """Converts an ISO-8601 formatted date to days from 1970-01-01"""
    return (date.fromisoformat(date_str) - EPOCH_DATE).days


def time_to_micros(time_str: str) -> int:
    """Converts an ISO-8601 formatted time to microseconds from midnight"""
    t = time.fromisoformat(time_str)
    return (((t.hour * 60 + t.minute) * 60) + t.second) * 1_000_000 + t.microsecond


def time_from_micros(micros: int) -> time:
    seconds = micros // 1_000_000
    minutes = seconds // 60
    hours = minutes // 60
    return time(hour=hours, minute=minutes % 60, second=seconds % 60, microsecond=micros % 1_000_000)


def datetime_to_micros(dt: datetime) -> int:
    """Converts a datetime to microseconds from 1970-01-01T00:00:00.000000"""
    if dt.tzinfo:
        delta = dt - EPOCH_TIMESTAMPTZ
    else:
        delta = dt - EPOCH_TIMESTAMP
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def timestamp_to_micros(timestamp_str: str) -> int:
    """Converts an ISO-9601 formatted timestamp without zone to microseconds from 1970-01-01T00:00:00.000000"""
    if ISO_TIMESTAMP.fullmatch(timestamp_str):
        return datetime_to_micros(datetime.fromisoformat(timestamp_str))
    raise ValueError(f"Invalid timestamp without zone: {timestamp_str} (must be ISO-8601)")


def timestamptz_to_micros(timestamptz_str: str) -> int:
    """Converts an ISO-8601 formatted timestamp with zone to microseconds from 1970-01-01T00:00:00.000000+00:00"""
    if ISO_TIMESTAMPTZ.fullmatch(timestamptz_str):
        return datetime_to_micros(datetime.fromisoformat(timestamptz_str))
    raise ValueError(f"Invalid timestamp with zone: {timestamptz_str} (must be ISO-8601)")


def to_human_day(day_ordinal: int) -> str:
    """Converts a DateType value to human string"""
    return (EPOCH_DATE + timedelta(days=day_ordinal)).isoformat()


def to_human_time(micros_from_midnight: int) -> str:
    """Converts a TimeType value to human string"""
    return time_from_micros(micros_from_midnight).isoformat()


def to_human_timestamptz(timestamp_micros: int) -> str:
    """Converts a TimestamptzType value to human string"""
    return (EPOCH_TIMESTAMPTZ + timedelta(microseconds=timestamp_micros)).isoformat()


def to_human_timestamp(timestamp_micros: int) -> str:
    """Converts a TimestampType value to human string"""
    return (EPOCH_TIMESTAMP + timedelta(microseconds=timestamp_micros)).isoformat()

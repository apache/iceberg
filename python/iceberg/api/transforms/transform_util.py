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
from datetime import datetime, timedelta

import pytz


EPOCH = datetime.utcfromtimestamp(0)
EPOCH_YEAR = datetime.utcfromtimestamp(0).year


def human_year(year_ordinal):
    return "{0:0=4d}".format(EPOCH_YEAR + year_ordinal)


def human_month(month_ordinal):
    return "{0:0=4d}-{1:0=2d}".format(EPOCH_YEAR + int(month_ordinal / 12), 1 + int(month_ordinal % 12))


def human_day(day_ordinal):
    day = EPOCH + timedelta(days=day_ordinal)
    return "{0:0=4d}-{1:0=2d}-{2:0=2d}".format(day.year, day.month, day.day)


def human_time(micros_from_midnight):
    day = EPOCH + timedelta(microseconds=micros_from_midnight)
    return "{}".format(day.time())


def human_timestamp_with_timezone(timestamp_micros):
    day = EPOCH + timedelta(microseconds=timestamp_micros)
    return pytz.timezone("UTC").localize(day).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def human_timestamp_without_timezone(timestamp_micros):
    day = EPOCH + timedelta(microseconds=timestamp_micros)
    return day.isoformat()


def human_hour(hour_ordinal):
    time = EPOCH + timedelta(hours=hour_ordinal)
    return "{0:0=4d}-{1:0=2d}-{2:0=2d}-{3:0=2d}".format(time.year, time.month, time.day, time.hour)


def base_64_encode(buffer):
    return base64.b64encode(buffer).decode('utf-8')


def diff_hour(date1, date2):
    return int((date1 - date2).total_seconds() / 3600)


def diff_day(date1, date2):
    return (date1 - date2).days


def diff_month(date1, date2):
    return (date1.year - date2.year) * 12 + (date1.month - date2.month) - (1 if date1.day < date2.day else 0)


def diff_year(date1, date2):
    return (date1.year - date2.year) - \
           (1 if date1.month < date2.month or (date1.month == date2.month and date1.day < date2.day) else 0)


def unscale_decimal(decimal_value):
    value_tuple = decimal_value.as_tuple()
    return int(("-" if value_tuple.sign else "") + "".join([str(d) for d in value_tuple.digits]))

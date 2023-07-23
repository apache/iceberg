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
from datetime import datetime, timezone, tzinfo

import pytest
import pytz

from pyiceberg.utils.datetime import datetime_to_millis

timezones = [
    pytz.timezone("Etc/GMT"),
    pytz.timezone("Etc/GMT+0"),
    pytz.timezone("Etc/GMT+1"),
    pytz.timezone("Etc/GMT+10"),
    pytz.timezone("Etc/GMT+11"),
    pytz.timezone("Etc/GMT+12"),
    pytz.timezone("Etc/GMT+2"),
    pytz.timezone("Etc/GMT+3"),
    pytz.timezone("Etc/GMT+4"),
    pytz.timezone("Etc/GMT+5"),
    pytz.timezone("Etc/GMT+6"),
    pytz.timezone("Etc/GMT+7"),
    pytz.timezone("Etc/GMT+8"),
    pytz.timezone("Etc/GMT+9"),
    pytz.timezone("Etc/GMT-0"),
    pytz.timezone("Etc/GMT-1"),
    pytz.timezone("Etc/GMT-10"),
    pytz.timezone("Etc/GMT-11"),
    pytz.timezone("Etc/GMT-12"),
    pytz.timezone("Etc/GMT-13"),
    pytz.timezone("Etc/GMT-14"),
    pytz.timezone("Etc/GMT-2"),
    pytz.timezone("Etc/GMT-3"),
    pytz.timezone("Etc/GMT-4"),
    pytz.timezone("Etc/GMT-5"),
    pytz.timezone("Etc/GMT-6"),
    pytz.timezone("Etc/GMT-7"),
    pytz.timezone("Etc/GMT-8"),
    pytz.timezone("Etc/GMT-9"),
]


def test_datetime_to_millis() -> None:
    dt = datetime(2023, 7, 10, 10, 10, 10, 123456)
    expected = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000)
    datetime_millis = datetime_to_millis(dt)
    assert datetime_millis == expected


@pytest.mark.parametrize("tz", timezones)
def test_datetime_tz_to_millis(tz: tzinfo) -> None:
    dt = datetime(2023, 7, 10, 10, 10, 10, 123456, tzinfo=tz)
    expected = int(dt.timestamp() * 1_000)
    datetime_millis = datetime_to_millis(dt)
    assert datetime_millis == expected

import pytest

from pyiceberg.utils.datetime import datetime_to_millis
from datetime import datetime, tzinfo
from pytz import timezone


timezones = [
    timezone('Etc/GMT'),
    timezone('Etc/GMT+0'),
    timezone('Etc/GMT+1'),
    timezone('Etc/GMT+10'),
    timezone('Etc/GMT+11'),
    timezone('Etc/GMT+12'),
    timezone('Etc/GMT+2'),
    timezone('Etc/GMT+3'),
    timezone('Etc/GMT+4'),
    timezone('Etc/GMT+5'),
    timezone('Etc/GMT+6'),
    timezone('Etc/GMT+7'),
    timezone('Etc/GMT+8'),
    timezone('Etc/GMT+9'),
    timezone('Etc/GMT-0'),
    timezone('Etc/GMT-1'),
    timezone('Etc/GMT-10'),
    timezone('Etc/GMT-11'),
    timezone('Etc/GMT-12'),
    timezone('Etc/GMT-13'),
    timezone('Etc/GMT-14'),
    timezone('Etc/GMT-2'),
    timezone('Etc/GMT-3'),
    timezone('Etc/GMT-4'),
    timezone('Etc/GMT-5'),
    timezone('Etc/GMT-6'),
    timezone('Etc/GMT-7'),
    timezone('Etc/GMT-8'),
    timezone('Etc/GMT-9'),
]


def test_datetime_to_millis() -> None:
    dt = datetime(2023, 7, 10, 10, 10, 10, 123456)
    expected = int(dt.timestamp() * 1_000)
    datetime_millis = datetime_to_millis(dt)
    assert datetime_millis == expected


@pytest.mark.parametrize("tz", timezones)
def test_datetime_tz_to_millis(tz: tzinfo) -> None:
    dt = datetime(2023, 7, 10, 10, 10, 10, 123456, tzinfo=tz)
    expected = int(dt.timestamp() * 1_000)
    datetime_millis = datetime_to_millis(dt)
    assert datetime_millis == expected

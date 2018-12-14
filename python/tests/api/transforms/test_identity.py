import unittest

from iceberg.api.expressions import Literal
from iceberg.api.transforms import (Identity,
                                    Transforms)
from iceberg.api.types import (DateType,
                               LongType,
                               TimestampType,
                               TimeType)


class TestIdentity(unittest.TestCase):

    def test_null_human_string(self):
        long_type = LongType.get()
        identity = Identity(long_type)
        self.assertEquals("null", identity.to_human_string(None))

    def test_date_human_string(self):
        date = DateType.get()

        identity = Transforms.identity(date)
        date_str = "2017-12-01"
        d = Literal.of(date_str).to(date)
        self.assertEquals(date_str, identity.to_human_string(d.value))

    def test_time_human_string(self):
        time = TimeType.get()

        identity = Transforms.identity(time)
        time_str = "10:12:55.038194"
        d = Literal.of(time_str).to(time)
        self.assertEquals(time_str, identity.to_human_string(d.value))

    def test_timestamp_with_zone_human_string(self):
        ts_tz = TimestampType.with_timezone()
        identity = Transforms.identity(ts_tz)
        ts = Literal.of("2017-12-01T10:12:55.038194-08:00").to(ts_tz)

        self.assertEquals("2017-12-01T18:12:55.038194Z", identity.to_human_string(ts.value))

    def test_timestamp_without_zone_human_string(self):
        ts_tz = TimestampType.without_timezone()
        identity = Transforms.identity(ts_tz)
        ts_str = "2017-12-01T10:12:55.038194"
        ts = Literal.of(ts_str).to(ts_tz)

        self.assertEquals(ts_str, identity.to_human_string(ts.value))

    def test_long_to_human_string(self):
        pass

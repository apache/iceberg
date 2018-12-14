import unittest

from iceberg.api.expressions import Literal
from iceberg.api.transforms import Transforms
from iceberg.api.types import TimestampType


class TestTimestamps(unittest.TestCase):

    def test_ts_without_zone_to_human_string(self):
        type_var = TimestampType.without_timezone()
        date = Literal.of("2017-12-01T10:12:55.038194").to(type_var)

        year = Transforms.year(type_var)
        self.assertEquals("2017", year.to_human_string(year.apply(date.value)))
        month = Transforms.month(type_var)
        self.assertEquals("2017-12", month.to_human_string(month.apply(date.value)))
        day = Transforms.day(type_var)
        self.assertEquals("2017-12-01", day.to_human_string(day.apply(date.value)))
        hour = Transforms.hour(type_var)
        self.assertEquals("2017-12-01-10", hour.to_human_string(hour.apply(date.value)))

    def test_ts_with_zone_to_human_string(self):
        type_var = TimestampType.with_timezone()
        date = Literal.of("2017-12-01T10:12:55.038194-08:00").to(type_var)

        year = Transforms.year(type_var)
        self.assertEquals("2017", year.to_human_string(year.apply(date.value)))
        month = Transforms.month(type_var)
        self.assertEquals("2017-12", month.to_human_string(month.apply(date.value)))
        day = Transforms.day(type_var)
        self.assertEquals("2017-12-01", day.to_human_string(day.apply(date.value)))
        hour = Transforms.hour(type_var)
        self.assertEquals("2017-12-01-18", hour.to_human_string(hour.apply(date.value)))

    def test_null_human_string(self):
        type_var = TimestampType.with_timezone()
        self.assertEquals("null", Transforms.year(type_var).to_human_string(None))
        self.assertEquals("null", Transforms.month(type_var).to_human_string(None))
        self.assertEquals("null", Transforms.day(type_var).to_human_string(None))
        self.assertEquals("null", Transforms.hour(type_var).to_human_string(None))

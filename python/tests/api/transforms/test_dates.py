import unittest

from iceberg.api.expressions import Literal
from iceberg.api.transforms import Transforms
from iceberg.api.types import DateType


class TestDates(unittest.TestCase):

    def test_date_to_human_string(self):
        type_var = DateType.get()
        date = Literal.of("2017-12-01").to(type_var)

        year = Transforms.year(type_var)
        self.assertEqual("2017", year.to_human_string(year.apply(date.value)))
        month = Transforms.month(type_var)
        self.assertEqual("2017-12", month.to_human_string(month.apply(date.value)))
        day = Transforms.day(type_var)
        self.assertEqual("2017-12-01", day.to_human_string(day.apply(date.value)))

    def test_null_human_string(self):
        type_var = DateType.get()
        self.assertEqual("null", Transforms.year(type_var).to_human_string(None))
        self.assertEqual("null", Transforms.month(type_var).to_human_string(None))
        self.assertEqual("null", Transforms.day(type_var).to_human_string(None))

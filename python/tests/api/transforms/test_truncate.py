from decimal import Decimal
import unittest

from iceberg.api.transforms import Truncate
from iceberg.api.types import (DecimalType,
                               IntegerType,
                               LongType,
                               StringType)


class TestTruncate(unittest.TestCase):

    def test_truncate_integer(self):
        trunc = Truncate.get(IntegerType.get(), 10)
        self.assertEquals(0, trunc.apply(0))
        self.assertEquals(0, trunc.apply(1))
        self.assertEquals(0, trunc.apply(5))
        self.assertEquals(0, trunc.apply(9))
        self.assertEquals(10, trunc.apply(10))
        self.assertEquals(10, trunc.apply(11))
        self.assertEquals(-10, trunc.apply(-1))
        self.assertEquals(-10, trunc.apply(-5))
        self.assertEquals(-10, trunc.apply(-10))
        self.assertEquals(-20, trunc.apply(-12))

    def test_truncate_long(self):
        trunc = Truncate.get(LongType.get(), 10)
        self.assertEquals(0, trunc.apply(0))
        self.assertEquals(0, trunc.apply(1))
        self.assertEquals(0, trunc.apply(5))
        self.assertEquals(0, trunc.apply(9))
        self.assertEquals(10, trunc.apply(10))
        self.assertEquals(10, trunc.apply(11))
        self.assertEquals(-10, trunc.apply(-1))
        self.assertEquals(-10, trunc.apply(-5))
        self.assertEquals(-10, trunc.apply(-10))
        self.assertEquals(-20, trunc.apply(-12))

    def test_truncate_decimal(self):
        trunc = Truncate.get(DecimalType.of(9, 2), 10)
        self.assertEquals(Decimal("12.30"), trunc.apply(Decimal(12.34).quantize(Decimal(".01"))))
        self.assertEquals(Decimal("12.30"), trunc.apply(Decimal(12.30).quantize(Decimal(".01"))))
        self.assertEquals(Decimal("12.20"), trunc.apply(Decimal(12.20).quantize(Decimal(".01"))))
        self.assertEquals(Decimal("0.00"), trunc.apply(Decimal(0.05).quantize(Decimal(".01"))))
        self.assertEquals(Decimal("-0.10"), trunc.apply(Decimal(-0.05).quantize(Decimal(".01"))))

    def test_truncate_string(self):
        trunc = Truncate.get(StringType.get(), 5)
        self.assertEquals("abcde", trunc.apply("abcdefg"))
        self.assertEquals("abc", trunc.apply("abc"))

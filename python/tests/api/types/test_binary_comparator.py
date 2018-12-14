import unittest

from iceberg.api.expressions import Literal
from iceberg.api.types import (FixedType)


class TestBinaryComparator(unittest.TestCase):

    def test_binary_unsigned_comparator(self):
        b1 = bytearray([0x01, 0x01, 0x02])
        b2 = bytearray([0x01, 0xFF, 0x01])

        self.assertTrue(Literal.of(b2) > Literal.of(b1))

    def test_fixed_unsigned_comparator(self):
        b1 = bytearray([0x01, 0x01, 0x02])
        b2 = bytearray([0x01, 0xFF, 0x01])

        self.assertTrue(Literal.of(b2) > Literal.of(b1).to(FixedType.of_length(3)))

    def test_null_handling(self):
        b1 = bytearray([0x01])

        self.assertTrue(None < Literal.of(b1))
        self.assertTrue(Literal.of(b1) > None)
        self.assertTrue(Literal.of(b1).to(FixedType.of_length(3)) == Literal.of(b1).to(FixedType.of_length(4)))

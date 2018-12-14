import unittest

from iceberg.api.types import (DoubleType,
                               IntegerType,
                               LongType)
from iceberg.api.types.conversions import Conversions


class TestConversions(unittest.TestCase):

    def test_from_bytes(self):
        self.assertEqual(1234, Conversions.from_byte_buffer(IntegerType.get(),
                                                            b'\xd2\x04\x00\x00'))
        self.assertEqual(1234, Conversions.from_byte_buffer(LongType.get(),
                                                            b'\xd2\x04\x00\x00\x00\x00\x00\x00'))
        self.assertAlmostEqual(1.2345, Conversions.from_byte_buffer(DoubleType.get(),
                                                                    b'\x8d\x97\x6e\x12\x83\xc0\xf3\x3f'))

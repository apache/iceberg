import unittest

from iceberg.api.expressions import (IntegerLiteral,
                                     Literal)


class TestComparableComparator(unittest.TestCase):

    def test_natural_order(self):
        self.assertTrue(Literal.of(34) > Literal.of(33))

    def test_null_handling(self):
        self.assertTrue(IntegerLiteral(None) < IntegerLiteral(34))
        self.assertTrue(IntegerLiteral(34) > IntegerLiteral(None))
        self.assertTrue(IntegerLiteral(None) == IntegerLiteral(None))

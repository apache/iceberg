import unittest

from iceberg.api.expressions import (Literal,
                                     StringLiteral)


class TestCharSeqComparator(unittest.TestCase):

    def test_str_and_utf8(self):
        s1 = "abc"
        s2 = u'abc'

        self.assertEqual(Literal.of(s1), Literal.of(s2))

    def test_seq_length(self):
        s1 = "abc"
        s2 = "abcd"

        self.assertTrue(Literal.of(s2).value > Literal.of(s1).value)
        self.assertTrue(Literal.of(s1).value < Literal.of(s2).value)

        self.assertTrue(Literal.of(s2) > Literal.of(s1))
        self.assertTrue(Literal.of(s1) < Literal.of(s2))

    def test_char_order_before_length(self):
        s1 = "adc"
        s2 = "abcd"

        self.assertTrue(Literal.of(s1).value > Literal.of(s2).value)
        self.assertTrue(Literal.of(s2).value < Literal.of(s1).value)

        self.assertTrue(Literal.of(s1) > Literal.of(s2))
        self.assertTrue(Literal.of(s2) < Literal.of(s1))

    def test_null_handling(self):
        s1 = "abc"
        self.assertTrue(Literal.of(s1) > None)
        self.assertTrue(None < Literal.of(s1))
        self.assertTrue(StringLiteral(None) == StringLiteral(None))

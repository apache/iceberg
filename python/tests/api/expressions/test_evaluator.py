# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

import iceberg.api.expressions as exp
from iceberg.api.types import (IntegerType,
                               NestedField,
                               StringType,
                               StructType)
from tests.api.test_helpers import TestHelpers


class TestingEvaluator(unittest.TestCase):

    STRUCT = StructType.of([NestedField.required(13, "x", IntegerType.get()),
                           NestedField.required(14, "y", IntegerType.get()),
                           NestedField.optional(15, "z", IntegerType.get())])

    def test_less_than(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.less_than("x", 7))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((7, 8, None))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((6, 8, None))))

    def test_less_than_or_equal(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.less_than_or_equal("x", 7))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((7, 8, None))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((6, 8, None))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((8, 8, None))))

    def test_greater_than(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.greater_than("x", 7))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((7, 8, None))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((6, 8, None))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((8, 8, None))))

    def test_greater_than_or_equal(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.greater_than_or_equal("x", 7))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((7, 8, None))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((6, 8, None))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((8, 8, None))))

    def test_equal(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.equal("x", 7))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((7, 8, None))))

    def test_not_equal(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.not_equal("x", 7))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((7, 8, None))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((6, 8, None))))

    def test_always_true(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.always_true())
        self.assertTrue(evaluator.eval(TestHelpers.Row.of(())))

    def test_always_false(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.always_false())
        self.assertFalse(evaluator.eval(TestHelpers.Row.of(())))

    def test_is_null(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.is_null("z"))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((1, 2, None))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((1, 2, 3))))

    def test_is_not_null(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.not_null("z"))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((1, 2, None))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((1, 2, 3))))

    def test_and(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.and_(exp.expressions.Expressions.equal("x", 7),
                                                                             exp.expressions.Expressions.not_null("z")))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((7, 0, 3))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((8, 0, 3))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((7, 0, None))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((8, 0, None))))

    def test_or(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.or_(exp.expressions.Expressions.equal("x", 7),
                                                                            exp.expressions.Expressions.not_null("z")))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((7, 0, 3))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((8, 0, 3))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((7, 0, None))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((8, 0, None))))

    def test_not(self):
        evaluator = exp.evaluator.Evaluator(TestingEvaluator.STRUCT,
                                            exp.expressions.Expressions.not_(exp.expressions.Expressions.equal("x", 7)))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of((7,))))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of((8,))))

    def test_char_seq_value(self):
        struct = StructType.of([NestedField.required(34, "s", StringType.get())])
        evaluator = exp.evaluator.Evaluator(struct, exp.expressions.Expressions.equal("s", "abc"))
        self.assertTrue(evaluator.eval(TestHelpers.Row.of(("abc",))))
        self.assertFalse(evaluator.eval(TestHelpers.Row.of(("abcd",))))

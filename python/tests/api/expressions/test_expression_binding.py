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

import unittest # noqa

import iceberg.api.exceptions as ice_ex
from iceberg.api.expressions.binder import Binder
from iceberg.api.expressions.expression import (And,
                                                Not,
                                                Or)
from iceberg.api.expressions.expressions import Expressions
from iceberg.api.types import (IntegerType,
                               NestedField,
                               StructType)
from nose.tools import raises
from tests.api.test_helpers import TestHelpers


class TestingExpressionBinding(unittest.TestCase):
    STRUCT = StructType.of([NestedField.required(0, "x", IntegerType.get()),
                            NestedField.required(1, "y", IntegerType.get()),
                            NestedField.required(2, "z", IntegerType.get())])

    def test_missing_reference(self):
        expr = Expressions.and_(Expressions.equal("t", 5),
                                Expressions.equal("x", 7))
        try:
            Binder.bind(TestingExpressionBinding.STRUCT, expr)
        except ice_ex.ValidationException as e:
            self.assertTrue("Cannot find field 't' in struct" in "{}".format(e))

    @raises(RuntimeError)
    def test_bound_expression_fails(self):
        expr = Expressions.not_(Expressions.equal("x", 7))
        Binder.bind(TestingExpressionBinding.STRUCT,
                    Binder.bind(TestingExpressionBinding.STRUCT, expr))

    def test_single_reference(self):
        expr = Expressions.not_(Expressions.equal("x", 7))
        TestHelpers.assert_all_references_bound("Single reference",
                                                Binder.bind(TestingExpressionBinding.STRUCT,
                                                            expr))

    def test_multiple_references(self):
        expr = Expressions.or_(Expressions.and_(Expressions.equal("x", 7),
                                                Expressions.less_than("y", 100)),
                               Expressions.greater_than("z", -100))

        TestHelpers.assert_all_references_bound("Multiple references",
                                                Binder.bind(TestingExpressionBinding.STRUCT,
                                                            expr))

    def test_and(self):
        expr = Expressions.and_(Expressions.equal("x", 7),
                                Expressions.less_than("y", 100))
        bound_expr = Binder.bind(TestingExpressionBinding.STRUCT, expr)
        TestHelpers.assert_all_references_bound("And", bound_expr)

        and_ = TestHelpers.assert_and_unwrap(bound_expr, And)

        left = TestHelpers.assert_and_unwrap(and_.left, None)
        # should bind x correctly
        self.assertEquals(0, left.ref.field_id)
        right = TestHelpers.assert_and_unwrap(and_.right, None)
        # should bind y correctly
        self.assertEquals(1, right.ref.field_id)

    def test_or(self):
        expr = Expressions.or_(Expressions.greater_than("z", -100),
                               Expressions.less_than("y", 100))
        bound_expr = Binder.bind(TestingExpressionBinding.STRUCT, expr)
        TestHelpers.assert_all_references_bound("Or", bound_expr)

        or_ = TestHelpers.assert_and_unwrap(bound_expr, Or)

        left = TestHelpers.assert_and_unwrap(or_.left, None)
        # should bind z correctly
        self.assertEquals(2, left.ref.field_id)
        right = TestHelpers.assert_and_unwrap(or_.right, None)
        # should bind y correctly
        self.assertEquals(1, right.ref.field_id)

    def test_not(self):
        expr = Expressions.not_(Expressions.equal("x", 7))
        bound_expr = Binder.bind(TestingExpressionBinding.STRUCT, expr)
        TestHelpers.assert_all_references_bound("Not", bound_expr)

        not_ = TestHelpers.assert_and_unwrap(bound_expr, Not)

        child = TestHelpers.assert_and_unwrap(not_.child, None)
        # should bind x correctly
        self.assertEquals(0, child.ref.field_id)

    def test_always_true(self):
        self.assertEquals(Expressions.always_true(),
                          Binder.bind(TestingExpressionBinding.STRUCT,
                                      Expressions.always_true()))

    def test_always_false(self):
        self.assertEquals(Expressions.always_false(),
                          Binder.bind(TestingExpressionBinding.STRUCT,
                                      Expressions.always_false()))

    def test_basic_simplification(self):
        # Should simplify or expression to alwaysTrue
        self.assertEquals(Expressions.always_true(),
                          Binder.bind(TestingExpressionBinding.STRUCT,
                                      Expressions.or_(Expressions.less_than("y", 100),
                                                      Expressions.greater_than("z", -9999999999))))
        # Should simplify or expression to alwaysfalse
        self.assertEquals(Expressions.always_false(),
                          Binder.bind(TestingExpressionBinding.STRUCT,
                                      Expressions.and_(Expressions.less_than("y", 100),
                                                       Expressions.less_than("z", -9999999999))))

        bound = Binder.bind(TestingExpressionBinding.STRUCT,
                            Expressions.not_(Expressions.not_(Expressions.less_than("y", 100))))
        pred = TestHelpers.assert_and_unwrap(bound, None)
        self.assertEquals(1, pred.ref.field_id)

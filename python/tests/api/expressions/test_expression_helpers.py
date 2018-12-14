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

from iceberg.api.expressions.expressions import Expressions
from nose.tools import raises

pred = Expressions.less_than("x", 7)


class TestingExpressionHelpers(unittest.TestCase):

    def test_simplify_or(self):
        self.assertEqual(Expressions.always_true(),
                         Expressions.or_(Expressions.always_true(), pred))
        self.assertEqual(Expressions.always_true(),
                         Expressions.or_(pred, Expressions.always_true()))

        self.assertEqual(pred,
                         Expressions.or_(Expressions.always_false(), pred))
        self.assertEqual(pred,
                         Expressions.or_(pred, Expressions.always_false()))

    def test_simplify_and(self):
        self.assertEqual(pred,
                         Expressions.and_(Expressions.always_true(), pred))
        self.assertEqual(pred,
                         Expressions.and_(pred, Expressions.always_true()))

        self.assertEqual(Expressions.always_false(),
                         Expressions.and_(Expressions.always_false(), pred))
        self.assertEqual(Expressions.always_false(),
                         Expressions.and_(pred, Expressions.always_false()))

    def test_simplify_not(self):
        self.assertEqual(Expressions.always_false(), Expressions.not_(Expressions.always_true()))
        self.assertEqual(Expressions.always_true(), Expressions.not_(Expressions.always_false()))
        self.assertEqual(pred, Expressions.not_(Expressions.not_(pred)))

    @raises(RuntimeError)
    def test_null_name(self):
        Expressions.equal(None, 5)

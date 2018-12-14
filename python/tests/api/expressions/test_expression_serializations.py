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

from iceberg.api.expressions.expression import (Expression,
                                                Operation)
from iceberg.api.expressions.expressions import Expressions
from iceberg.api.expressions.predicate import Predicate
from iceberg.api.expressions.reference import (BoundReference,
                                               NamedReference,
                                               Reference)
import iceberg.api.schema as sch
from iceberg.api.types.types import (IntegerType,
                                     NestedField)
from tests.api.test_helpers import TestHelpers


op_map = {Operation.TRUE: lambda left, right: True,
          Operation.FALSE: lambda left, right: True,
          Operation.NOT: lambda left, right: equals(left.child, right.child),
          Operation.AND: lambda left, right: equals(left.left, right.left) and equals(left.right, right.right),
          Operation.OR: lambda left, right: equals(left.left, right.left) and equals(left.right, right.right),
          }


class TestingExpressionSerializations(unittest.TestCase):

    def test_expressions(self):

        schema = sch.Schema(NestedField.optional(34, "a", IntegerType.get()))
        expressions = [Expressions.always_false(),
                       Expressions.always_true(),
                       Expressions.less_than("x", 5),
                       Expressions.less_than_or_equal("y", -3),
                       Expressions.greater_than("z", 0),
                       Expressions.greater_than_or_equal("t", 129),
                       Expressions.equal("col", "data"),
                       Expressions.not_equal("col", "abc"),
                       Expressions.not_null("maybeNull"),
                       Expressions.is_null("maybeNull2"),
                       Expressions.not_(Expressions.greater_than("a", 10)),
                       Expressions.and_(Expressions.greater_than_or_equal("a", 0),
                                        Expressions.less_than("a", 3)),
                       Expressions.or_(Expressions.less_than("a", 0),
                                       Expressions.greater_than("a", 10)),
                       Expressions.equal("a", 5).bind(schema.as_struct())]

        for expression in expressions:
            copy = TestHelpers.round_trip_serialize(expression)

            self.assertTrue(equals(copy, expression))


def equals(left, right):
    if isinstance(left, Reference) and isinstance(right, Reference):
        rv = reference_equals(left, right)
    elif isinstance(left, Predicate) and isinstance(right, Predicate):
        rv = predicate_equals(left, right)
    elif isinstance(left, Expression) and isinstance(right, Expression):
        rv = expression_equals(left, right)
    else:
        raise RuntimeError("both arguments must either be predicates or expressions:\n%s %s" % (left, right))

    return rv


def expression_equals(left, right):
    if left.op() != right.op():
        return False

    rv = op_map.get(left.op(), lambda: False)(left, right)
    return rv


def predicate_equals(left, right):
    if left.op != right.op:
        return False

    if not reference_equals(left.ref, right.ref):
        return False

    if left.op == Operation.IS_NULL or left.op == Operation.NOT_NULL:
        return True

    return left.lit == right.lit


def reference_equals(left, right):
    if isinstance(left, NamedReference):
        if not isinstance(right, NamedReference):
            return False

        return left.name == right.name
    elif isinstance(left, BoundReference):
        if not isinstance(right, BoundReference):
            return False

        return left.field_id == right.field_id and left._type == right._type

    return False

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from .expressions import Expressions, ExpressionVisitors
from .predicate import BoundPredicate


class Binder(object):

    @staticmethod
    def bind(struct, expr, case_sensitive=True):
        return ExpressionVisitors.visit(expr, Binder.BindVisitor(struct, case_sensitive))

    @staticmethod
    def bound_references(struct, exprs, case_sensitive=True):
        if exprs is None:
            return set()
        visitor = Binder.ReferenceVisitor()
        for expr in exprs:
            ExpressionVisitors.visit(Binder.bind(struct, expr, case_sensitive), visitor)

        return visitor.references

    def __init__(self):
        pass

    class BindVisitor(ExpressionVisitors.ExpressionVisitor):

        def __init__(self, struct, case_sensitive=True):
            self.struct = struct
            self.case_sensitive = case_sensitive

        def always_true(self):
            return Expressions.always_true()

        def always_false(self):
            return Expressions.always_false()

        def not_(self, result):
            return Expressions.not_(result)

        def and_(self, left_result, right_result):
            return Expressions.and_(left_result, right_result)

        def or_(self, left_result, right_result):
            return Expressions.or_(left_result, right_result)

        def predicate(self, pred):
            if isinstance(pred, BoundPredicate):
                raise RuntimeError("Found already bound predicate: {}".format(pred))

            return pred.bind(self.struct, self.case_sensitive)

    class ReferenceVisitor(ExpressionVisitors.ExpressionVisitor):

        def __init__(self):
            self.references = set()

        def always_true(self):
            return self.references

        def always_false(self):
            return self.references

        def not_(self, result):
            return self.references

        def and_(self, left_result, right_result):
            return self.references

        def or_(self, left_result, right_result):
            return self.references

        def predicate(self, pred):
            if isinstance(pred, BoundPredicate):
                self.references.add(pred.ref.field_id)
                return self.references

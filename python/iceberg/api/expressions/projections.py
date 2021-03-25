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


from .expressions import Expressions, ExpressionVisitors, RewriteNot
from .predicate import BoundPredicate, UnboundPredicate


def inclusive(spec, case_sensitive=True):
    return InclusiveProjection(spec, case_sensitive)


def strict(spec):
    return StrictProjection(spec)


class ProjectionEvaluator(ExpressionVisitors.ExpressionVisitor):

    def project(self, expr):
        raise NotImplementedError()


class BaseProjectionEvaluator(ProjectionEvaluator):

    def __init__(self, spec, case_sensitive=True):
        self.spec = spec
        self.case_sensitive = case_sensitive

    def project(self, expr):
        # projections assume that there are no NOT nodes in the expression tree. to ensure that this
        # is the case, the expression is rewritten to push all NOT nodes down to the expression
        # leaf nodes.
        # this is necessary to ensure that the default expression returned when a predicate can't be
        # projected is correct.
        #
        return ExpressionVisitors.visit(ExpressionVisitors.visit(expr, RewriteNot.get()), self)

    def always_true(self):
        return Expressions.always_true()

    def always_false(self):
        return Expressions.always_false()

    def not_(self, result):
        raise RuntimeError("[BUG] project called on expression with a not")

    def and_(self, left_result, right_result):
        return Expressions.and_(left_result, right_result)

    def or_(self, left_result, right_result):
        return Expressions.or_(left_result, right_result)

    def predicate(self, pred):
        bound = pred.bind(self.spec.schema.as_struct(), case_sensitive=self.case_sensitive)

        if isinstance(bound, BoundPredicate):
            return self.predicate(bound)

        return bound


class InclusiveProjection(BaseProjectionEvaluator):

    def __init__(self, spec, case_sensitive=True):
        super(InclusiveProjection, self).__init__(spec,
                                                  case_sensitive=case_sensitive)

    def predicate(self, pred):
        if isinstance(pred, UnboundPredicate):
            return super(InclusiveProjection, self).predicate(pred)

        part = self.spec.get_field_by_source_id(pred.ref.field.field_id)

        if part is None:
            return self.always_true()

        result = part.transform.project(part.name, pred)
        if result is not None:
            return result

        return self.always_true()


class StrictProjection(BaseProjectionEvaluator):

    def __init__(self, spec):
        super(StrictProjection, self).__init__(spec)

    def predicate(self, pred):
        part = self.spec.get_field_by_source_id(pred.ref.field.field_id)

        if part is None:
            return self.always_false()

        result = part.transform.project_strict(part.name, pred)

        if result is not None:
            return result

        return self.always_false()

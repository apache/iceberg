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


import decimal

from ..expressions import Expressions, Operation
from ..expressions.predicate import UnboundPredicate


class ProjectionUtil(object):
    @staticmethod
    def truncate_integer(name, pred, transform):
        boundary = pred.lit.value
        if pred.op == Operation.LT:
            return Expressions.predicate(Operation.LT_EQ, name, transform.apply(boundary - 1))
        elif pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.GT:
            return Expressions.predicate(Operation.GT_EQ, name, transform.apply(boundary + 1))
        elif pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.EQ:
            return Expressions.predicate(pred.op, name, transform.apply(boundary))

    @staticmethod
    def truncate_integer_strict(name, pred, transform):
        boundary = pred.lit.value
        if pred.op == Operation.LT:
            return Expressions.predicate(Operation.LT, name, transform.apply(boundary))
        elif pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT, name, transform.apply(boundary + 1))
        elif pred.op == Operation.GT:
            return Expressions.predicate(Operation.GT, name, transform.apply(boundary))
        elif pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT, name, transform.apply(boundary - 1))
        elif pred.op == Operation.NOT_EQ:
            return Expressions.predicate(Operation.NOT_EQ, name, transform.apply(boundary))

    @staticmethod
    def truncate_long(name, pred, transform):
        return ProjectionUtil.truncate_integer(name, pred, transform)

    @staticmethod
    def truncate_long_strict(name, pred, transform):
        return ProjectionUtil.truncate_integer_strict(name, pred, transform)

    @staticmethod
    def truncate_decimal(name, pred, transform):
        boundary = pred.lit.value

        if pred.op == Operation.LT:
            minus_one = boundary - decimal.Decimal(1)
            return Expressions.predicate(Operation.LT_EQ, name, transform.apply(minus_one))
        elif pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.GT:
            plus_one = boundary + decimal.Decimal(1)
            return Expressions.predicate(Operation.GT_EQ, name, transform.apply(plus_one))
        elif pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.EQ:
            return Expressions.predicate(pred.op, name, transform.apply(boundary))

    @staticmethod
    def truncate_decimal_strict(name, pred, transform):
        boundary = pred.lit.value
        minus_one = boundary - decimal.Decimal(1)
        plus_one = boundary + decimal.Decimal(1)

        if pred.op == Operation.LT:
            return Expressions.predicate(Operation.LT, name, transform.apply(boundary))
        elif pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT, name, transform.apply(plus_one))
        elif pred.op == Operation.GT:
            return Expressions.predicate(Operation.GT, name, transform.apply(boundary))
        elif pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT, name, transform.apply(minus_one))
        elif pred.op == Operation.NOT_EQ:
            return Expressions.predicate(Operation.NOT_EQ, name, transform.apply(boundary))

    @staticmethod
    def truncate_array(name, pred, transform):
        boundary = pred.lit.value

        if pred.op == Operation.LT or pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.GT or pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.EQ:
            return Expressions.predicate(pred.op, name, transform.apply(boundary))
        # todo elif pred.op == Operation.STARTS_WITH:

    @staticmethod
    def truncate_array_strict(name, pred, transform):
        boundary = pred.lit.value

        if pred.op == Operation.LT or pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT, name, transform.apply(boundary))
        elif pred.op == Operation.GT or pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT, name, transform.apply(boundary))
        elif pred.op == Operation.NOT_EQ:
            return Expressions.predicate(Operation.NOT_EQ, name, transform.apply(boundary))

    @staticmethod
    def project_transform_predicate(transform, partiton_name, pred):
        raise NotImplementedError

    @staticmethod
    def fix_inclusive_time_projection(projected: UnboundPredicate):
        if projected is None:
            return projected

        if projected.op == Operation.LT and projected.lit.value < 0:
            return Expressions.less_than(projected.term, projected.lit.value + 1)
        elif projected.op == Operation.LT_EQ and projected.lit.value < 0:
            return Expressions.less_than_or_equal(projected.term, projected.lit.value + 1)
        elif projected.op == Operation.EQ and projected.lit.value < 0:
            raise NotImplementedError  # todo implement in
        elif projected.op == Operation.IN:
            raise NotImplementedError  # todo implement in
        elif projected.op == Operation.NOT_IN or projected.op == Operation.NOT_EQ:
            return None

        return projected

    @staticmethod
    def fix_strict_time_projection(projected: UnboundPredicate):
        if projected is None:
            return projected

        if projected.op == Operation.GT and projected.lit.value <= 0:
            return Expressions.greater_than(projected.term, projected.lit.value + 1)
        elif projected.op == Operation.GT_EQ and projected.lit.value <= 0:
            return Expressions.greater_than_or_equal(projected.term, projected.lit.value + 1)
        elif projected.op == Operation.NOT_EQ and projected.lit.value < 0:
            raise NotImplementedError  # todo implement not_in
        elif projected.op == Operation.NOT_IN:
            raise NotImplementedError  # todo implement not_in
        elif projected.op == Operation.EQ or projected.op == Operation.IN:
            return None

        return projected

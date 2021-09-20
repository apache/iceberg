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

from iceberg.api.expressions import Expressions, Operation


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

    def truncate_long(name, pred, transform):
        return ProjectionUtil.truncate_integer(name, pred, transform)

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

    def truncate_array(name, pred, transform):
        boundary = pred.lit.value

        if pred.op == Operation.LT or pred.op == Operation.LT_EQ:
            return Expressions.predicate(Operation.LT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.GT or pred.op == Operation.GT_EQ:
            return Expressions.predicate(Operation.GT_EQ, name, transform.apply(boundary))
        elif pred.op == Operation.EQ:
            return Expressions.predicate(pred.op, name, transform.apply(boundary))
